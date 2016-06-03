import sys
import json
import os
import threading
import Queue
import logging
from sys import stdin, stdout
from nyamuk.nyamuk import *
from nyamuk.event import *

logging.basicConfig(filename='/home/lab/mqtt_helper.log',level=logging.INFO)
queueLock = threading.Lock()
workQueue = Queue.Queue(1000)

class command_thread(threading.Thread):
    def __init__(self, threadID):
        threading.Thread.__init__(self)
        self.client = Nyamuk("exabgp_command_client", server="127.0.0.1", log_level=logging.INFO)
        #ret = client.connect(version=4)
        ret = self.client.connect()
        ret = self.nloop() # ret should be EventConnack object
        if not isinstance(ret, EventConnack) or ret.ret_code != 0:
            logging.error("Cannot connect to mqtt server"); sys.exit(1)

    def nloop(self):
        self.client.packet_write()     # flush write buffer (messages sent to MQTT server)
        self.client.loop()             # fill read buffer   (enqueue received messages)
        return self.client.pop_event() # return 1st received message (dequeued)

    def run(self):
        self.client.subscribe('exabgp/command', qos=1)
        ret = self.nloop()
        if not isinstance(ret, EventSuback):
            logging.error('SUBACK not received')
            sys.exit(2)
        logging.info('granted qos is %s', ret.granted_qos[0])
        while True:
            evt = self.nloop() 
            if isinstance(evt, EventPublish):
                logging.info('we received a message: {0} (topic= {1})'.format(evt.msg.payload, evt.msg.topic))
                # received message is either qos 0 or 1
                # in case of qos 1, we must send back PUBACK message with same packet-id
                if evt.msg.qos == 1:
                    self.client.puback(evt.msg.mid)
                try:
                    json_msg = json.loads(str(evt.msg.payload))
                except:
                    logging.error("JSON decode error: %s", evt.msg.payload)
                    continue
                if json_msg.get('command'):
                    logging.info("Receive Command: %s", json_msg.get('command'))
                    stdout.write('%s\n' % json_msg.get('command'))
                    stdout.flush()
    

class message_parsing_thread(threading.Thread):
    def __init__(self, threadID, queueLock, workQueue):
        threading.Thread.__init__(self)
        self.client = Nyamuk("exabgp_client", server="127.0.0.1", log_level=logging.WARNING)
        #ret = client.connect(version=4)
        ret = self.client.connect()
        ret = self.nloop() # ret should be EventConnack object
        if not isinstance(ret, EventConnack) or ret.ret_code != 0:
            logging.error("Cannot connect to mqtt server"); sys.exit(1)
        self.queueLock = queueLock
        self.workQueue = workQueue
        self.threadID = threadID

    def nloop(self):
        self.client.packet_write()     # flush write buffer (messages sent to MQTT server)
        self.client.loop()             # fill read buffer   (enqueue received messages)
        return self.client.pop_event() # return 1st received message (dequeued)

    def run(self):
        while True:
            self.queueLock.acquire()
            while not self.workQueue.empty():
                line = self.workQueue.get()
                logging.debug("parsing line '%s'", line)
                # Parse JSON string  to dictionary
                try:
                    temp_message = json.loads(line)
                except:
                    logging.error("JSON decode error: %s", line)
                    continue
         
                msg_type = temp_message.get('type', None)
		if not msg_type:
                    logging.error("Message Type not defined in JSON %s. Skipping", temp_message)
                    continue
                msg_type = msg_type.encode('ascii', 'ignore')
                logging.debug("found msg type %s", msg_type)

                if msg_type == 'notification':
                    self.client.publish('exabgp/notification', line, qos=0)
                else:
                    neighbor = temp_message.get('neighbor', {}).get('ip', None)
                    if not neighbor:
                        logging.error("Message Type not defined in JSON %s. Skipping", temp_message)
                        continue
                    neighbor = neighbor.encode('ascii', 'ignore').replace('.', '_')
                    logging.debug("neighbor %s", neighbor)
                    
                    topic = 'exabgp/neighbor/%s/%s' % (neighbor, msg_type)
                    logging.debug("topic %s", topic)

                    self.client.publish(topic, line, qos=0)
            self.queueLock.release()
            ret = self.nloop() # ret should be EventPuback
    
#Start message_parser_thread
threadID = 1;
tx_thread = message_parsing_thread(threadID, queueLock, workQueue)
tx_thread.daemon=True
tx_thread.start()
threadID = threadID +1;
rx_thread = command_thread(threadID)
rx_thread.daemon=True
rx_thread.start()

counter = 0
#main loop
while True:
    try:
        line = stdin.readline().strip()
        
        # When the parent dies we are seeing continual newlines, so we only access so many before stopping
        if line == "":
            counter += 1
            if counter > 100:
                break
            continue
        counter = 0
        
        queueLock.acquire()
        workQueue.put(line)
        queueLock.release()

    except KeyboardInterrupt:
        logging.warning('KeyboardInterrupt Caught')
        pass
    except IOError:
        # most likely a signal during readline
        pass
    except SystemExit:
        logging.warning('SystemExit Caught, quitting')
