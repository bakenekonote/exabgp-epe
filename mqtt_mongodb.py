import sys
import time
import json
import os
import threading
import Queue
import logging
from sys import stdin, stdout
from datetime import datetime
import pymongo
from nyamuk.nyamuk import *
from nyamuk.event import *

class mqtt_rx_thread(threading.Thread):
    def __init__(self, threadID, queueLock, workQueue, stat):
        threading.Thread.__init__(self)
        self.queueLock = queueLock
        self.workQueue = workQueue
        self.threadID = threadID
        self.stat = stat
        self.client = Nyamuk("exabgp_mongo_client", server="127.0.0.1", log_level=logging.WARNING)
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
        global stop
        self.client.subscribe('#', qos=1)
        ret = self.nloop()
        if not isinstance(ret, EventSuback):
            logging.error('SUBACK not received')
            sys.exit(2)
        logging.debug('granted qos is %s', ret.granted_qos[0])
        while True:
            evt = self.nloop() 
            if isinstance(evt, EventPublish):
                logging.debug('we received a message: {0} (topic= {1})'.format(evt.msg.payload, evt.msg.topic))
                # received message is either qos 0 or 1
                # in case of qos 1, we must send back PUBACK message with same packet-id
                if evt.msg.qos == 1:
                    self.client.puback(evt.msg.mid)
                try:
                    json_msg = json.loads(str(evt.msg.payload))
                except:
                    logging.error("JSON decode error: %s", evt.msg.payload)
                    continue
                self.queueLock.acquire()
                self.stat['mq_rx'] = self.stat['mq_rx'] + 1
                self.workQueue.put(json_msg)
                self.queueLock.release()

class mongodb_thread(threading.Thread):
    def __init__(self, threadID, queueLock, workQueue, stat):
        threading.Thread.__init__(self)
        self.queueLock = queueLock
        self.workQueue = workQueue
        self.threadID = threadID
        self.mongo_client = pymongo.MongoClient("mongodb://127.0.0.1:27017")
        self.db = self.mongo_client.exabgp
        self.stat = stat

    def run(self):
        while True:
            self.queueLock.acquire()
            while not self.workQueue.empty():
                json_msg = self.workQueue.get()
                msg_type = json_msg.get('type')

                if msg_type == "notification":
                    neighbor = json_msg.get('neighbor')
                    if neighbor:
                        neighbor_ip = neighbor.get('ip')
                        logging.warning("Receive BGP Notification from %s, flushing DB", neighbor_ip)
                        self.db[neighbor_ip].drop()
                        self.db[neighbor_ip].create_index([("prefix", pymongo.ASCENDING)])
                        self.stat['db_drop'] = self.stat['db_drop'] + 1

                if msg_type == "open":
                    neighbor_ip = json_msg.get('neighbor').get('ip')
                    logging.warning("Receive BGP open from %s, flushing DB", neighbor_ip)
                    self.db[neighbor_ip].drop()
                    self.db[neighbor_ip].create_index([("prefix", pymongo.ASCENDING)])
                    self.stat['db_drop'] = self.stat['db_drop'] + 1
                    
                if msg_type == "update":
                    neighbor_ip = json_msg.get('neighbor').get('ip')
                    logging.debug("Receive BGP Update from %s", neighbor_ip)
                    r = self.db[neighbor_ip]
                    
                    withdraw = json_msg.get('neighbor').get('message').get('update').get('withdraw')
                    if withdraw:
                        attr = json_msg.get('neighbor').get('message').get('update').get('attribute')
                        for family in json_msg.get('neighbor').get('message').get('update').get('withdraw'):
                            for prefix in json_msg.get('neighbor').get('message').get('update').get('withdraw').get(family):
                                result = r.delete_many({'prefix': prefix, 'family':family})
                                logging.debug("del family %s, prefix %s, #records %s", family, prefix, result.deleted_count)
                                self.stat['db_delete'] = self.stat['db_delete'] + 1

                    announce = json_msg.get('neighbor').get('message').get('update').get('announce')
                    if announce:
                        attr = json_msg.get('neighbor').get('message').get('update').get('attribute')
                        for family in json_msg.get('neighbor').get('message').get('update').get('announce'):
                            for nh in json_msg.get('neighbor').get('message').get('update').get('announce').get(family):
                                for prefix in json_msg.get('neighbor').get('message').get('update').get('announce').get(family).get(nh):
                                    result = r.replace_one({'prefix' : prefix}, dict(prefix=prefix, nh=nh, family=family, attribute=attr), upsert=True)
                                    if result.upserted_id:
                                        logging.debug("add famiy %s i, prefix %s, nh %s, insert id %s", family, prefix, nh, result.upserted_id)
                                    else :
                                        logging.debug("add famiy %s i, prefix %s, nh %s, #updated records %s", family, prefix, nh, result.modified_count)
                                    self.stat['db_replace'] = self.stat['db_replace'] + 1

            self.queueLock.release()

#main thread starts here:
def main():
    logging.basicConfig(level=logging.INFO)
    queueLock = threading.Lock()
    workQueue = Queue.Queue(100000)
    stat = dict(mq_rx=0, db_replace=0, db_delete=0, db_drop=0)
    
    stop = False

    #Start message_parser_thread
    db_thread = mongodb_thread("db_threqd", queueLock, workQueue, stat)
    db_thread.daemon=True
    db_thread.start()
    mq_thread = mqtt_rx_thread("mq_thread", queueLock, workQueue, stat)
    mq_thread.daemon=True
    mq_thread.start()
    try:
        while True:
            queueLock.acquire()
            logging.info("Queue Length: %s ", workQueue.qsize())
            logging.info("Stat: %s ", stat)
            queueLock.release()
            time.sleep(10)
    except (KeyboardInterrupt, SystemExit):
            logging.error('\n! Received keyboard interrupt, quitting threads.\n')

main()