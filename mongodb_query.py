from pymongo import MongoClient

client = MongoClient()

db = client.exabgp

cursor = db['192.168.69.71'].find({"prefix": "46.34.136.0/21"})
#cursor = db['192.168.69.71'].find()

for document in cursor:
  print document
