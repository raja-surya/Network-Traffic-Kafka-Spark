from time import sleep
from struct import *
from kafka import KafkaProducer
import pcapy


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: x.encode('utf-8'))

print("Created Producer\n")

cap = pcapy.open_live("enp0s3", 65536 , 1 , 0)
count = 1

print("Started Capture\n")
 
while True:
 (header, payload) = cap.next()

 l2hdr = payload[:14]
 l2data = unpack("!6s6sH", l2hdr)

 srcmac = "%.2x:%.2x:%.2x:%.2x:%.2x:%.2x" % (l2hdr[0],\
          l2hdr[1], l2hdr[2], l2hdr[3], l2hdr[4], l2hdr[5])
 dstmac = "%.2x:%.2x:%.2x:%.2x:%.2x:%.2x" % (l2hdr[6],\
          l2hdr[7], l2hdr[8], l2hdr[9], l2hdr[10],l2hdr[11])

 msg = str(count) + ','+ str(srcmac) + ',' + str(dstmac)
 print(count, " ", msg)
 count+=1

 producer.send('pkttest', msg)
 sleep(0.5)

