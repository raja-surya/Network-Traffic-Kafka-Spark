from time import sleep
from struct import *
from kafka import KafkaProducer
import MyScapyExtract as myscap


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: x.encode('utf-8'))

print("Created Producer\n")

file0 = 'laptop-pcap.pcap'
packets = myscap.scapy_read_packets(file0)


datalst = myscap.parse_scapy_packets(packets)
print(datalst[0:2])
print(len(datalst))

count = 1 

for i in range(len(datalst)):
	pkt = datalst[i]

	if (pkt['etype'] == '2048'):
		isrc = pkt['isrc']
		idst = pkt['idst']
		iproto = pkt['iproto']
      
		if (iproto == '17'):
			sport = pkt['utsport']
			dport = pkt['utdport']
		else:
			sport = pkt['tsport']
			dport = pkt['tdport']
     
		msg = str(count) + ',' + str(isrc) + ',' + str(idst) + \
		',' + str(iproto) + ',' + str(sport) + \
                ',' + str(dport) 
		print(msg)
		count+=1 

		producer.send('pkttest_pcap', msg)
		sleep(1)


