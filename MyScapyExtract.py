## A function to parse the packets read by scapy and convert in to a list of dictionary items 
### so that the list can be converted in to a pandas data frame.

from scapy.all import *
import datetime
import pandas as pd


def scapy_read_packets(file0):
    return (rdpcap(file0))


def parse_scapy_packets(packetlist):
    
    count=0
    datalist=[]
    
    for packet in packetlist:
        dpack={}
        dpack['id'] = str(count)
        dpack['len']= str(len(packet))
        dpack['timestamp'] = datetime.datetime.fromtimestamp(packet.time)\
                                .strftime('%Y-%m-%d %H:%M:%S.%f')
        
        if packet.haslayer(Ether):
            dpack.setdefault('esrc', packet[Ether].src)
            dpack.setdefault ('edst', packet[Ether].dst)
            dpack.setdefault('etype', str(packet[Ether].type))
            
        if packet.haslayer(Dot1Q):
            dpack.setdefault('vlan', str(packet[Dot1Q].vlan))
                                 
        if packet.haslayer(IP):
            dpack.setdefault('isrc', packet[IP].src)
            dpack.setdefault('idst', packet[IP].dst)
            dpack.setdefault('iproto', str(packet[IP].proto))
            dpack.setdefault('iplen', str(packet[IP].len))
            dpack.setdefault('ipttl', str(packet[IP].ttl))
            
        if packet.haslayer(TCP):
            dpack.setdefault('tsport', str(packet[TCP].sport))
            dpack.setdefault('tdport', str(packet[TCP].dport))
            dpack.setdefault('twindow', str(packet[TCP].window))
            
        if packet.haslayer(UDP):
            dpack.setdefault('utsport', str(packet[UDP].sport))
            dpack.setdefault('utdport', str(packet[UDP].dport))
            dpack.setdefault('ulen', str(packet[UDP].len))
            
        if packet.haslayer(ICMP):
            dpack.setdefault('icmptype', str(packet[ICMP].type))
            dpack.setdefault('icmpcode', str(packet[ICMP].code))
        
        if packet.haslayer(IPerror):
            dpack.setdefault('iperrorsrc', packet[IPerror].src)
            dpack.setdefault('iperrordst', packet[IPerror].dst)
            dpack.setdefault('iperrorproto', str(packet[IPerror].proto))
            
        if packet.haslayer(UDPerror):
            dpack.setdefault('uerrorsrc', str(packet[UDPerror].sport))
            dpack.setdefault('uerrordst', str(packet[UDPerror].dport))
                              
        if packet.haslayer(BOOTP):
            dpack.setdefault('bootpop', str(packet[BOOTP].op))
            dpack.setdefault('bootpciaddr', packet[BOOTP].ciaddr)
            dpack.setdefault('bootpyiaddr', packet[BOOTP].yiaddr)
            dpack.setdefault('bootpsiaddr', packet[BOOTP].siaddr)
            dpack.setdefault('bootpgiaddr', packet[BOOTP].giaddr)
            dpack.setdefault('bootpchaddr', packet[BOOTP].chaddr)
                              
        if packet.haslayer(DHCP):
            dpack.setdefault('dhcpoptions', packet[DHCP].options)
                              
        if packet.haslayer(ARP):
            dpack.setdefault('arpop', packet[ARP].op)
            dpack.setdefault('arpsrc', packet[ARP].hwsrc)
            dpack.setdefault('arpdst', packet[ARP].hwdst)                  
            dpack.setdefault('arppsrc', packet[ARP].psrc)
            dpack.setdefault('arppdst', packet[ARP].pdst)
            
        if packet.haslayer(NTP):
            dpack.setdefault('ntpmode', str(packet[NTP].mode))
            
        if packet.haslayer(DNS):
            dpack.setdefault('dnsopcode', str(packet[DNS].opcode))
            
        if packet.haslayer(SNMP):
            dpack.setdefault('snmpversion', packet[SNMP].version)
            dpack.setdefault('snmpcommunity', packet[SNMP].community)
            
        datalist.append(dpack)
        count+=1
        
    return datalist 