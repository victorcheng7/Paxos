import socket
import time
import threading
import sys
import Queue
import copy
 
def main():
    if(len(sys.argv) != 3):
        print("USAGE: python [prm_id] [setup_file]")
        exit(1)
    prm_id = int(sys.argv[1])
    print "I am prm {0}".format(prm_id)
    prm = Prm(prm_id)
    setup_file = sys.argv[2]
    setup(prm, setup_file)
    print "Setup success"

    
    while True:
        pass
    #prm.checkIncomingChannels

    # Do all the paxos protocol related stuff

    #commThread(prm)

    #assume that all connections are there prm and cli connections


def commThread(prm):


    # make sure all incoming channels are initialized first
    while len(prm.incoming_channels) < prm.num_nodes:
        for con in prm.incoming_channels:  
            try:
                data = con.recv(1024)
                if data == "confirmInit":
                    prm.cli[0] = con
                    for dest_id, sock in prm.outgoing_channels.iteritems():
                        sock.send("confirmInit!")

            except socket.error, e:
                continue

    # separate cli from prm connections
    prm.cli[1] = prm.outgoing_channels[prm.id]
    prm.outgoing_channels.pop(prm.id, None)
    prm.incoming_channels.remove(prm.cli[0])

    #start listening
    while True:


        #listen to prms
        for con in prm.incoming_channels:  
            try:
                data = con.recv(1024)
                if data != "confirmInit!":
                    print data  

                

            except socket.error, e:
                continue

        # listen to cli
        try:
            data = prm.cli[0].recv(1024)
            print "receive {0} from cli".format(data)
            if data == "replicate!":
                    for dest_id, sock in prm.outgoing_channels.iteritems():
                        sock.send("replicate received from a prm")

        except socket.error, e:
            pass


def setup(prm, setup_file):
    #Read setup file. ex - setup.txt  
    with open(setup_file, 'r') as f:
      N = int(f.readline().strip())
      prm.num_nodes = N
      process_id = 0
      for line in f.readlines():
          process_id += 1
          if process_id <= N:
              IP1, port1, IP2, port2 = line.strip().split()
              port1 = int(port1)
              port2 = int(port2)

              if process_id == prm.id:
                  prm.addr_book.append( (IP1, port1) )
                  prm.openListeningSocket( IP2, port2) #open for traffic

              else:
                  prm.addr_book.append( (IP2, port2) )

              prm.addOutgoingChannel(process_id)

    prm.openOutgoingChannels()
    
    # start commThread
    cThread = threading.Thread(target = commThread, args=(prm,))
    cThread.daemon = True
    cThread.start()

    prm.openIncomingChannels()

    # separate cli

class Message(object):
    PREPARE = 0
    ACK = 1
    ACCEPT = 2
    DECIDE = 3
    def __init__(self, source_id, ballot_num, accept_num, accept_val, index, type):
        self.source_id = source_id
        self.ballot_num = ballot_num
        self.accept_num = accept_num
        self.accept_val = accept_val
        self.index = index
        self.type = type

    def __str__(self):
        res = str(self.source_id) + " " + str(self.ballot_num) + " " + str(index) + " " + str(self.type) 
        if self.accept_num != None:
            res += " " + str(self.accept_num)
        if self.accept_val != None: 
            res += " " + str(self.accept_val)
        res += "||"
        return res

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def reconstructFromString(str):
        keyWords = str.strip().split()
        source_id = int(keyWords[0])
        ballot_num = keyWords[1]
        index = int(keyWords[2])
        msg_type = int(keyWords[3])
        accept_num = None
        accept_val = None
        if msg_type == Message.ACK:
            accept_num = int(keyWords[4])
            accept_val = int(keyWords[5])
        if msg_type == Message.ACCEPT:
            accept_num = int(keyWords[4]) #this is Done Process ID
            accept_val = int(keyWords[5])
        return Message(source_id, ballot_num, accept_num, accept_val, index, msg_type)

    @staticmethod
    def split(str):
        res = []
        for msg in str.strip().split("||"):
            res.append(msg)
        del res[-1]
        return res

class Prm(object):
    def __init__(self, prm_id):
        self.id = prm_id
        self.addr_book = []
        self.incoming_channels = []
        self.outgoing_channels = {}
        self.cli = [None]*2 # format: [incoming, outgoing]
        self.listeningSocket = None
        self.done_processes = set()

        self.num_nodes = 0
        self.numAccepts = 0
        self.numAcks = 0
        self.ballotTuple = (None, None)
        self.acceptTuple = (None, None)
        self.acceptVal = 0
        self.logs = []

    def openListeningSocket(self, IP, port):
        self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listeningSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listeningSocket.bind( (IP, port) )
        self.listeningSocket.setblocking(0) 
        self.listeningSocket.listen(1)

    def addOutgoingChannel(self, dest_id):
        self.outgoing_channels[dest_id] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def openOutgoingChannels(self):
        #time.sleep(5) #To make sure all other processes are up
        for dest_id, sock in self.outgoing_channels.iteritems():
            while True:
                try: 
                    sock.connect(self.addr_book[dest_id - 1])
                    break
                except Exception:
                    continue

    def openIncomingChannels(self):
        while len(self.incoming_channels) != self.num_nodes:
            try:
                con, addr = self.listeningSocket.accept()
                con.setblocking(0)
                self.incoming_channels.append(con)
            except socket.error:
                continue            
    #               PREPARE = 0
    # ACK = 1
    # ACCEPT = 2
    # DECIDE = 3
    # def __init__(self, source_id, ballot_num, accept_num, accept_val, index, type):
    #   self.source_id = source_id
    #   self.ballot_num = ballot_num
    #   self.accept_num = accept_num
    #   self.accept_val = accept_val
    #   self.index = index
    #   self.type = type
    #               if msg.type === Message.PREPARE:
                        
                        

main()
