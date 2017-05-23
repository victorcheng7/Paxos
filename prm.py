import socket
import time
import threading
import sys
import Queue
import copy

'''
******************* 
To help debug, print out every receive and send message and who you're sending to
States I need to add to PRM - stop/resume, send update messages to all outgoing

TODO
make a modular function that loops through and sends all the relevant log entries one by one
make a modular function that does a timeout and checks to see if you have majority
1) accept 2) acknowledge
Problem - How do I detect majority accept/ack or Not? 
(Wait 400ms to make sure it didn't actually get majority accept or ack messages, time.sleep random # before reprposing)
*******************
 if replicate:
    prm.proposedFile = the second part of the replicate message that was validated by CLI
    prm.ballot[0] += 1 #change ballot number
    send all prms Message(prm.ballot, prm.id, index, "PREPARE")
    TODO ORIGINAL PRM SHOULD BE LISTENING TO MAJORITY OF ACKNOWLEDGEMENTS, IF NOT REPROPOSE
idea 1- 
if stop: 
    tear down all incoming channels, so you can only listen to cmds from CLI
if resume: 
    reestablish all connections
idea 2 - 
if stop: 
    Add an extra if statement validation on resume/stop state of PRM on commThread()
    Any message taken in, don't do anything 
if resume: 
    resume as normal inside of commThread
    if state = replicate: 
        send proposal to everyone 
    if state != replicate: 
        upon receiving a proposal, if index they're proposing > last index in ur array, ask that node for updates

if total, print, merge EASY: 
    add up total words inside each files 



if Prepare: 
	if msg.index < prm.index: 
		send("update", log entries and corresponding index) to msg.source_id
    if msg.ballotTuple > prm.ballotTuple:
        if index of proposal > prm.index:
            ask that source_id for your missing entries in log
        if index of proposal < prm.index: 
            update the source_id all the entries in the log before the proposal
        prm.ballotTuple = msg.ballotTuple
        send Ack(prm.id, prm.ballotTuple, acceptTuple, acceptVal, index, msg.originalPRM ACK) to original preparer
if Ack: 
    Keep track array of array of ACK messages objects
    prm.ackArray.append(msg)
    if len(prm.ackarray) + 1 > upper(0.5*prm.num_nodes):
        tempAcceptTuple = [None, None] #message with the highest acceptTuple
        tempAcceptVal = None #acceptVal with the highest acceptTuple
        isValue = false
        for msgs messages prm.ackArray: 
            check if there are any messages that contains acceptVal 
            if there are:
                isValue = true
                #max of acceptTuple for ACK messages array, 
                tempAcceptTuple = msg with highest acceptTuple.acceptTuple 
                tempAcceptVal = msg with highest acceptTuple.acceptVal    

        if counter == 0:
            prm.acceptTuple = prm.ballotTuple
            prm.acceptVal = prm.proposedFile
        else: 
            prm.acceptTuple = tempAcceptTuple
            prm.acceptVal = tempAcceptVal
        for all outgoing channels:
            send(prm.id, prm.ballotTuple, prm.acceptTuple, prm.acceptVal, prm.index, originalPRM, "ACCEPT")
            TODO ORIGINAL PRM SHOULD BE LISTENING FOR ACCEPTS FROM MAJORITY IF NOT REPROPOSE

if ACCEPT: 
    if msg.index > prm.index: 
       	send(prm.id, None, None, None, None, None, None, "UPDATE") #Ask for entire log
    if msg.ballot >= prm.ballotTuple: 
        prm.acceptTuple = msg.acceptTuple 
        prm.acceptVal = msg.acceptVal
        for all outgoing channels:
            send(prm.id, prm.ballotTuple, None, prm.acceptVal, prm.index,  "ACCEPT")
        if msg.originalPRM == prm.id: 
            prm.numAccepts += 1
        if prm.numAccepts >= upper(prm.num_nodes/2): 
            log[msg.index] = prm.proposedFile
            for all outgoing channels: 
                send(prm.id, None, None, prm.proposedFile, prm.index, msg.originalPRM, None, "DECIDE")
            send("finished with replicate and your file successfully added to log") to CLI
    
if DECIDE: 
	if msg.index > prm.index:
		send(prm.id, prm.index, "UPDATE") to msg.originalPRM
	prm.log[prm.index] = msg.acceptVal #filename
	prm.index += 1
	prm.newRoundCleanUp()

if UPDATE:
	if msg.index < prm.index:
		#update msg.source_id
		counter = prm.index-msg.index
		while counter != prm.index + 1
			send(prm.id, counter, prm.log[counter], "UPDATE") to msg.source_id #send a bunch of corresponding log entries
	elif (msg.index > prm.index) and (msg.log != None):
		#please update me
		prm.log[msg.index] = msg.log 			
'''

def main():
	if(len(sys.argv) != 3):
		print("USAGE: python [prm_id] [setup_file]")
		exit(1)
	prm_id = int(sys.argv[1])
	prm = Prm(prm_id)
	setup_file = sys.argv[2]
	setup(prm, setup_file)

	while True:
		pass
	#site.checkIncomingChannels

	# Do all the paxos protocol related stuff

	#commThread(site)

	#assume that all connections are there prm and cli connections


def commThread(prm):
	while (len(prm.incoming_channels) < (prm.num_nodes-1)) or prm.cli[0] == None:
		for con in prm.incoming_channels_unordered:
			try:
				data = con.recv(1024)
				if data == "confirmInit":
					prm.cli[0] = con
					for dest_id, sock in prm.outgoing_channels.iteritems():
						sock.send("sendID from {0}".format(prm.id))

				# add to ordered incoming channels 
				elif data.split()[0] == "sendID":
					src = int(data.split()[2])
					prm.incoming_channels[src] = con
			except socket.error, e:
				continue

	
	# separate cli from prm connections
	prm.cli[1] = prm.outgoing_channels[prm.id]
	prm.outgoing_channels.pop(prm.id, None)
	prm.incoming_channels.pop(prm.cli[0], None)


# MAKE SURE CORRESPOND TO THIS FORMAT 	def __init__(self, source_id, ballot, acceptTuple, acceptVal, index, originalPRM, log, type):
	while True:
		incomingChannelIndex = 0
		for node_id, con in prm.incoming_channels.iteritems():  
			#node_id is the source_id of the message
			try:
				data = con.recv(1024)
				print data  
				for msg in Message.split(data):
					msg = Message.reconstructFromString(msg.strip())
					if msg.type == Message.PREPARE:
						print msg
			except socket.error, e:
				continue
			incomingChannelIndex += 1

		  # listen to cli
		try:
			data = prm.cli[0].recv(1024)
			print "receive {0} from cli".format(data)
			if data == "replicate!":
					for dest_id, sock in prm.outgoing_channels.iteritems():
						sock.send("replicate")
		except socket.error, e:
			pass

def setup(prm, setup_file):
	#Read setup file. ex - setup.txt  
	with open(setup_file, 'r') as f:
	  N = int(f.readline().strip())
	  print N
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

class Ballot(object):
	def __init__(self, ballot1, ballot2):
		self.ballot1 = ballot1
		self.ballot2 = ballot2


class Message(object):
	PREPARE = 0
	ACK = 1
	ACCEPT = 2
	DECIDE = 3
	UPDATE = 4
	def __init__(self, source_id, ballot, acceptTuple, acceptVal, index, originalPRM, log, type):
		self.source_id = source_id
		self.ballot = ballot
		self.acceptTuple = acceptTuple
		self.acceptVal = acceptVal
		self.index = index
		self.originalPRM = originalPRM
		self.log = log
		self.type = type

	def __str__(self):
		res = str(self.source_id) + " " + str(self.ballot) + " " + str(self.index) + " " + str(self.type) 
		if self.acceptTuple != None:
			res += " " + str(self.acceptTuple)
		if self.acceptVal != None: 
			res += " " + str(self.acceptVal)
		if self.originalPRM != None:
			res += " " + str(self.originalPRM)
		if self.log != None:
			res += " " + str(self.log)
		res += "||"
		return res

	def __repr__(self):
		return self.__str__()

	@staticmethod
	def reconstructFromString(str):
		keyWords = str.strip().split()
		source_id = int(keyWords[0])
		ballotTuple = keyWords[1]
		index = int(keyWords[2])
		msg_type = int(keyWords[3])
		acceptTuple = None
		acceptVal = None
		originalPRM = None
		log = None
		if msg_type == Message.ACK:
			acceptTuple = int(keyWords[4])
			acceptVal = int(keyWords[5])
			originalPRM = int(keyWords[6])
		if msg_type == Message.ACCEPT:
			acceptTuple = int(keyWords[4]) #this is Done Process ID
			acceptVal = int(keyWords[5])
			originalPRM = int(keyWords[6])
		if msg_type == Message.UPDATE:
			log = str(keyWords[4])
		return Message(source_id, ballotTuple, acceptTuple, acceptVal, index, originalPRM, log, msg_type)

	@staticmethod
	def split(str):
		res = []
		for msg in str.strip().split("||"):
			res.append(msg)
		del res[-1]
		return res

class Prm(object):
	def __init__(self, site_id):
		self.id = site_id
		self.addr_book = []
		self.incoming_channels_unordered = []
		self.incoming_channels = {}
		self.outgoing_channels = {}
		self.cli = [None]*2 #format: [incoming,outgoing]
		self.listeningSocket = None
		self.done_processes = set()

		self.proposedFile = None
		self.num_nodes = 0
		self.numAccepts = 1
		self.ballotTuple = [0, site_id]
		self.acceptTuple = [None, None]
		self.acceptVal = None
		self.index = 0
		self.ackArray = []
		self.log = []
	def newRoundCleanUp(self):
		self.proposedFile = None
		self.numAccepts = 1
		self.numAcks = 1
		self.ballot = Ballot(0, site_id)
		#self.ballotTuple = [0, site_id]
		self.acceptTuple = [None, None]
		self.acceptVal = None
		sellf.ackArray = []
		
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
		while len(self.incoming_channels_unordered) != self.num_nodes:
			try:
				con, addr = self.listeningSocket.accept()
				con.setblocking(0)
				self.incoming_channels_unordered.append(con)
			except socket.error:
				continue           




	def execute(self, command):
		self.checkIncomingMsgs()
		keyWords = command.split()
		if "send" == keyWords[0]:
			dest_id = int(keyWords[1])
			amount = int(keyWords[2])
			self.sendMoney(dest_id, amount)
		elif "snapshot" == keyWords[0]:
			self.startSnapshot()
		elif "sleep" == keyWords[0]:
			time = float(keyWords[1])
			self.sleep(time)
		else:
			print "CANNOT RECOGNIZE THE COMMAND: " + command
			exit(1)
		self.checkIncomingMsgs()


	def sendProposal(self, dest_id, site_id):
			#def __init__(self, source_id, ballotTuple, acceptTuple, acceptVal, index, type):
		self.ballot[0] += 1
		msg = Message(self.id, self.ballot, None, None, len(self.log), Message.PREPARE)
		self.outgoing_channels[dest_id].send(str(msg))
		'''
			def __init__(self, source_id, ballot_num, accept_num, accept_val, index, type):
			self.num_nodes = 0
				self.numAccepts = 0
				self.numAcks = 0
				self.ballotTuple = (0, 0)
				self.acceptTuple = (0, 0)
				self.acceptVal = 0
				self.logs = []

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
						if msg.type === Message.PREPARE:
		'''

	def sendMarkers(self, snap_id):
		msg = Message(self.id, snap_id, None, Message.MARKER_TYPE)
		for dest_id, sock in self.outgoing_channels.iteritems():
			sock.send(str(msg))

	def sendDone(self, done_process_id):
		#Piggy Back amount with done_process_id
		msg = Message(self.id, None, done_process_id, Message.DONE_TYPE)
		for dest_id, sock in self.outgoing_channels.iteritems():
			sock.send(str(msg))

	def startSnapshot(self):
		self.snap_count += 1
		counter = 0
		snap_id = str(self.id) + "." + str(self.snap_count)
		site_state = self.balance #Take Local State Snapshot
		incoming_channels_states = copy.deepcopy(self.SnapIDTableLastEntryTemplate)
		self.snapID_table[snap_id] = [counter, site_state, incoming_channels_states]
		self.sendMarkers(snap_id)


	def checkIncomingMsgs(self):
		BUF_SIZE = 1024
		for con in self.incoming_channels:
			try:
				msgs = con.recv(BUF_SIZE)
				for msg in Message.split(msgs):
					msg = Message.reconstructFromString(msg.strip())
					if msg.type == Message.MARKER_TYPE:
						if msg.snap_id not in self.snapID_table: #First Marker -> input into snapID_table
							counter = 1
							site_state = self.balance #Take Local State Snapshot
							incoming_channels_states = copy.deepcopy(self.SnapIDTableLastEntryTemplate)
							self.snapID_table[msg.snap_id] = [counter, site_state, incoming_channels_states]
							self.snapID_table[msg.snap_id][2][msg.source_id][0] = True #Shut down the incoming channel of 1st Marker
							self.sendMarkers(msg.snap_id)
							if self.snapID_table[msg.snap_id][0] == len(self.incoming_channels): #Received all markers
								self.outputLocalSnapshotAt(msg.snap_id)
						else: #Not the first marker
							self.snapID_table[msg.snap_id][0] += 1 #increase counter by 1
							self.snapID_table[msg.snap_id][2][msg.source_id][0] = True #Take no more Snapshot on the channel
							if self.snapID_table[msg.snap_id][0] == len(self.incoming_channels): #Received all markers
								self.outputLocalSnapshotAt(msg.snap_id)
					elif msg.type == Message.MONEY_TRANSFER_TYPE: #received a msg
						self.balance += msg.amount #Fix the real-time balance
						for _, v in self.snapID_table.iteritems():
							if v[0] == len(self.incoming_channels): #Finished Snapshot
								continue
							if v[2][msg.source_id][0] == False: #The current money message is before Marker
								v[2][msg.source_id][1] += msg.amount #Record the increase in amount
					elif msg.type == Message.DONE_TYPE:
						#Piggy back ammount as done_process_id
						done_process_id = msg.amount
						if done_process_id not in self.done_processes:
							self.done_processes.add(done_process_id)
							self.sendDone(done_process_id)
					else:
						print "ERROR: DO NOT UNDERSTAND MESSAGE TYPE"
						print msg
						exit(1)
			except socket.error, e:
				continue

main()
