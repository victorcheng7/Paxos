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

 if replicate:
    prm.proposedFile = the second part of the replicate message that was validated by CLI
    prm.ballot[0] += 1 #change ballot number
    send all prms Message(prm.ballot, prm.id, index, "PREPARE")
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


if ACCEPT: 
    if msg.index > prm.index: 
       	send(prm.id, None, None, None, None, None, None, "UPDATE") #Ask for entire log
    if msg.ballot >= prm.ballot: 
        prm.acceptTuple = msg.acceptTuple 
        prm.acceptVal = msg.acceptVal
        for all outgoing channels:
            send(prm.id, prm.ballot, None, prm.acceptVal, prm.index,  "ACCEPT")
        if msg.originalPRM == prm.id: 
            prm.numAccepts += 1
        if prm.numAccepts >= upper(prm.num_nodes/2): 
            log[msg.index] = prm.proposedFile
            for all outgoing channels: 
                send(prm.id, None, None, prm.proposedFile, prm.index, msg.originalPRM, None, "DECIDE")
            send("finished with replicate and your file successfully added to log") to CLI
    if it's the first time receiving accept for a given ballot number, send to all other num_nodes
    otherwise don't send anything
    
if DECIDE: 
	if msg.index > prm.index:
		send(prm.id, prm.index, "UPDATE") to msg.originalPRM
	prm.log[prm.index] = msg.acceptVal #filename
	prm.index += 1
	prm.newRoundCleanUp()

if UPDATE:
	if msg.index < prm.index:
		#update msg.source_id
		counter = msg.index
		while counter != prm.index + 1: #send all missing log entries one by one
			send(prm.id, counter, prm.log[counter], "UPDATE") to msg.source_id 
			counter += 1
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
	prm.cli[1].send("finishedSetup")

# msg format - def __init__(self, source_id, ballot, acceptTuple, acceptVal, index, originalPRM, log, type):
	while True:
		'''
		Questions when should you ask for update? and when should you send an update automatically? Look at every mendMessage, sendPrepare, etc.
		when should you send each message? prepare only after replicating. when would you receive a message? only receive prepare if a node wants
		to propose a value
		
		def sendAccept(self, dest_id):
			#TODO when constructing message object, originalPRM should be msg.originalPRM
		def sendDecide(self, dest_id):
			#TODO when constructing message object, originalPRM should be msg.originalPRM
			send CLI that you've decided on a value and what is it
		def sendUpdate(self, dest_id):
			#TODO when constructing message object, originalPRM should be None
		def checkMajorityAcks(self, msg):
			time.sleep(.4)
			#TODO checks to see if !majority accepts, repropose with time.sleep(random)
		def checkMajorityAccepts(self, msg):
			time.sleep(.4)
			#TODO checks to see if !majority acks, reproposes with time.sleep(random)
		Message(self, source_id, ballot, acceptTuple, acceptVal, index, originalPRM, log, type):

		'''
		for source_id, con in prm.incoming_channels.iteritems():  
			try:
				data = con.recv(1024)
				if prm.listening:
					#incoming messages from other PRMS
					for msg in Message.split(data):
						msg = Message.reconstructFromString(msg.strip())

						if msg.msgType == Message.PREPARE:
							print ("Received Prepare Message from ", msg.source_id, msg.ballot, msg.index, msg.originalPRM, msg.msgType)
							'''
							if msg.index < prm.index: #update source_id with missing log entries
								TODO update Moduralize? send("update", log entries and corresponding index) to msg.source_id
   							'''
   							msgBallotTuple = msg.ballot.split(",") # ex "1,0"
   							prmBallotTuple = prm.ballot.split(",") # ex. "1,0"
   							if ((msgBallotTuple[0] > prmBallotTuple[0]) or ((msgBallotTuple[0] == prmBallotTuple[0]) and (msgBallotTuple[1] > prmBallotTuple))):
							    	'''
								        if index of proposal > prm.index: #ask for update from source_id, cause you have missing log entries
											#TODO send("update", log entries and corresponding index) to msg.source_id
								        if index of proposal < prm.index: 
							            	#update the source_id all the entries in the log before the proposal
					           		'''
						   		prm.ballot = msg.ballot
						   		print "this is originalPRM", msg.originalPRM
								ackMsg = Message(prm.id, prm.ballot, prm.acceptTuple, msg.acceptVal, prm.index, msg.originalPRM, None, Message.ACK)
								prm.outgoing_channels[msg.source_id].send(str(ackMsg)) #send prepare message to original proposer
						    	print ("Sent Ack message to node ", dest_id)

						if msg.msgType == Message.ACK:
							print ("Received ACK Message from ", msg.source_id, msg.ballot, prm.acceptTuple, prm.acceptVal, msg.index, msg.originalPRM, msg.log, msg.msgType)
							if not prm.checkingMajorityAcks: 
								prm.checkingMajorityAcks = True
								prm.ackarray.append(msg)
								majorityAckThread = threading.Thread(target=prm.checkMajorityAcks)
								majorityAckThread.daemon = True
								majorityAckThread.start()
							else:
								prm.ackarray.append(msg)

						if msg.msgType == Message.ACCEPT:
							#send all outgoing channels same accept message, relay it. if it's the first time receiving it
							print "inside of Message.ACCEPT receive"
						if msg.msgType == Message.DECIDE:
							print "inside of Message.DECIDE receive"
							#send message back to CLI that the value has been decided.. if msg is decide
						if msg.msgType == Message.UPDATE:
							print "inside of Message.UPDATE receive"

			except socket.error, e:
				continue

		  # listen to cli
		try:
			data = prm.cli[0].recv(1024)
			splitData = data.split(" ")

			if prm.listening:
				print "received {0} from cli".format(data)
				if splitData[0] == "replicate": #ex. replicate words.txt
					time.sleep(1)
					prm.proposedFile = splitData[1]
					prm.incrementBallot()
					for dest_id, sock in prm.outgoing_channels.iteritems():#Send all prms a prepare message
						print ("Sent Prepare message to node ", dest_id)
						msg = Message(prm.id, prm.ballot, None, prm.proposedFile, prm.index, prm.id, None, Message.PREPARE)
						sock.send(str(msg))						
				elif data == "stop":
					print "Stopping PRM"
					prm.listening = False

			elif data == "resume":
				print "Resuming PRM"
				prm.listening = True
				#for dest_id, sock in prm.outgoing_channels.iteritems():
					#sendUpdate(everything in message)
				#send a message to all PRMs asking for updates

			else:
				prm.cli[1].send("stopped")
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
	print "setup finished"

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
	def __init__(self, source_id, ballot, acceptTuple, acceptVal, index, originalPRM, log, msgType):
		self.source_id = source_id
		self.ballot = ballot
		self.acceptTuple = acceptTuple
		self.acceptVal = acceptVal
		self.index = index
		self.originalPRM = originalPRM
		self.log = log
		self.msgType = msgType

	def __str__(self):
		res = str(self.source_id) + " " + str(self.ballot) + " " + str(self.index) + " " + str(self.msgType) 
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
		ballot = keyWords[1]
		index = int(keyWords[2])
		msgType = int(keyWords[3])
		acceptTuple = None
		acceptVal = None
		originalPRM = None
		log = None
		if msgType == Message.PREPARE: 
			acceptVal = keyWords[4]
			originalPRM = int(keyWords[5])
		if msgType == Message.ACK:
			acceptTuple = keyWords[4]
			acceptVal = keyWords[5]
			originalPRM = int(keyWords[6])
		if msgType == Message.ACCEPT:
			acceptVal = keyWords[4]
			originalPRM = int(keyWords[5])
		if msgType == Message.UPDATE:
			log = str(keyWords[4])
		return Message(source_id, ballot, acceptTuple, acceptVal, index, originalPRM, log, msgType)

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
		self.listening = True
		self.done_processes = set()

		self.proposedFile = None
		self.num_nodes = 0
		self.ballot = "0," + str(site_id) 
		self.acceptTuple =  "0,0" #Default acceptTuple if None
		self.acceptVal = "-1" #Default acceptVal if None
		self.index = 0
		self.ackarray = []
		self.acceptarray = []
		self.checkingMajorityAcks = False
		self.checkingMajorityAccepts = False
		self.log = []

	def newRoundCleanUp(self):
		self.proposedFile = None
		self.ballot = "0," + str(site_id) 
		#self.ballot = [0, site_id]
		self.acceptTuple = "0,0"
		self.acceptVal = None
		self.ackarray = []
		self.acceptarray = []

	def stop():
		self.listening = False
	def resume():
		self.listening = True


	def checkMajorityAcks(self):
		print "Checking to see if I have Majority Acks..."
		time.sleep(0.5) 
		if (len(self.ackarray) + 1) >= 0.5*(self.num_nodes+1):
			print "I have majority Acks!"
			highestAcceptNum = "0,0" #message with the highest acceptTuple
			highestAcceptVal = "-1" #acceptVal with the highest acceptTuple
			checkAcceptNum = [0,0]

			for msg in self.ackarray: #set highestAcceptNum = highest AcceptNum out of all the messages
				tempAcceptNum = msg.acceptTuple.split(",")
				if ((tempAcceptNum[0] > checkAcceptNum[0]) or ((tempAcceptNum[0] == checkAcceptNum[0]) and (tempAcceptNum[1] > checkAcceptNum))):
					highestAcceptNum = msg.acceptTuple
					highestAcceptVal = msg.acceptVal

			if highestAcceptNum == "0,0": #None of the msgs have acceptNum set already
				self.acceptTuple = self.ackarray[0].ballot
				self.acceptVal = self.ackarray[0].acceptVal
			else: 
				self.acceptTuple = highestAcceptNum
				self.acceptVal = highestAcceptVal

			for dest_id, sock in self.outgoing_channels.iteritems():#Send all prms a prepare message
				print ("Sent Accept message to node ", dest_id)
				msg = Message(self.id, self.ballot, None, self.acceptVal, self.ackarray[0].index, self.ackarray[0].originalPRM, None, Message.ACCEPT)
				sock.send(str(msg))		
			
			cThread = threading.Thread(target = self.checkMajorityAccepts)
			cThread.daemon = True
			cThread.start()
		self.checkingMajorityAcks = False

	def checkMajorityAccepts(self):
		print "Checking to see if I have Majority Accepts..."
		time.sleep(0.5)
		#TODO Check for majorityAccepts

	def incrementBallot(self): 
		tempBallot = self.ballot.split(",")
		ballotFirstIndex = int(tempBallot[0])
		ballotFirstIndex += 1
		self.ballot = str(ballotFirstIndex) + "," + str(self.id)
	def changeAcceptTuple(self, first, second): #passed in as int
		self.acceptTuple = str(first) + "," + str(second)



	def openListeningSocket(self, IP, port):
		self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.listeningSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.listeningSocket.bind( (IP, port) )
		self.listeningSocket.setblocking(0) 
		self.listeningSocket.listen(1)
		self.listening = True

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

main()
