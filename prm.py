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
	site = Site(prm_id)
	setup_file = sys.argv[2]
	setup(site, setup_file)
	print "success"
	commThread(site)


def commThread(site):

	finish = 0
	while finish == 0:

		for con in site.incoming_channels:	
			receiveFromPrm(con)

		receiveFromCli(site.cli[1])


def receiveFromPrm(con):
	try:
		data = con.recv(1024)
		print data	
			
		if data == "exit":
			for dest_id, sock in site.outgoing_channels.iteritems():
					sock.send("exit")
			finish = 1
	except socket.error, e:
		pass


def receiveFromCli(con):
	try:
		data = con.recv(1024)
		print data	
		if data == "replicate!":
			for dest_id, sock in site.outgoing_channels.iteritems():
					sock.send("replicate from prm with id {0}".format(site.id))
			
		elif data == "exit"	:
			for dest_id, sock in site.outgoing_channels.iteritems():
				if dest_id != site.id:
					sock.send("exit")
			finish = 1
	except socket.error, e:
		pass


def setup(site, setup_file):
	#Read setup file. ex - setup.txt  
	print "gothere"   
	with open(setup_file, 'r') as f:
		N = int(f.readline().strip())
		site.num_nodes = N
		process_id = 0
		for line in f.readlines():
			process_id += 1
			if process_id <= N+1:
				IP1, port1, IP2, port2 = line.strip().split()
				port1 = int(port1)
				port2 = int(port2)

				if process_id == site.id:
					site.cli[0] = (IP1, port1)
					site.addr_book.append( (IP1, port1) )
					site.openListeningSocket( IP2, port2) #open for traffic

				else:
					site.addr_book.append( (IP2, port2) )
					site.addOutgoingChannel(process_id)
					site.addIncomingChannel(process_id)

				

	print site.outgoing_channels
	print site.incoming_channels
	site.openOutgoingChannels()
	print "am"
	site.openIncomingChannels()

	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	site.cli[1] = sock
	while True:
		try: 
			sock.connect(site.cli[0])
			print "connected to cli"
			break
		except Exception:
			continue


	while True:
		try:

			con, addr = site.listeningSocket.accept()
			con.setblocking(0)

			# if addr == site.cli[0]:
			site.cli[2] = con
			print "connected to cli2"
			break
		except socket.error:
			continue


	print "hello"
	print site.cli[0]
	print site.cli[1]
	print site.cli[2]

	# # remove cli from outgoing channels
	# site.outgoing_channels.pop(site.id, None)


class Site(object):
	def __init__(self, site_id):
		self.id = site_id
		self.snap_count = 0
		self.snap_count = 0
		self.balance = 10
		self.incoming_channels = []
		self.SnapIDTableLastEntryTemplate = {}
		self.addr_book = []
		self.outgoing_channels = {}
		self.listeningSocket = None
		self.cli = [None]*3 # cli[0] = addr cli[1] = incoming, cli[2] = outgoing
		self.snapID_table = {}
		self.done_processes = set()


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
		# self.cli[2] = self.outgoing_channels[self.id]

	def addIncomingChannel(self, source_id):
		self.SnapIDTableLastEntryTemplate[source_id] = [False, 0]

	def openIncomingChannels(self):
		while len(self.incoming_channels) != len(self.SnapIDTableLastEntryTemplate):
			
			try:

				con, addr = self.listeningSocket.accept()
				con.setblocking(0)
				# if addr == self.cli[0]:
				# 	# self.cli[1] = con
				# else:
				self.incoming_channels.append(con)
			except socket.error:
				continue

		print "terminate"
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
