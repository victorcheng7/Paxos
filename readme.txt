# Paxos

To run:

1) Create setup file in this format:

[numNodes]
[cli_IP cli_Port prm_IP prm_Port] #write numNodes lines of these 

example :
2
127.0.0.1 5001 127.0.0.1 5002  
127.0.0.1 5101 127.0.0.1 5102


2) open [numNodes] CLIs and PRMs in separate terminal windows using:

	python prm.py [nodeNum] [setup_file]
	python cli.py [nodeNum] [setup_file]


3) wait for all windows to display "setup finished"

4) now you can type these commands into any CLI window:

	map [filename]
	reduce [filename1] [filename2]
	replicate [filename]
	stop
	resume
	print
	total [pos1] [pos2]
	merge [pos1] [pos2]

   map, reduce, total, and merge do not currently function

Author: Samuel Chu (7651706), Victor Cheng (3900552)



