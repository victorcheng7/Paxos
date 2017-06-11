#!/bin/sh
killall -9 python


# node 1
gnome-terminal --tab set-title "Prm $n" -x bash -c "ssh -i paxos_pem/Node1.pem ubuntu@ec2-54-186-215-28.us-west-2.compute.amazonaws.com;
cd paxos; python prm.py 1 setup.txt; bash"

gnome-terminal --tab set-title "Cli $n" -x bash -c "ssh -i paxos_pem/Node1.pem ubuntu@ec2-54-186-215-28.us-west-2.compute.amazonaws.com;
cd paxos; python cli.py 1 setup.txt; bash"

# node 2

gnome-terminal --tab set-title "Prm $n" -x bash -c "ssh -i paxos_pem/Node1.pem ubuntu@ec2-54-186-215-28.us-west-2.compute.amazonaws.com;
cd paxos; python prm.py 2 setup.txt; bash"

gnome-terminal --tab set-title "Cli $n" -x bash -c "ssh -i paxos_pem/Node1.pem ubuntu@ec2-54-186-215-28.us-west-2.compute.amazonaws.com;
cd paxos; python cli.py 2 setup.txt; bash"

# node 3

gnome-terminal --tab set-title "Prm $n" -x bash -c "ssh -i paxos_pem/Node1.pem ubuntu@ec2-54-186-215-28.us-west-2.compute.amazonaws.com;
cd paxos; python prm.py 3 setup.txt; bash"

gnome-terminal --tab set-title "Cli $n" -x bash -c "ssh -i paxos_pem/Node1.pem ubuntu@ec2-54-186-215-28.us-west-2.compute.amazonaws.com;
cd paxos; cli.py 3 setup.txt; bash"




