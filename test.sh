#!/bin/sh
killall -9 python

for n in {1..5}
do
sleep 0.5
gnome-terminal --tab set-title "Prm $n" -e "python prm.py $n setup4.txt"
sleep 0.5
gnome-terminal --tab set-title "Cli $n" -e "python cli.py $n setup4.txt"
done


