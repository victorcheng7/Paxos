#!/bin/sh
killall -9 python

for n in {1..2}
do
gnome-terminal --tab set-title "Prm $n" -e "python prm.py $n setup.txt"
gnome-terminal --tab set-title "Cli $n" -e "python cli.py $n setup.txt"
done


