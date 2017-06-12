#!/bin/sh
killall -9 python

for n in {1..3}
do
gnome-terminal --tab set-title "Prm $n" -e "python prm.py $n setup2.txt"
gnome-terminal --tab set-title "Cli $n" -e "python cli.py $n setup2.txt"
done
