#!/bin/sh
killall -9 python

for n in {1..9}
do
gnome-terminal --tab set-title "Prm $n" -e "python another.py $n setup7.txt"
gnome-terminal --tab set-title "Cli $n" -e "python cli.py $n setup7.txt"
done


