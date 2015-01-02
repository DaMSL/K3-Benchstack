#!/bin/bash

HOST=$2

start() {
    echo "python collect_cadv.py $HOST $1 &"
    python collect_cadv.py $HOST $1 &
    echo "Data collection initiated"
}

stop() {
    pid=`ps -ef | grep '[p]ython collect_cadv' | awk '{ print $2 }'`
    echo $pid
    kill $pid
    sleep 1
    echo "Data collection complete"
}

printhelp() {
    echo
    echo "Damsl profile tool"
    echo "Note: Ensure that the collect_cadv.py script is the in same dir as this shell script."
    echo
    echo " To START:   profile.sh <engine> host start"
    echo " To STOP:    profile.sh stop"
    echo
    echo "where <engine> is the docker label for the container you want to collect upon"
    echo
    exit 0
}

if [[ $# -eq 3 && $3 -eq 'start' ]]
then
  start $1 $2
else
    if [[ $# -eq 2 && $1 -eq 'stop' ]]
    then
      stop
    else
      printhelp
    fi
fi



