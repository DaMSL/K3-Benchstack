#!/bin/bash

if [ $# -le 1 ]
then
  echo "Usage: $0 jfr_folder output_folder"
  exit 1
fi

IN=$1
OUT=$2

if [ -d $OUT ]
then
  echo "Error. Output directory '$OUT' already exists"
  exit 1
fi

mkdir -p $OUT

# Convert each jfr to xml
# The main class is broken, and will only open a file named 'r' at the specified location. Using /tmp for now.
for file in $(ls $IN);
do
  cp $IN/$file /tmp/r
  java -cp jars/com.jrockit.mc.flightrecorder_5.4.0.162463.jar:jars/com.jrockit.mc.common_5.4.0.162463.jar com.jrockit.mc.flightrecorder.RecordingPrinter /tmp > $OUT/$file.xml;
done
