#!/bin/bash

for d in $1/*; do
  echo $d
  cd $d
  rename 's/$/.dat/' *
  cd ..
done
