#!/bin/bash


for d in $1/*; do
  cd $d
  rename 's/.dat//' *
  cd ..
done