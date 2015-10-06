#!/bin/bash

# Run this script inside a docker container, with the build directory mounted at /build 

cd /build
sbt package
