#!/bin/bash

# Wrapper script to run a vertica sql query and return the results in easily parsable form
vsql -f $1 -t -A -F "~"
