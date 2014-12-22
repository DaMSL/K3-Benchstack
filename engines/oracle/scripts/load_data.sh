#!/bin/bash

# Change all filenames to include .dat extension
#./rename.sh $1

# Run SQLLDR to load all data from files
sudo LD_LIBRARY_PATH=../cli ../cli/sqlldr system/manager@localhost:11521/orcl control=region.ctl
sudo LD_LIBRARY_PATH=../cli ../cli/sqlldr system/manager@localhost:11521/orcl control=nation.ctl
sudo LD_LIBRARY_PATH=../cli ../cli/sqlldr system/manager@localhost:11521/orcl control=supplier.ctl
sudo LD_LIBRARY_PATH=../cli ../cli/sqlldr system/manager@localhost:11521/orcl control=customer.ctl
sudo LD_LIBRARY_PATH=../cli ../cli/sqlldr system/manager@localhost:11521/orcl control=orders.ctl
sudo LD_LIBRARY_PATH=../cli ../cli/sqlldr system/manager@localhost:11521/orcl control=partsupp.ctl
sudo LD_LIBRARY_PATH=../cli ../cli/sqlldr system/manager@localhost:11521/orcl control=lineitem.ctl

# Reset source data files to original name
#./unrename.sh $1
