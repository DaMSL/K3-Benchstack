#!/bin/bash

if [ ! "$1" ];
then
  echo "Usage: prep_step2.sh <shm size (in GB)>"
  exit 0
fi


# set up install script
echo "#!/bin/bash" > step1/install
chmod 755 step1/install
echo "mount -t tmpfs shmfs -o size=$(($1+8))g /dev/shm" >> step1/install
echo "date" >> step1/install
echo "su -s /bin/bash oracle -c 'cd /tmp/install/database/ && ./runInstaller -ignoreSysPrereqs -ignorePrereq -silent -noconfig -responseFile /tmp/db_install.rsp'" >> step1/install


# Set up the script to create started & ORCL database
cat step2/setenv_orcl > step2/create
chmod 755 step2/create
echo "mount -t tmpfs shmfs -o size=$(($1+8))g /dev/shm" >> step2/create
cat step2/setupdb_orcl >> step2/create

# Create the ORCL parameter file
cat step2/initORCL_shell > step2/initORCL.ora
echo "inmemory_size=$1G" >> step2/initORCL.ora
echo "sga_target=$(($1+8))G" >> step2/initORCL.ora
echo "pga_aggregate_target=$((2 + $1 / 5))G" >> step2/initORCL.ora

# Create system config file to allocate shared memory
cat step2/sysctl_shell > step2/sysctl.conf
echo "kernel.shmmax = $((($1 + 8) * 1024*1024*1024))" >> step2/sysctl.conf
echo "kernel.shmall = $((($1 + 8) * 262144 + 16384))" >> step2/sysctl.conf
echo "kernel.shmmni = 4096" >> step2/sysctl.conf

cp step2/sysctl.conf step1/sysctl.conf

# Create the start script
echo "#!/bin/bash" > step3/start
chmod 755 step3/start
echo "mount -t tmpfs shmfs -o size=$(($1+8))g /dev/shm" >> step3/start
echo "sysctl -p" >> step3/start
cat step3/start_shell >> step3/start

