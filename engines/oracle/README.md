# Oracle datase server

## Updated by Ben (Dec '14)
The build is updated to run col-store in-memory from within a docker container. Container requires privileged access to the host and it needs to set shared memory for both the System Global Area (SGA) and the Program Global Area (PGA) when creating the database. I have included a "prep" script (prep.sh) which takes a single argument to set up the install/create scripts with the requested shared memory. Thus, before running anything run './prep.sh 100' to build a 100 GB in-memory oracle instance. Other annotations are updated, just follow the build instructions down below.....


## BUILDING
Currently, the build is set up to configure shared memory upfront which means you need to know the size of the shared mem segment before building (TODO: write "step3" script to adjust shm for setting SGA/PGA on the fly). It is a 4-step process:

0) Run prep to set up the install scripts
1) Install Oracle
2) Install ORCL DB
3) Set up run environment

## RUNNING
Data loading & query running scripts are included. These scripts assume you have installed SQL*Plus and SQL*Loader locally in your home dir (under a /cli sub-folder). You will need to install associated libs and an environment var in the run context (see scripts/ README). 

Option 2 is to run a shell into the instance and run it all from there (keep in mind that if you're going to load data, the docker run command will have needed a mounted volume to access the data and you may a 2nd one for these scripts/queries):
```

$ docker exec -it orcl bash
```

