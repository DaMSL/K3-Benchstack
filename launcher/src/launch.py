import sys
import datetime 

# Entities are simple Python objects
# That correspond to a row in a table in the database.
# The .tup() method allows for easy insertion as a tuple
from entities.experiment import Experiment
from entities.result import *
from entities.trial import *

# Each system provides two methods:
# .checkExperiment(e) which performs some initial checks to ensure the experiment can be run on the system
# .runExperiment(e) runs the actual experiment and returns a Result
from systems.impala.Impala import Impala
from systems.vertica.Vertica import Vertica
from systems.oracle.Oracle import Oracle
from systems.spark.Spark import Spark

# Profiler's are python Threads that poll a web-service for Docker OS-level metrics
# On all the machines specified in system.machines
from profiler.profiler import Profiler

import plot.plot as plot
import db.db as db
from utils.utils import InterruptException

# Initialize the database, returning a usable connection
def initDatabase(shouldDrop):
  print("Setting up Database")
  conn = db.getConnection()

  if shouldDrop:
    print("\tDropping Tables")
    db.dropTables(conn)
  
  print("\t Creating Tables")
  db.createTables(conn)

  return conn

def checkExperiments(experiments, systems):
  print("Ensuring that all systems can run specified queries")
  for experiment in experiments:
    for system in systems:
      if not system.checkExperiment(experiment):
        print("%s can not run experiment: %s. Aborting." % (system.name(), experiment.query) )
        sys.exit(1)

def runExperiments(experiments, systems, numTrials):
  print("Running experiments")
  for experiment in experiments:
    print("------Running experiment: %s------" % experiment.name()  )
    # Enter experiment into the database
    exp_id = db.insertExperiment(conn, experiment)

    for system in systems:
      print("\tRunning System: %s" % (system.name()) )
      for trialNum in range(1, numTrials + 1):
      
        # Enter a new trial into the database
        trial = Trial(exp_id, trialNum, system.name(), datetime.datetime.now())
        trial_id = db.insertTrial(conn, trial)
   
        # Run the experiment with profiling. 
        p = Profiler(system.machines, system.container, trial_id)
        p.start()
  
        result = None
        try:
          result = system.runExperiment(experiment, trial_id)
        except InterruptException as inst:
          p.finished = True
          p.join()
          sys.exit(1)
        except Exception as inst:
          result = Result(trial_id, "Failure", 0, "Unhandled exception: " + str(inst))
 
        p.finished = True
        p.join()

        if result.status == "Failure":
          print("\tTrial Failed: %s" % (result.notes) )
          db.insertResult(conn, result)
        
        elif result.status == "Skipped":
          print("\tTrial skipped: %s" % (result.notes))
          db.insertResult(conn, result)
        
        elif result.status == "Success":
          print("\tTrial Succeeded. Elapsed Time: %s ms" % (result.elapsed))
          db.insertResult(conn, result)

        else:
          print("\t Unknown result status: %s. Exiting." % (result.status))
          sys.exit(1)

hms = [ "qp-hm" + str(i) for i in range(1,9) ]
allSystems = [Spark(hms), Impala(hms), Vertica("mddb"), Oracle("mddb")]
allTPCH = [1, 3, 5, 6, 11, 18, 22]

if __name__ == "__main__":
  conn = initDatabase(False)

  systems = allSystems
  numTrials = 1
  experiments = []
  for i in [1]:
    experiments.append(Experiment("tpch",str(i),"tpch10g"))
  
  checkExperiments(experiments, systems)
  runExperiments(experiments, systems, numTrials)

  #plot.plotLatest(conn)
  conn.close()
