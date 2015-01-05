import datetime 

from entities.experiment import Experiment
from entities.result import *
from entities.trial import *

from systems.impala.Impala import Impala
from systems.vertica.Vertica import Vertica
from systems.oracle.Oracle import Oracle

from profiler.profiler import Profiler

import plot.plot as plot
import db.db as db
     
if __name__ == "__main__":
  print("Setting up Database")
  conn = db.getConnection()
  #print("\t Dropping Tables")
  #db.dropTables(conn)
  print("\t Creating Tables")
  db.createTables(conn)

  numTrials = 2
  # Build the set of experiments to be run
  experiments = []
  # TPCH experiments
  for i in [1]:
  #for i in [1, 3, 5, 6, 11, 18, 22]:
    experiments.append(Experiment("tpch",str(i),"tpch100g"))
  
  # Set up systems 
  hms = [ "qp-hm" + str(i) for i in range(1,9) ]
  systems = [Impala(hms), Vertica("mddb"), Oracle("mddb")]

  print("Ensuring that all systems can run specified queries")
  for experiment in experiments:
    for system in systems:
      if not system.checkExperiment(experiment):
        print("%s can not run experiment: %s. Aborting." % (system.name(), experiment.query) )
        exit(1)

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
        except Exception as inst:
          result = Result(trial_id, "Failure", 0, "Unhandled exception: " + str(inst))
 
        p.finished = True
        p.join()

        # Upon Failure, notify user. 
        if result.status == "Failure":
          print("\tTrial Failed:" + result.message)
          db.insertResult(conn, result)
        
        # Upon skipped, notify user. Enter a 0 entry into the database.
        # (All entries will be zero, leaving a gap in the plot for this system)
        elif result.status == "Skipped":
          print("\tTrial skipped")
          db.insertResult(conn, result)
        
        # Upon success, grab the elapsed time and enter it into the db
        elif result.status == "Success":
          print("\tTrial Succeeded. Elapsed Time: %s ms" % (result.elapsed))
          db.insertResult(conn, result)
  
  #plot.plotLatest(conn)
  conn.close()
