import datetime 

from entities.experiment import Experiment
from entities.result import *
from entities.trial import *

from systems.impala.Impala import Impala
from systems.vertica.Vertica import Vertica
from systems.oracle.Oracle import Oracle


import plot.plot as plot
import db.db as db
     
if __name__ == "__main__":
  print("Setting up Database")
  conn = db.getConnection()
  print("\t Dropping Tables")
  db.dropTables(conn)
  print("\t Creating Tables")
  db.createTables(conn)

  # Build the set of experiments to be run
  experiments = []
  # TPCH experiments
  for i in [5]:
  #for i in [1, 3, 5, 6, 11, 18, 22]:
    experiments.append(Experiment("tpch",str(i),"tpch10g"))

  #systems = [Impala(), Vertica(), Oracle()]
  systems = [Oracle()]

  print("Ensuring that all systems can run specified queries")
  for experiment in experiments:
    for system in systems:
      if not system.checkExperiment(experiment):
        print("%s can not run experiment: %s. Aborting." % (system.name(), experiment.query) )
        exit(1)

  print("Running experiments")
  for experiment in experiments:
    print("------Running experiment: %s------" % experiment.name()  )
    for system in systems:
      print("\tRunning System: %s" % (system.name()) )

      # Enter a new trial into the database
      trial = Trial(system.name(), experiment.query, experiment.dataset, 1, datetime.datetime.now())
      run_id = db.insertTrial(conn, trial)
   
      # Run the experiment 
      result = system.runExperiment(experiment)
      # Upon Failure, notify user. Do not insert into database. 
      if isinstance(result, Failure):
        print("\tTrial Failed:" + result.message)
        db.insertResult(conn, Result(run_id, "Failed", 0))
      # Upon skipped, notify user. Enter a 0 entry into the database.
      # (All entries will be zero, leaving a gap in the plot for this system)
      elif isinstance(result, Skipped):
        print("\tTrial skipped")
        db.insertResult(conn, Result(run_id, "Skipped", 0))
      # Upon success, grab the elapsed time and enter it into the db
      elif isinstance(result, Success):
        print("\tTrial Succeeded. Elapsed Time: %s ms" % (result.elapsed))
        db.insertResult(conn, Result(run_id, "Success", result.elapsed))
  
  #plot.plotLatest(conn)
  conn.close()
