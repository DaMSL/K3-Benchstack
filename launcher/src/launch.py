import datetime 

from entities.experiment import Experiment
from entities.result import *
from entities.trial import *

from systems.impala import Impala
from systems.vertica import Vertica
import plot.plot as plot
import db.db as db
     
if __name__ == "__main__":
  print("Setting up Database")
  conn = db.getConnection()
  db.dropTables(conn)
  db.createTables(conn)

  # Build the set of experiments to be run
  experiments = []
  # TPCH experiments
  for i in [1]:
  #for i in [1, 3, 5, 6, 11, 18, 22]:
    experiments.append(Experiment("tpch",str(i),"tpch10g"))

  systems = [Impala.Impala(), Vertica.Vertica()]

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
      result = system.runExperiment(experiment)
      trial = None
   
      # Upon Failure, notify user. Do not insert into database. 
      if isinstance(result, Failure):
        print("\tTrial Failed:" + result.message)
      # Upon skipped, notify user. Enter a 0 entry into the database.
      # (All entries will be zero, leaving a gap in the plot for this system)
      elif isinstance(result, Skipped):
        print("\tTrial skipped")
        trial = Trial(system.name(), experiment.query, experiment.dataset, 0, 1, datetime.datetime.now())
        #db.insertTrial(trial)
      # Upon success, grab the elapsed time and enter it into the db
      elif isinstance(result, Success):
        print("\tTrial Succeeded")
        trial = Trial(system.name(), experiment.query, experiment.dataset, result.elapsed, 1, datetime.datetime.now())
        #db.insertTrial(conn, trial)
  
  plot.plotLatest(conn)
  conn.close()
