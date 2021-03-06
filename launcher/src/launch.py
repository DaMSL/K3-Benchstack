import sys
import datetime 
import argparse
import traceback

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
from systems.k3.K3 import K3


# Profiler's are python Threads that poll a web-service for Docker OS-level metrics
# On all the machines specified in system.machines
from profiler.profiler import Profiler

#import plot.plot as plot
import db.db as db 

import utils.log as log

# Initialize the database, returning a usable connection
def initDatabase(shouldDrop):
  log.logHeader("Initializing the database")
  conn = db.getConnection()

  #if shouldDrop:
  #  log.logEvent(1, "Dropping Tables")
  #  db.dropTables(conn)
  #
  #log.logEvent(1, "Creating Tables") 
  #db.createTables(conn)

  log.logEvent(1, "SUCCESS")
  log.endSection()
  return conn

# Verify that experiments are runnable on all systems, exiting if they are not.
def checkExperiments(experiments, systems):
  log.logHeader("Ensuring that all systems can run specified experiments")
  for experiment in experiments:
    for system in systems:
      if not system.checkExperiment(experiment):
        log.logEvent(1, ("%s can not run experiment: %s. Aborting." % (system.name(), experiment.query) ))
        sys.exit(1)
  log.logEvent(1, "SUCCESS")
  log.endSection()

def runExperiments(experiments, systems, numTrials, debug=False, profiling=False):
  log.logHeader("Running Experiments") 
  for experiment in experiments:
    log.logEvent(1, "Running experiment: %s" % experiment.name() )
    # Enter experiment into the database
    exp_id = db.insertExperiment(conn, experiment)

    for system in systems:
      log.logEvent(2, "Running System: %s" % (system.name()) )
      for trialNum in range(1, numTrials + 1):
        log.logEvent(3, "Running Query %s on %s, Trial: %d" % (experiment.query, system.name(), trialNum) )
      
        # Enter a new trial into the database
        trial = Trial(exp_id, trialNum, system.name(), datetime.datetime.now())
        trial_id = db.insertTrial(conn, trial)
   
        # Run the experiment with profiling. 
        p = Profiler(system.machines, system.container, trial_id)


        if profiling:
          p.start()
  
        result = None
        try:
          result = system.runExperiment(experiment, trial_id)
        except KeyboardInterrupt as inst:
          log.logEvent(4,"Received interrupt. Shutting down...")
          p.finished = True
          p.join()
          result = Result(trial_id, "Failure", 0, "Cancelled by User ")
          db.insertResult(conn, result)
          sys.exit(1)
        except Exception as inst:
          print(traceback.format_exc())
          result = Result(trial_id, "Failure", 0, "Unhandled exception: " + str(inst))

        if profiling:
          p.finished = True
          p.join()

        if result == None:
          print("Experiment returned None... aborting")
          return

        if result.status == "Failure":
          log.logEvent(4, "Trial Failed: %s" % (result.notes) )
          db.insertResult(conn, result)
        
        elif result.status == "Skipped":
          log.logEvent(4,"Trial skipped: %s" % (result.notes))
          db.insertResult(conn, result)
        
        elif result.status == "Success":
          log.logEvent(4, "Trial Succeeded. Elapsed Time: %s ms" % (result.elapsed))
          db.insertResult(conn, result)
          for op in result.operators:
            db.insertOperator(conn, op)

        else:
          log.logEvent(4, "Unknown result status: %s. Exiting." % (result.status))
          sys.exit(1)
    log.endSection()

hms = [ "qp-hm" + str(i) for i in range(1,9) ]
allSystems = [Spark(hms), Impala(hms), Vertica("mddb"), Oracle("mddb")]
allTPCH = [1, 3, 5, 6, 11, 18, 22]

systemMap = {'Spark': Spark(hms), 'Impala': Impala(hms, partitioned=False), 'Impala-p': Impala(hms, partitioned=True), 'Vertica': Vertica("mddb"), 'Oracle': Oracle("mddb"), 'K3': K3(hms)}

def parseSystems(lst):
  result = []
  for system in lst:
    if system not in systemMap:
      print("Unrecognized System: %s" % (system))
      sys.exit(1)
    else:
      result.append(systemMap[system])

  return result
      
def parseTPCHExperiments(lst, dataset):
  result = []
  for query in lst:
    result.append(Experiment("tpch",query,dataset))
  return result

def parseAmplabExperiments(lst):
  result = []
  for query in lst:
    result.append(Experiment("amplab", query, "amplab"))
  return result

def parseScalabilityExperiments(lst, dataset):
  result = []
  for query in lst:
    result.append(Experiment("scalability", query, dataset))
  return result

def parseMLExperiments(query, dataset):
  result = []
  result.append(Experiment("ml", query, dataset))
  return result

def parseMLScalabilityExperiments(lst, query):
  result = []
  for dataset in lst:
    result.append(Experiment("ml_scalability", query, dataset))
  return result
 
def parseArgs():
  parser = argparse.ArgumentParser() 
  parser.add_argument('--systems', nargs='+', help='Space seperated list of systems to run. Choices include (Spark, Impala, Vertica, Oracle). If omitted, all systems will be used')
  parser.add_argument('--trials', type=int, default=10, help='Number of trials to run each experiment. Default is 10.')
  parser.add_argument('--amplab', nargs='*',  help='Run the Amplab BDB workload. Provide a space seperated list of queries to run.')
  parser.add_argument('--tpch10g', nargs='*', help='Run the TPCH workload on the 10G dataset. Provide a space seperated list of queries to run.')
  parser.add_argument('--tpch100g', nargs='*',  help='Run the TPCH workload on the 100G dataset. Provide a space seperated list of queries to run.')
  parser.add_argument('--scalability16g', nargs='*',  help='Run the TPCH workload on the 16G dataset. Provide a space seperated list of queries to run.')
  parser.add_argument('--scalability32g', nargs='*',  help='Run the TPCH workload on the 32G dataset. Provide a space seperated list of queries to run.')
  parser.add_argument('--scalability64g', nargs='*',  help='Run the TPCH workload on the 64G dataset. Provide a space seperated list of queries to run.')
  parser.add_argument('--scalability128g', nargs='*',  help='Run the TPCH workload on the 128G dataset. Provide a space seperated list of queries to run.')
  parser.add_argument('--scalability256g', nargs='*',  help='Run the TPCH workload on the 256G dataset. Provide a space seperated list of queries to run.')
  parser.add_argument('--k_means10g', action="store_true",  help='Run kmeans 10g. No arguments.')
  parser.add_argument('--k_means100g', action="store_true", help='Run kmeans 100g. No arguments.')
  parser.add_argument('--sgd10g', action="store_true",  help='Run sgd 10g. No arguments.')
  parser.add_argument('--sgd100g', action="store_true", help='Run sgd 100g. No arguments.')
  parser.add_argument('--pagerank', action="store_true", help='Run pagerank. No arguments.')
  parser.add_argument('--scalability_k_means', nargs='*',  help='Run kmeans scalability. Provide a space seperated list of datasets (16,32,64,128,256)')
  parser.add_argument('--scalability_sgd', nargs='*',  help='Run kmeans scalability. Provide a space seperated list of datasets (16,32,64,128,256)')
  args = parser.parse_args()

  log.logHeader("Parsing Arguments: ")
  systems = allSystems
  if args.systems:
    systems = parseSystems(args.systems)
    log.logEvent(1, "Using specified systems: %s" % (str(args.systems)))
  else:
    log.logEvent(1, "Using all available systems. (Default)")

  experiments = []
  if args.tpch10g:
    experiments.extend(parseTPCHExperiments(args.tpch10g, "tpch10g"))
  if args.tpch100g:
    experiments.extend(parseTPCHExperiments(args.tpch100g, "tpch100g"))
  if args.scalability16g:
    experiments.extend(parseScalabilityExperiments(args.scalability16g, "16"))
  if args.scalability32g:
    experiments.extend(parseScalabilityExperiments(args.scalability32g, "32"))
  if args.scalability64g:
    experiments.extend(parseScalabilityExperiments(args.scalability64g, "64"))
  if args.scalability128g:
    experiments.extend(parseScalabilityExperiments(args.scalability128g, "128"))
  if args.scalability256g:
    experiments.extend(parseScalabilityExperiments(args.scalability256g, "256"))
  if args.amplab:
    experiments.extend(parseAmplabExperiments(args.amplab))
  if args.k_means10g:
    experiments.extend(parseMLExperiments("k_means","sgd10g"))
  if args.k_means100g:
    experiments.extend(parseMLExperiments("k_means","sgd100g"))
  if args.sgd10g:
    experiments.extend(parseMLExperiments("sgd", "sgd10g"))
  if args.sgd100g:
    experiments.extend(parseMLExperiments("sgd", "sgd100g"))
  if args.scalability_k_means:
    experiments.extend(parseMLScalabilityExperiments(args.scalability_k_means, "k_means"))
  if args.scalability_sgd:
    experiments.extend(parseMLScalabilityExperiments(args.scalability_sgd, "sgd"))
  if args.pagerank:
    experiments.extend([Experiment("graph", "pagerank", "twitter")])

  
  if len(experiments) == 0:
    log.logEvent(1, "No Experiments Specified: Exiting.")
    sys.exit(1)
  else:
    log.logEvent(1, "Experiments to run: ")
    for exp in experiments:
      log.logEvent(2, exp.name())

  log.endSection() 

  numTrials = args.trials

  return (experiments, systems, numTrials)

def plotNew(conn):
  log.logHeader("Plotting New Experiments")
  plot.plotMostRecentOps()
  plot.plotAllTimes()
  log.logEvent(1, "SUCCESS")
  log.endSection()
 
if __name__ == "__main__":
  (experiments, systems, numTrials) = parseArgs()
  
  conn = initDatabase(False)
  debug = True
  
  checkExperiments(experiments, systems)
  runExperiments(experiments, systems, numTrials, debug, profiling=True)

#  plotNew(conn)
  conn.close()
