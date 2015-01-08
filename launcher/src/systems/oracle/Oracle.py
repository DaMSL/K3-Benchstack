import os
import utils.utils as utils
from entities.result import *

class Oracle:
  def __init__(self, machine):
    self.machines = [machine]
    self.container = "orcl"

  def name(self):
    return "Oracle"

  workloadMap = {'tpch': './systems/oracle/sql/tpch'}
  datasetMap = {'tpch10g': 'mddb2', 'tpch100g': 'mddb'}
  
  # Verify that Oracle can run the experiment.
  # Assume the sql file is (e.query).sql
  # Check for missing .sql files, etc.
  def checkExperiment(self, e):
    if e.workload not in self.workloadMap:
      print("Unknown workload for Oracle: %s" % e.workload)
      return False

    if e.dataset not in self.datasetMap:
      print("Unknown dataset for Oracle: %s" % e.workload)
      return False
      
    queryFolder = self.workloadMap[e.workload]
    queryFile = os.path.join(queryFolder, e.query + ".sql")      

    if not os.path.isfile(queryFile):
      print("Oracle can't find sql file: %s" % queryFile)
      return False

    else:
      return True
  
  def runExperiment(self, e, trial_id):
    queryFolder = self.workloadMap[e.workload]
    queryFile = os.path.join(queryFolder, e.query + ".sql")      
   
    # TODO different database for each dataset 
    # instead of "orcl". Removing the need to set ORACLE_HOST on the fly 
    host = self.datasetMap[e.dataset]
    return self.runOracle(host, "orcl", queryFile, trial_id) 

  def runOracle(self, host, database, queryFile, trial_id):
    command = "ORACLE_HOST=%s ./systems/oracle/run_oracle.sh %s %s" % (host, database, queryFile)
    output = utils.runCommand(command)
    # Parse hh:mm:ss output
    hms = output.split(":")
    secContrib = 1000 * float(hms[-1][:-1])
    minContrib = 60 * 1000 * float(hms[-2])
    hourContrib = 60 * 60 * 1000 * float(hms[-3]) 
    elapsed = secContrib + minContrib + hourContrib
    return Result(trial_id, "Success", float(elapsed), "")

