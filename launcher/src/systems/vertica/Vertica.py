import os
import utils.utils as utils
from entities.result import *

class Vertica:
  def __init__(self, machine):
    self.machines = [machine]
    self.container = "vertica"

  def name(self):
    return "Vertica"

  workloadMap = {'tpch': './systems/vertica/sql/tpch'}

  # Verify that vertica can run the experiment.
  # Assume the sql file is (e.query).sql
  # Check for missing .sql files, etc.
  def checkExperiment(self, e):
    if e.workload not in self.workloadMap:
      print("Unknown workload for Vertica: %s" % e.workload)
      return False
      
    queryFolder = self.workloadMap[e.workload]
    queryFile = os.path.join(queryFolder, e.query + ".sql")      

    if not os.path.isfile(queryFile):
      print("Vertica can't find sql file: %s" % queryFile)
      return False

    else:
      return True
  
  def runExperiment(self, e, trial_id):
    queryFolder = self.workloadMap[e.workload]
    queryFile = os.path.join(queryFolder, e.query + ".sql")      
   
    if e.workload == 'tpch' and e.query == '5':
     return Result(trial_id, "Skipped", 0, "TPCH Query 5 is too slow on Vertica")

    return self.runVertica(e.dataset, queryFile, trial_id)


  def runVertica(self, schema, queryFile, trial_id):
    command = "./systems/vertica/run_vertica.sh %s %s" % (schema, queryFile)
    output = utils.runCommand(command)
    elapsed = output.split(" ")[-2]
    return Result(trial_id, "Success", float(elapsed), "")
