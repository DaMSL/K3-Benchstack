import os
import utils.utils as utils

from entities.result import *

class Impala:
  def __init__(self, machines):
    self.machines = machines
    self.container = "Impala_slave" 
    # Special code for running TPCH Query 11
    # Need to run subquery and main query  
    self.tpch11SubQueryFile = './systems/impala/sql/tpch/q11_10G/11sub.sql'
    self.tpch11MainQueryFile = './systems/impala/sql/tpch/q11_10G/11.sql'

  def name(self):
    return "Impala"

  queryMap = {'tpch': 'systems/impala/sql/tpch/queries/'}
  schemaMap = {'tpch': 'systems/impala/sql/tpch/schema/'}
  scaleFactorMap  = {'tpch10g': '10g', 'tpch100g': '100g'} 

  # Verify that Impala can run the experiment 
  # Assume the sql file is (e.query).sql
  def checkExperiment(self, e):
    if e.workload not in self.queryMap:
      print("Unknown workload for Impala: %s" % e.workload)
      return False

    schemaFolder = self.schemaMap[e.workload]
    if not os.path.exists(schemaFolder):
      print("Schema folder not found for Impala: %s" % schemaFolder)
      return False
        
    if e.dataset not in self.scaleFactorMap:
      print("Unknown dataset for Impala: %s" % e.dataset)
      return False
   
    if e.dataset == 'tpch10g' and e.query == '11':
      return self.checkTPCH11()

    scaleFactor = self.scaleFactorMap[e.dataset]
    queryFolder = self.queryMap[e.workload]
    queryFile = os.path.join(queryFolder, e.query + ".sql")

    if not os.path.isfile(queryFile):
      print("Query file not found for Impala: " + queryFile)
      return False
   
    return True 

  def runExperiment(self,e, trial_id):
    if e.workload == 'tpch' and e.query == '11':
      return self.runTPCH11(e, trial_id)

    schemaFolder = self.schemaMap[e.workload]
    scaleFactor = self.scaleFactorMap[e.dataset]
    queryFolder = self.queryMap[e.workload]
    queryFile = os.path.join(queryFolder, e.query + ".sql")

    command = "./systems/impala/run_impala.sh %s %s %s" % (schemaFolder, scaleFactor, queryFile)
    
    output = utils.runCommand(command)
    # Convert from seconds to milliseconds
    elapsed = 1000 * float(output.split(" ")[-1][:-2])
    return Result(trial_id, "Success", elapsed, "")
 
  def checkTPCH11(self):
    if not os.path.isfile(self.tpch11MainQueryFile):
      print("Could not find main query for Impala tpch11: %s" % self.tpch11MainQueryFile) 
      return False

    if not os.path.isfile(self.tpch11SubQueryFile):
      print("Could not find sub query for Impala tpch11: %s" % self.tpch11SubQueryFile)
      return False

    return True

  def runTPCH11(self, e, trial_id):
    scaleFactor = self.scaleFactorMap[e.dataset]
    schemaFolder = self.schemaMap[e.workload]
    command1 = "./systems/impala/run_impala.sh %s %s %s" % (schemaFolder, scaleFactor, self.tpch11SubQueryFile)
    command2 = "./systems/impala/run_impala.sh %s %s %s" % (schemaFolder, scaleFactor, self.tpch11MainQueryFile)
    output = utils.runCommand(command1)
    elapsed1 = 1000 * float(output.split(" ")[-1][:-2])
    
    output = utils.runCommand(command2)
    elapsed2 = 1000 * float(output.split(" ")[-1][:-2])
    return Result(trial_id, "Success", elapsed1 + elapsed2, "")

