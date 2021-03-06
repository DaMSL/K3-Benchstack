import os
import sys
import utils.utils as utils
from entities.result import *
from entities.operator import *



class oracleOp(object):
  def __init__ (self, oid, name, time, percent, mem, obj):
    self.oid = oid
    self.name = name
    self.time = time
    self.percent = percent
    self.mem = mem
    self.obj = obj

class Oracle:
  def __init__(self, machine):
    self.machines = [machine]
    self.container = "orcl2"

  def name(self):
    return "Oracle"

  workloadMap = {'tpch': './systems/oracle/sql/tpch', 'amplab': './systems/oracle/sql/amplab'}
  portMap = {'tpch': '11521', 'amplab':'12521'}
  datasetMap = {'tpch10g': 'mddb2', 'tpch100g': 'mddb', 'amplab':'mddb'}

  
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
    port = self.portMap[e.workload]
    return self.runOracle(host, 'orcl', port, queryFile, trial_id) 

  def runOracle(self, host, database, port, queryFile, trial_id):
    command = "ORACLE_HOST=%s ORACLE_PORT=%s ./systems/oracle/run_oracle.sh %s %s" % (host, port, database, queryFile)
    output = utils.runCommand(command)
    lines = output.split('\n')

    # TODO: Try to capture metrics for querried run under 100 ms
    if lines[0].strip() == '' or lines[1].strip() == '':
      elapsed = 100
      result =  Result(trial_id, "Success", elapsed, "")
      return result
  
    run_time = 1000 * float(lines[0].strip())
    exec_time = 1000 * float(lines[1].strip())
    index = 1
    pre_percent = 100.
    ops = []
    for line in lines[2:]:
      vals = [ val.strip() for val in line.split(',') ]
      if len(vals) != 10:
        continue
      depth, op, obj, mem, pga_max, pga_avg, time, percent = (int(vals[1]), vals[2], vals[3], float(vals[5]), float(vals[6]), float(vals[7]), int(vals[8]), float(vals[9]))
      ops.append(oracleOp(index, op, time, percent, pga_max, obj))
      pre_percent -= percent
      index += 1

    operators = [Operator(trial_id, 0, 'Pre-Execution', run_time - exec_time, pre_percent, 0, '')]
    for op in ops:
      operators.append(Operator(trial_id, op.oid, op.name, op.time, op.percent, op.mem, op.obj))

    result =  Result(trial_id, "Success", exec_time, "")
    result.setOperators(operators)
    return result 
