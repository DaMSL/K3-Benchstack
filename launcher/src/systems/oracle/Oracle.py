import os
import sys
import utils.utils as utils
from entities.result import *
from entities.operator import *



class oracleJob(object):
  def __init__ (self, depth):
    self.depth = depth
    self.oplist = []
    self.objs = []
    self.mem = 0
    self.time = 0
    self.percent = 0.0
  def addOp(self, op, obj):
    if op not in self.oplist:
      self.oplist.append(op)
    if len(obj) > 0 and obj not in self.objs:
      self.objs.append(obj)
  def name(self):
    return(','.join(self.oplist))
  def objects(self):
    return(','.join(self.objs))
  def time(self):
    return self.end - self.start
  def update(self, mem, time, percent):
    self.time += time
    self.percent += percent

  #  Helper function to check if a Spark job exists in a given list
def checkJob (jl, d):
  for job in jl:
    if job.depth == d:
      return jl.index(job)
  return -1


class Oracle:
  def __init__(self, machine):
    self.machines = [machine]
    self.container = "orcl"

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
    if lines[0].strip() == '' or lines[1].strip() == '':
      elapsed = 100
      result =  Result(trial_id, "Success", elapsed, "")
      return result
  
    elapsed = 1000 * float(lines[0].strip())
    exec_time = 1000 * float(lines[1].strip())
    preexec_time = max((elapsed - exec_time), 0)
    prexec_percent = 100.0
    ops = []

    #  Split Query plan into jobs based on exchange operations
    cur_op = oracleJob(0)
    px_op = oracleJob(-1)
    px_op.addOp('EXCHANGE', '')
    joblist = [cur_op]
    total_time = preexec_time
    for line in lines[2:]:
      vals = [ val.strip() for val in line.split(',') ]
      if len(vals) != 8:
        continue
      depth, op, obj, mem, time, percent = (int(vals[1]), vals[2], vals[3], long(vals[5]), int(vals[6]), float(vals[7]))
      total_time += time

      if op.startswith('PX'):
        px_op.update(mem, time, 0)
      

      if op.startswith('PX REC'):
        exists = checkJob(joblist, depth)
        cur_op = joblist[exists] if exists > 0 else oracleJob(depth)
#        cur_op.update(mem, time, 0)
        if exists < 0:
          joblist.append(cur_op)
#      elif op.startswith('PX'):
#        cur_op.update(mem, time, 0)
      else:
        cur_op.addOp(op, obj)
        cur_op.update(mem, time, 0)

    joblist.append(px_op)
    
    for j in joblist:
      j.percent = 100.0 * j.time / float(total_time)
    prexec_percent = 100.0 * preexec_time / float(total_time)

#    for j in joblist:
#      print (j.name(), j.time, j.percent, j.mem) 

    operators = [Operator(trial_id, i, joblist[i].name(), joblist[i].time, joblist[i].percent, joblist[i].mem, joblist[i].objects()) for i in range(len(joblist))]
    operators.append(Operator(trial_id, -1, 'Pre-Execution', preexec_time, prexec_percent, 0, ""))
#    operators.append(Operator(trial_id, operator_num, operator_name, time, percent_time, memory))
    result =  Result(trial_id, "Success", elapsed, "")
    result.setOperators(operators)

    return result 
