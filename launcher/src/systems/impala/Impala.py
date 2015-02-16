import os
import utils.utils as utils
import re

from entities.result import *
from entities.operator import *

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

  queryMap = {'tpch': 'systems/impala/sql/tpch/queries/',
              'amplab': 'systems/impala/sql/amplab/queries/'}
  schemaMap = {'tpch': 'systems/impala/sql/tpch/schema/',
               'amplab': 'systems/impala/sql/amplab/schema/'}
  scaleFactorMap  = {'tpch10g': '10g', 'tpch100g': '100g', 'amplab': 'amplab'} 

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
   
   # if e.dataset == 'tpch10g' and e.query == '11':
    if e.workload == 'tpch' and e.query == '11':
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

    command = "./systems/impala/run_impala.sh %s %s" % (scaleFactor, queryFile)

    # Parse output and summary    
    output = utils.runCommand(command)
    return self.parseOutput(trial_id, output)

  def parseOutput(self, trial_id, output):
    lines = output.split("\n")
    elapsed = 0
    summary = []
    for i in xrange(len(lines)):
      if "Fetched" in lines[i]:
        elapsed = 1000 * float(lines[i].split(" ")[-1][:-2])
        summary = lines[i+1:]
        break

    # strip summary header and footer
    summary = summary[:-2]
    summary = summary[3:]
   
    # split at pipes and strip whitespace 
    clean = []
    offsets = []
    for line in summary:
      x = line.split('|')
      offsets.append(len(x) - 11)
      clean.append([s.strip(" -") for s in x])

    # break apart columns
    operators = []
    p = re.compile('(\d+\.\d+)(.+)')
    p2 = re.compile('(\d+\.*\d*) (.+)')
    totalTime = 0
    lastOperator = None
    for line,off in zip(clean, offsets):
      opNum = int(line[1+off].split(":")[0])
      opName = line[1+off].split(":")[1]
      numHosts = int(line[2+off])
 
      # convert time to ms
      opTimeStr = line[4+off]
      m = p.match(opTimeStr)
      time = float(m.group(1))
      unit = m.group(2)
      if unit == 's':
        time = time * 1000
      elif unit == 'ms':
        pass
      elif unit == 'us':
        time = time / 1000
      else:
        return Result(trial_id, "Failure", 0, "Unrecognized time unit: " + unit)

      # Table or object of interest
      obj = line[-2]
     
      # convert mem to mb 
      maxMemStr = line[off+7]
      m = p2.match(maxMemStr)
      maxMem = float(m.group(1)) 
      unit = m.group(2)
      if unit == 'B':
        maxMem = maxMem / (1024 * 1024)
      elif unit == 'KB':
        maxMem = maxMem / 1024
      elif unit == 'MB':
        pass
      elif unit == 'GB':
        maxMem = maxMem * 1024
      else:
        return Result(trial_id, "Failure", 0, "Unrecognized memory unit:" + unit)

      # approx. memory usage
      maxMem = maxMem * numHosts

      # dont double count times or memory for sub-operators. 
      # simply concatenate the names
      o = Operator(trial_id, opNum, opName, time, 0, maxMem, obj)
      if off == 1:
        lastOperator.operator_name += ", " + opName
        time = 0
        # maxMem = 0
      else:
        operators.append(o)
        lastOperator = o
        totalTime = totalTime + time

    for operator in operators:
      operator.percent_time = 100 * operator.time / totalTime

    r =  Result(trial_id, "Success", elapsed, "")
    r.setOperators(operators)

    return r

  def checkTPCH11(self):
    if not os.path.isfile(self.tpch11MainQueryFile):
      print("Could not find main query for Impala tpch11: %s" % self.tpch11MainQueryFile) 
      return False

    if not os.path.isfile(self.tpch11SubQueryFile):
      print("Could not find sub query for Impala tpch11: %s" % self.tpch11SubQueryFile)
      return False

    return True

  # TODO operator metrics
  def runTPCH11(self, e, trial_id):
    scaleFactor = self.scaleFactorMap[e.dataset]
    schemaFolder = self.schemaMap[e.workload]
    command1 = "./systems/impala/run_impala.sh %s %s" % (scaleFactor, self.tpch11SubQueryFile)
    command2 = "./systems/impala/run_impala.sh %s %s" % (scaleFactor, self.tpch11MainQueryFile)
    output = utils.runCommand(command1)
    r1 = self.parseOutput(trial_id, output)
    
    output = utils.runCommand(command2)
    r2 = self.parseOutput(trial_id, output)
    for op in r2.operators:
      op.operator_num = len(r1.operators) + op.operator_num + 1

    allOps = r1.operators + r2.operators
    
    totalTime = 0
    for op in allOps:
      totalTime = totalTime + op.time

    for op in allOps: 
      op.percent_time = 100 * op.time / totalTime

    r = Result(trial_id, "Success", r1.elapsed + r2.elapsed, "")
    r.setOperators(allOps)

    return r
