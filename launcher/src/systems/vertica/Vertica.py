import os
import re

import utils.utils as utils
from entities.result import *
from entities.operator import *

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
    # First line of output is a HINT for profiling
    lines = output.split("\n")
    hint = lines[0]
    try:
      m = re.search("[.]*transaction_id=(\d+) and statement_id=(\d+)[.]*", hint)
      transaction = m.group(1)
      statement = m.group(2)
    except Exception as inst:
      return Result(trial_id, "Failure", 0, "Failed to parse profiling hint: %s" % (str(inst)))
    operators = [] 
    try:  
      operators = self.doOperatorProfiling(trial_id, transaction, statement)
    except Exception as inst:
      return Result(trial_id, "Failure", 0, "Failed to query Vertica profiling tables %s" % (str(inst)))
    
    # Second line is the elapsed time
    elapsed = lines[1].split(" ")[-2]
    result = Result(trial_id, "Success", float(elapsed), "")
    result.setOperators(operators)
    return result

  def doOperatorProfiling(self, trial_id, t_id, s_id):
    query = ""
    with open("systems/vertica/sql/profiling/operators.sql.template","r") as f:
      query = f.read() % {'t_id': t_id, 's_id': s_id}

    tempfile = "systems/vertica/sql/profiling/curr_operators.sql"
    with open(tempfile, "w") as f:
      f.write(query)

    output = utils.runCommand("systems/vertica/sql/profiling/run_sql.sh %s" % tempfile)
    
    utils.runCommand("rm %s" % (tempfile) )
    operators = []
    lines = output.split('\n')
    for line in lines:
      vals = [ val.strip() for val in line.split(',')]
      if len(vals) >= 6:
        operators.append(Operator(trial_id, vals[0], vals[1], vals[2], vals[3], vals[4]))
        desc = "".join(vals[5:]))

    return operators
