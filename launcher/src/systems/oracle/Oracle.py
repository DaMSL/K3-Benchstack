import os
import subprocess
from entities.result import *

class Oracle:
  def name(self):
    return "Oracle"

  workloadMap = {'tpch': './systems/oracle/sql/tpch'}
  
  # Verify that Oracle can run the experiment.
  # Assume the sql file is (e.query).sql
  # Check for missing .sql files, etc.
  def checkExperiment(self, e):
    if e.workload not in self.workloadMap:
      print("Unknown workload for Oracle: %s" % e.workload)
      return False
      
    queryFolder = self.workloadMap[e.workload]
    queryFile = os.path.join(queryFolder, e.query + ".sql")      

    if not os.path.isfile(queryFile):
      print("Oracle can't find sql file: %s" % queryFile)
      return False

    else:
      return True
  
  def runExperiment(self, e):
    queryFolder = self.workloadMap[e.workload]
    queryFile = os.path.join(queryFolder, e.query + ".sql")      
   
    # TODO different database for each dataset 
    # instead of "orcl"
    return self.runOracle("orcl", queryFile) 

  def runOracle(self, database, queryFile):
    command = "./systems/oracle/run_oracle.sh %s %s" % (database, queryFile)
    try:
      output = subprocess.check_output(command, shell=True)
      print(output)
      # Parse hh:mm:ss output
      hms = output.split(":")
      secContrib = 1000 * float(hms[-1][:-1])
      minContrib = 60 * 1000 * float(hms[-2])
      hourContrib = 60 * 60 * 1000 * float(hms[-3]) 
      elapsed = secContrib + minContrib + hourContrib
      return Success(float(elapsed))

    except Exception as inst:
      return Failure("Oracle Run failed: " + str(inst))
