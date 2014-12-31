import os
import subprocess

class ImpalaRunner:
  def __init__(self, schemaFolder, queryFile, scaleFactor, experiment):
    self.command = "./impala/run_impala.sh %s %s %s" % (schemaFolder, scaleFactor, queryFile)
    self.system = "Impala"
    self.experiment = experiment

  def run(self):
    try:
      output = subprocess.check_output(self.command, shell=True)
      elapsed = 1000 * float(output.split(" ")[-1][:-2])
      return elapsed

    except Exception as inst:
      return Failure("Run failed: " + str(inst))

class ImpalaQ11Runner:
  def __init__(self, schemaFolder, subQueryFile, mainQueryFile, scaleFactor, experiment):
    self.command1 = "./impala/run_impala.sh %s %s %s" % (schemaFolder, scaleFactor, subQueryFile)
    self.command2 = "./impala/run_impala.sh %s %s %s" % (schemaFolder, scaleFactor, mainQueryFile)
    self.system = "Impala"
    self.experiment = experiment

  def run(self):
    try:
      output = subprocess.check_output(self.command1, shell=True)
      elapsed1 = 1000 * float(output.split(" ")[-1][:-2])
      
      output = subprocess.check_output(self.command2, shell=True)
      elapsed2 = 1000 * float(output.split(" ")[-1][:-2])
      return elapsed1 + elapsed2

    except Exception as inst:
      return Failure("Run failed: " + str(inst))


class Impala:
  queryMap = {'tpch': './impala/sql/tpch/queries/'}
  schemaMap = {'tpch': './impala/sql/tpch/schema/'}
  scaleFactorMap  = {'tpch10g': '10g', 'tpch100g': '100g'} 

 
  # Assume the sql file has the same name as e.query with .sql extension
  def getRunner(self, e):
    if e.workload in self.queryMap:
      schemaFolder = self.schemaMap[e.workload]
      scaleFactor = self.scaleFactorMap[e.dataset]

      # Special case for tpch query 11
      if e.dataset == 'tpch10g' and e.query == '11':
        subQueryFile = './impala/sql/tpch/q11_10G/11sub.sql'
        mainQueryFile = './impala/sql/tpch/q11_10G/11.sql'
        
        if not os.path.isfile(mainQueryFile):
          raise Exception("Query file not found for Impala: " + mainQueryFile)
        
        if not os.path.isfile(subQueryFile):
          raise Exception("Query file not found for Impala: " + subQueryFile)

        return ImpalaQ11Runner(schemaFolder, subQueryFile, mainQueryFile, scaleFactor, e)

      else:
        queryFolder = self.queryMap[e.workload]
        queryFile = os.path.join(queryFolder, e.query + ".sql")

        if not os.path.isfile(queryFile):
          raise Exception("Query file not found for Impala: " + queryFile)
        
        if not os.path.exists(schemaFolder):
          raise Exception("Schema folder not found for Vertica: " + schemaFolder)

        if e.dataset not in self.scaleFactorMap:
          raise Exception("Unknown dataset for Impala: " + e.dataset)

        return ImpalaRunner(schemaFolder, queryFile, scaleFactor, e)

    else:
      raise Exception("Unknown workload for Impala: " + e.workload) 
