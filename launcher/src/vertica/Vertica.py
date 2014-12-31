import os
import subprocess
from data import *

class FailRunner:
  def __init__(self, message, experiment, system):
    self.message = message
    self.experiment = experiment
    self.system = system

  def run(self):
    return Failure(self.message)


class VerticaRunner:
  def __init__(self, schema, queryFile, experiment):
    self.command = "./vertica/run_vertica.sh %s %s" % (schema, queryFile)
    self.system = "Vertica"
    self.experiment = experiment

  def run(self):
    try:
      output = subprocess.check_output(self.command, shell=True)
      elapsed = output.split(" ")[-2]
      return float(elapsed)

    except Exception as inst:
      return Failure("Vertica Run failed: " + str(inst))


class Vertica:
  queryMap = {'tpch': './vertica/sql/tpch'}

  # Assume the sql file has the same name as e.query with .sql extension
  def getRunner(self, e):
    if e.workload in self.queryMap:

      # Exception for tpch5
      if e.workload == 'tpch' and e.query == '5':
        return FailRunner("Vertica tpch5 is too slow. Aborting trial.", e, "Vertica")

      queryFolder = self.queryMap[e.workload]
      queryFile = os.path.join(queryFolder, e.query + ".sql")      

      if not os.path.isfile(queryFile):
        raise Exception("Query file not found for Vertica: " + queryFile)

      return VerticaRunner(e.dataset, queryFile, e)

    else:
      raise Exception("Unknown workload for Vertica: " + e.workload) 
