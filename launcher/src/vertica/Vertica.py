import os
import subprocess

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
      queryFolder = self.queryMap[e.workload]
      queryFile = os.path.join(queryFolder, e.query + ".sql")      

      if not os.path.isfile(queryFile):
        raise Exception("Query file not found for Vertica: " + queryFile)

      return VerticaRunner(e.dataset, queryFile, e)

    else:
      raise Exception("Unknown workload for Vertica: " + e.workload) 
