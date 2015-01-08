import sys
import os
import utils.utils as utils

from entities.result import *

class Spark:
  scaleFactorMap  = {'tpch10g': '10g', 'tpch100g': '100g'}
  buildDir = 'systems/spark/'
  jarFile  = os.path.join(buildDir, 'target/scala-2.10/spark-benchmarks_2.10-1.0.jar')
  
  def __init__(self, machines):
    self.machines = machines
    self.container = "sparkWorker"


  def name(self):
    return "Spark"

  def getClassName(self, e):
    if e.workload == 'tpch':
      return "TPCHQuery%s" % (e.query)
 
    elif e.workload == 'amplab':
      return "AmplabQuery%s" % (e.query)

    else:
      print("Unknown workload for Spark %s" % (e.workload) )
      sys.exit(1)

  # Verify that the .jar file has been built.
  def checkExperiment(self, e):

    if e.dataset not in self.scaleFactorMap:
      print("Unknown dataset for Spark: %s" % (e.dataset) )

    # Check if we need to build the jar file for spark programs
    if not os.path.isfile(self.jarFile):
      try:
        buildCommand = "cd %s && sbt package" % (self.buildDir)
        utils.runCommand(buildCommand)
      except Exception as inst:
        print("Failed to build jar file for Spark queries") 
        print(inst)
        sys.exit(1)

    # TODO Check that the correct class exists in the jar file using sbt "show discoveredMainClasses"
    return True

  def runExperiment(self, e, trial_id):
    if e.dataset == "tpch100g" and e.workload == "tpch" and (e.query == "18" or e.query == "22"):
      return Result(trial_id, "Skipped", 0, "TPCH 100G Query %s fails to finish on Spark" % (e.query))

    className = self.getClassName(e)
    sf = self.scaleFactorMap[e.dataset]
    command = "systems/spark/run_spark.sh %s %s %s" % (self.jarFile, sf, className) 

    output = utils.runCommand(command)
    elapsed = float(output.split(":")[-1][:-1])
    return Result(trial_id, "Success", elapsed, "")
