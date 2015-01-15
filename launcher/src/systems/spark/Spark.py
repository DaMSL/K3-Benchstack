import sys
import os
import json
import utils.utils as utils
from entities.result import *
from entities.operator import *

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

    output = utils.runCommand(command).split('\n')
    print ("OUTPUT:")
    print (output)
    logfile = output[0].split(':')[-1]+'/EVENT_LOG_1' 
    elapsed = float(output[1].split(":")[-1][:-1])
    print ("LOGFILE:" , logfile)
    source_data = open(logfile, 'r').read()
    ops = []
    for row in source_data.split('\n'):
      if row == '':
        continue
      event = json.loads(row)
      if event['Event'] == 'SparkListenerStageCompleted':
        stage = event['Stage Info']
        op_num = stage['Stage ID']
        op_name = stage['Stage Name'].split('.')[0].encode('ascii')
        op_time = stage['Completion Time'] - stage['Submission Time']
        op_percent = float(op_time) / elapsed
        op_mem = 0
        for rdd in stage['RDD Info']:
          op_mem += rdd['Memory Size']/1024/1024
        ops.append(Operator(trial_id, op_num, op_name, op_time, op_percent, op_mem))
    print "OP LIST:"
    for op in ops:
      print op
    result = Result(trial_id, "Success", elapsed, "")
    result.setOperators(ops)
    return result
