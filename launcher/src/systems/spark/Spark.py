import sys
import os
import json
import utils.utils as utils
from entities.result import *
from entities.operator import *

from entities.result import *


class sparkStage(object):
  def __init__(self, sid, name, time, mem):
    self.sid = sid
    self.name = name
    self.time = time
    self.mem = mem
    self.percent = 0.0

# Mapping for each query to the highest stage included for RDD chaching from HDFS
#  -- all stages upto & including it are caching and omitted from metric
rddLoadingStage = {'tpch': {1:1, 3:5, 5:12, 6:1, 11:2, 18:5, 22:5},
                   'amplab': {1:1, 2:1, 3:3}  }


class Spark:
  scaleFactorMap  = {'tpch10g': '10g', 'tpch100g': '100g', 'amplab': 'amplab', "sgd10g": "10g", "sgd100g": "100g"}
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

    elif e.workload == 'ml' and e.query == 'k_means':
      return "KMeans"
    
    elif e.workload == 'ml' and e.query == 'sgd':
      return "SGD"


    else:
      print("Unknown workload for Spark %s" % (e.workload) )
      sys.exit(1)

  # Verify that the .jar file has been built.
  def checkExperiment(self, e):

    if e.dataset not in self.scaleFactorMap:
      print("Unknown dataset for Spark: %s" % (e.dataset) )
      return False

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
    #if e.dataset == "tpch100g" and e.workload == "tpch" and (e.query == "18" or e.query == "22"):
    #  return Result(trial_id, "Skipped", 0, "TPCH 100G Query %s fails to finish on Spark" % (e.query))

    className = self.getClassName(e)
    sf = self.scaleFactorMap[e.dataset]
    command = "systems/spark/run_spark.sh %s %s %s" % (self.jarFile, sf, className) 

    output = utils.runCommand_stderr(command)
    #output = utils.runCommand(command)

    #  Extract Query Plan from output, parse & convert to set of operation tuples
    elapsed = 0
    for l in output.split('\n'):
      if l.startswith('Elapsed:'):
        elapsed =  int(float(l[8:].strip()))
        break

    # TODO we skip operator profiling on ML for now
    if e.workload == "ml":
      return Result(trial_id, "Success", elapsed, "")

    #  Find the JSON formatted event log (should be first line of output
    out_lines = output.split('\n')
    logfile = ''
    for l in output.split('\n'):
      if "EventLoggingListener" in l:
        logfile = l.split(':')[-1]+'/EVENT_LOG_1'
        break

    print "LOGFILE: %s" % logfile

    # Load & parse JSON event log to retrieve stage metrics
    source_data = open(logfile, 'r').read()
    eventlist = [json.loads(ev) for ev in source_data.split('\n') if len(ev) > 0]
    stages = [ev['Stage Info'] for ev in eventlist if ev['Event'] == 'SparkListenerStageCompleted']

    total_time = 0.
    ops = []

    # Collect data for each stage
    for s in stages:
      mem = 0
      for rdd in s['RDD Info']:
        mem += rdd['Memory Size']/1024/1024
      sid = s['Stage ID']

      # Identify stage operation:
            #  1. All table caching from HDFS is a TableScan
            #  2. Any collection operation is an Exchange (shuffle) op
            #  3. Any call to HashJoin on the call stack is a Join Op
            #  4. Everything else is deemed a GroupBy or pipeline of it (filter, project, etc...)
      operation = ('TableScan' if s['Stage ID'] <= rddLoadingStage[e.workload][int(e.query)] 
        else 'Exchange' if s['Stage Name'].startswith('collect') 
          else 'Join' if 'HashJoin' in s['Details'] 
            else 'GroupBy' )
      
      # EXCLUDE TableScan time (opertion & mem will still show)
      time = (0 if operation == 'TableScan'
                else int(s['Completion Time']) - int(s['Submission Time']) )
      total_time += time
      ops.append(sparkStage(sid, operation, time, mem))

    operations = []
    for s in ops:
      operations.append(Operator(trial_id, s.sid, s.name, s.time, float(s.time)/total_time, s.mem, ''))

    result = Result(trial_id, "Success", elapsed, "")
    result.setOperators(operations)
    return result
