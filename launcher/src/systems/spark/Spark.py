import sys
import os
import json
import utils.utils as utils
from entities.result import *
from entities.operator import *

from entities.result import *


class sparkJob(object):
    def __init__ (self, depth):
        self.depth = depth
        self.oplist = []
        self.mem = 0
        self.start = 0
        self.end = 0
        self.percent = 0.0
        self.job_id = 0
    def addOp(self, op):
        if op not in self.oplist:
            self.oplist.append(op)
    def name(self):
        return('-'.join(self.oplist))
    def time(self):
        return self.end - self.start
    def set(self, job_id, name, start, end):
        self.job_id = job_id
        self.oplist = [name]
        self.start= start
        self.end = end

  #  Helper function to check if a Spark job exists in a given list
def checkJob (jl, d):
  for job in jl:
    if job.depth == d:
      return jl.index(job)
  return -1



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

    output = utils.runCommand_stderr(command)

    #  Extract Query Plan from output, parse & convert to set of operation tuples
    q_start = output.find('---->>') + 6
    q_end = output.find('<<----', q_start)
    ops = []
    for line in output[q_start:q_end].split('\n'):
      cur_depth = 0
      while len(line) > 0 and line[0] == ' ':
        cur_depth += 1
        line = line[1:]
      ops.append((cur_depth, line[:line.find(' ')]))
    for o in ops:
      print o

    #  Split Query plan into jobs based on exchange operations
    cur_op = sparkJob(0)
    joblist = [cur_op]
    for depth, op in ops:
      if op.startswith('Exchange'):
        exists = checkJob(joblist, depth)
        cur_op = joblist[exists] if exists > 0 else sparkJob(depth)
        if exists < 0:
          joblist.append(cur_op)
      else:
        cur_op.addOp(op)
    for j in joblist:
      print j.name()


    #  Find the JSON formatted event log (should be first line of output
    out_lines = output.split('\n')
    print out_lines
    for l in output.split('\n'):
      if "EventLoggingListener" in l:
        logfile = l.split(':')[-1]+'/EVENT_LOG_1'
        break

    #  Get elapse time: should be last line of output
 #   elapsed = float(out_lines[-1].split(":")[-1][:-1])
    

    print "LOGFILE: %s" % logfile
    source_data = open(logfile, 'r').read()
    eventlist = [json.loads(e) for e in source_data.split('\n') if len(e) > 0]
    
    # Load start time, job list and stage list from JSON
    app_start_time = [e['Timestamp'] for e in eventlist if e['Event'] == 'SparkListenerApplicationStart'][0]
    jobmap = [e['Stage IDs'] for e in eventlist if e['Event'] == 'SparkListenerJobStart']
    stages = [e['Stage Info'] for e in eventlist if e['Event'] == 'SparkListenerStageCompleted']
    stagelist = [None] * len(stages)

    # Sort stage list
    for s in stages:
      stagelist[s['Stage ID']] = s

    # Collect metrics from all stages grouped by job ID
    for j in range(len(jobmap)):
      start_time = []
      end_time = []
      for s in jobmap[j]:
        start_time.append(stagelist[s]['Submission Time'])
        end_time.append(stagelist[s]['Completion Time'])
        for rdd in stagelist[s]['RDD Info']:
          joblist[j].mem += rdd['Memory Size']/1024/1024
      joblist[j].start = min(start_time)
      joblist[j].end = max(end_time)
      joblist[j].job_id = j

    exec_end = max([j.end for j in joblist])
    exec_start = min([j.start for j in joblist])

    #  Calculate pre-execution and inter-job execution time
    run_time = exec_end - app_start_time
    pre_job = sparkJob(0)
    pre_job.set(-1, "PRE-EXECUTION", app_start_time, exec_start)
    pre_job.percent = 100.0 * float(pre_job.time()) / float(run_time)
    exec_time = pre_job.time()
    for j in range(len(joblist)):
      exec_time += joblist[j].time()
      joblist[j].percent = 100.0 * float(joblist[j].time()) / float(run_time)
    com_job = sparkJob(0)
    com_job.set(-1, "COMM / OTHER", 0, run_time - exec_time)
    com_job.percent = 100.0 * float(com_job.time()) / float(run_time)

    for j in joblist:
      print (j.job_id, j.name(), j.time(), j.percent, j.mem)
    print (pre_job.job_id, pre_job.name(), pre_job.time(), pre_job.percent, pre_job.mem)
    print (com_job.job_id, com_job.name(), com_job.time(), com_job.percent, com_job.mem)
    operations = [Operator(trial_id, -1, pre_job.name(), pre_job.time(), pre_job.percent, 0)]
    operations.append(Operator(trial_id, -1, com_job.name(), com_job.time(), com_job.percent, 0))
    for j in joblist:
      operations.append(Operator(trial_id, j.job_id, j.name(), j.time(), j.percent, j.mem))

    result = Result(trial_id, "Success", run_time, "")
    result.setOperators(operations)
    return result
