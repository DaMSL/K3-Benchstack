import os
import utils.utils as utils
import time

from entities.result import *
from entities.operator import *

class K3:
  def __init__(self, machines):
    self.machines = machines
    self.container = "K3"

  def name(self):
    return "K3"
  
  webAddress = "http://192.168.0.11:8002"
  k3Dir = '/K3'
  schedulerDir = '/K3/tools/scheduler/scheduler/'
  schedulerPath = os.path.join(schedulerDir, 'dispatcher.py')
  webServer = '/build'
  queryMap = {'tpch': os.path.join(k3Dir, 'examples/sql/tpch/queries/k3'),
              'amplab': os.path.join(k3Dir, 'examples/distributed/amplab')}

  def getBinaryName(self, e):
    return e.workload + 'q' + e.query

  def getYamlPath(self, e):
    return os.path.join('./systems/k3/yaml', self.getBinaryName(e) + '.yaml')


  # Compile program, place it in web server folder
  # Return true on success, false otherwise
  def compileProgram(self, e):
    sourceName = 'q' + e.query + '.k3'
    sourcePath = os.path.join(self.queryMap[e.workload], sourceName)
    if not os.path.isfile(sourcePath):
      print("Could not find k3 source: %s" % sourcePath)
      return False
  

    try:
      cleanupCmd = "rm %s" % os.path.join(self.k3Dir, '__build/A')
      utils.runCommand(cleanupCmd)
      cleanupCmd = "rm %s" % os.path.join(self.k3Dir, '__build/__build/*')
      utils.runCommand(cleanupCmd)
    except Exception:
      pass

    compileCmd = os.path.join(self.k3Dir, 'tools/scripts/run/compile.sh') + " " + sourcePath
    utils.runCommand('cd ' + self.k3Dir + ' && ' + compileCmd)

    binaryName = self.getBinaryName(e)
    src = os.path.join(self.k3Dir, '__build/A')
    dest = os.path.join(self.webServer, binaryName)
    utils.runCommand("mv %s %s" % (src,dest))
    return True

  def checkExperiment(self, e):
    if e.workload not in self.queryMap:
      print("Unknown workload for K3 %s" % e.workload)
      return False

    # Ensure k3scheduler and k3executor are available
    if not os.path.isfile(self.schedulerPath):
      print("Could not find k3scheduler: %s " % self.schedulerPath)
      return False

    execPath = os.path.join(self.webServer, 'k3executor')
    if not os.path.isfile(execPath):
      print("Could not find k3executor: %s " % execPath)
      return False

    # Try to compile the query for k3
    binaryName = self.getBinaryName(e)
    if not os.path.isfile(os.path.join(self.webServer, binaryName)):
      print("Need to compile %s %s" % (e.workload, e.query))
      res = self.compileProgram(e)
      if not res:
        print ("Failed to compile.")
        return False

    # Ensure yaml file exists for deployment
    queryYaml = self.getYamlPath(e)
    if not os.path.isfile(queryYaml):
      print("Could not find yaml file: %s" % queryYaml)
      return False

    return True

  # TODO toggle 100g vs 10g
  def runExperiment(self, e, trial_id, retries=3):
    sched = self.schedulerPath
    binary = self.webAddress + "/" + self.getBinaryName(e)
    yaml = self.getYamlPath(e)
    tmp = "/tmp/k3.yaml"

    precmd = ""
    # hack, replace 10g with 100g for tpch
    if e.dataset == "tpch100g":
      precmd = "sed s/10g/100g/g " + yaml
    else:
      precmd = "cat " + yaml
      
    
    cmd = "%s > %s && PYTHONPATH=%s python %s --binary %s --roles %s 2>&1"  % (precmd, tmp, self.schedulerDir, self.schedulerPath, binary, tmp)

    output = utils.runCommand(cmd)
    lines = output.split('\n')

    elapsedTime = 0
    r = None
    operators = []
    r = None
    totalOpTime = 0.0
    for line in lines:
      if "Time Query:" in line:
        elapsedTime = int(line.split(":")[-1].strip())
        r = Result(trial_id, "Success", elapsedTime, "")
      if "GROUPBY" in line:
        time = int(line.split(":")[-1].strip())
        totalOpTime = totalOpTime + time
        o = Operator(trial_id, len(operators), "GROUPBY", time, 0, 0, "")
        operators.append(o)
      if "JOIN" in line:
        time = int(line.split(":")[-1].strip())
        totalOpTime = totalOpTime + time
        o = Operator(trial_id, len(operators), "JOIN", time, 0, 0, "")
        operators.append(o)

  
    if elapsedTime == 0:
      r = Result(trial_id, "Failure", 0, "Failed to find elapsedTime in output. error.")

    for o in operators:
      o.percent_time = 100 * o.time / totalOpTime
    r.setOperators(operators)

    return r
