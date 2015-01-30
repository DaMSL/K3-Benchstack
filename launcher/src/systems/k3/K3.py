import os
import utils.utils as utils
import time

from entities.result import *

class K3:
  def __init__(self, machines):
    self.machines = machines
    self.container = "k3"

  def name(self):
    return "K3"

  k3Core = '/k3/K3-Core'  
  k3Driver = '/k3/K3-Driver'
  schedulerPath = './systems/k3/k3scheduler'
  webServer = '/build'
  queryMap = {'tpch': os.path.join(k3Core, 'examples/sql/tpch/queries/k3')}

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
      cleanupCmd = "rm %s" % os.path.join(self.k3Driver, '__build/A')
      utils.runCommand(cleanupCmd)
      cleanupCmd = "rm %s" % os.path.join(self.k3Driver, '__build/__build/*')
      utils.runCommand(cleanupCmd)
    except Exception:
      pass

    compileCmd = os.path.join(self.k3Driver, 'compile.sh') + " " + sourcePath
    utils.runCommand('cd ' + self.k3Driver + ' && ' + compileCmd)

    binaryName = self.getBinaryName(e)
    src = os.path.join(self.k3Driver, '__build/A')
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

  def runExperiment(self, e, trial_id, retries=3, pause=30):
    sched = self.schedulerPath
    binary = self.getBinaryName(e)
    yaml = self.getYamlPath(e)

    cmd = "%s %s 128 -y %s"  % (sched, binary, yaml)

    output = utils.runCommand(cmd)
    print(output)
    lines = output.split('\n')

    elapsedTime = 0
    r = None
    for line in lines:
      if "Time query:" in line:
        elapsedTime = int(line.split(":")[-1].strip())
        r = Result(trial_id, "Success", elapsedTime, "")
      if "RECEIVED UNKNOWN STATUS" in line and retries > 0:

        print("RECEIVED UNKNOWN STATUS... retrying query in %s seconds" % pause)
        time.sleep(pause)

        return self.runExperiment(e, trial_id, retries-1, pause*2)
    
    if elapsedTime == 0:
      r = Result(trial_id, "Failure", 0, "Failed to find elapsedTime in output. error.")

    print("sleeping to avoid addr_in_use errors")
    time.sleep(30)
    return r
