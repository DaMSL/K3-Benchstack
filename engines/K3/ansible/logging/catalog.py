import sys
import os
import yaml

def parseHostIPs(path):
  ips = []
  with open(path, "r") as f:
    for line in f:
      if "ip" in line and (line.strip()[0] != "#"):
        keyVal = line.split(" ")[-1]
        ip = keyVal.split("=")[1][1:-2]
        ips.append(ip)
  return ips

def getResultVar(path):
  with open(path, "r") as f:
   d = yaml.load(f)
   resultVar  = d[0]['tasks'][0]['vars'].get('result_var', None)
   return resultVar

def parseApp(path):
  with open(path, "r") as f:
   d = yaml.load(f)
   numProcs   = d[0]['tasks'][0]['vars']['numprocs'] 
   appName    = d[0]['tasks'][0]['vars']['app_name']
   logEnabled = d[0]['tasks'][0]['vars'].get('log_enabled', None)
   resultVar  = d[0]['tasks'][0]['vars'].get('result_var', None)
   return (numProcs, appName, logEnabled, resultVar)

def buildCatalog(hostsPath, yamlPath):
  if not os.path.isfile(hostsPath):
    print("Failed to find hosts file: %s" % hostsPath)
    sys.exit(1)
  
  if not os.path.isfile(yamlPath):
    print("Failed to find deployment file: %s" % yamlPath)
    sys.exit(1)

  ips = parseHostIPs(hostsPath)
  (numProcs, appName, logEnabled, resultVar) = parseApp(yamlPath)
  ports = [40000 + i for i in range(numProcs)]

  basePath = os.path.join("/tmp/k3_results", "k3_" + appName)
  
  
  globs = []
  messages = []
  results = []
  for ip in ips:
    for port in ports:
      x = os.path.join(basePath, ip)
      x2 = os.path.join(x, str(port))
      prefix = ip + ":" + str(port)
      gname = prefix + "_Globals.dsv"
      mname = prefix + "_Messages.dsv"
      rname = prefix + "_Result.txt"
      if logEnabled:
        globs.append(os.path.join(x2, gname))
        messages.append(os.path.join(x2, mname))
      if resultVar: 
        results.append(os.path.join(x2, rname))
  return (globs, messages, results)

def validateCatalog(catalog, checkResult, werr):
  (gs, ms, rs) = catalog
  l = gs + ms
  if checkResult:
    l = l + rs

  for f in l:
    if not os.path.isfile(f):
      sys.stderr.write("WARNING: file does not exist: %s\n" % f)
      if werr:
        sys.stderr.write("ABORTING\n", file=sys.stderr)
        sys.exit(1)

def genCopyStatements(globalPaths, messPaths):
  template = "COPY %s FROM '%s' WITH DELIMITER '|';"
  output = []
  for glob in globalPaths:
    output.append(template % ("Globals", glob))
  for mess in messPaths:
    output.append(template % ("Messages", mess))
  return output


