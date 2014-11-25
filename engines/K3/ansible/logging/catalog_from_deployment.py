import sys
import os
import yaml

def parseHostIPs(path):
  ips = []
  with open(path, "r") as f:
    for line in f:
      if "ip" in line:
        keyVal = line.split(" ")[-1]
        ip = keyVal.split("=")[1][1:-2]
        ips.append(ip)
  return ips

def parseApp(path):
  with open(path, "r") as f:
   d = yaml.load(f)
   numProcs = d[0]['tasks'][0]['vars']['numprocs'] 
   appName  = d[0]['tasks'][0]['vars']['app_name']
   return (numProcs, appName)
  
if __name__ == "__main__":
  if (len(sys.argv) < 4):
    print("Usage: %s hosts_ini app_yml (globals|messages|result)", sys.argv[0])
    sys.exit(1)
 
  desiredCatalog = sys.argv[3]
  
  ips = parseHostIPs(sys.argv[1])
  (numProcs, appName) = parseApp(sys.argv[2])
  ports = [40000 + i for i in range(numProcs)]
  print(ports)

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
      rname = prefix +  "_Result.txt"
      globs.append(os.path.join(x2, gname))
      messages.append(os.path.join(x2, mname))
      results.append(os.path.join(x2, rname))
  
  if desiredCatalog == "globals":
    for g in globs:
      print(g)
  elif desiredCatalog == "messages":
    for m in messages:
      print(m)
  elif desiredCatalog == "result":
    for r in results:
      print(r)
  else:
    print("Invalid catalog: choose globals|messages|result")
