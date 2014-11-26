from ktrace import *
from catalog import *
import sys

if __name__ == "__main__":
  if len(sys.argv) < 4:
    print("usage: %s hosts_ini deploy_yml k3_source" % sys.argv[0])
    exit(1)
 
  hosts_ini = sys.argv[1]
  deploy_yml = sys.argv[2]
  k3_source = sys.argv[3]
 
  # Check for results variable 
  checkResult = False  
  resultVar = getResultVar(deploy_yml)
  if resultVar:
    checkResult = True

  # Build and validate catalog 
  c = buildCatalog(hosts_ini, deploy_yml) 
  validateCatalog(c, checkResult, False)
  (globFiles, messFiles, resFiles) = c
   
  # Call Ktrace to generate SQL (generates loaders for Result) 
  ktrace_lines  = genKTraceSQL(k3_source, resultVar, resFiles)

  # Generate COPY statements for Globals and Messages
  copy_lines = genCopyStatements(globFiles, messFiles) 

  # Dump SQL to stdout for now
  for l in ktrace_lines:
    print(l)
  for l in copy_lines:
    print(l)
