import os
import sys
from subprocess import Popen, PIPE

def stripKTraceHeader(lines):
  i = 0
  for line in lines:
    if "create table" in line:
      break
    i = i + 1
  return lines[i:]

def genKTraceSQL(k3_base, k3_source, result_variable, resultFiles):
  includes      = ["-I " + os.path.join(k3_base, x) for x in ["K3-Core/examples/sql/", "K3-Core/lib/k3"] ]
  ktrace_args   = ["compile","-l", "ktrace"]
  k3_executable = os.path.join(k3_base, "K3-Driver/dist/build/k3/k3")

  # Check that the K3 source exists
  if not os.path.isfile(k3_source):
    print("Failed to locate k3 source at: " + k3_source)
    sys.exit(1)

  # Check that K3 executable exists
  if not os.path.isfile(k3_executable):
    print("Failed to locate k3 executable at: " + k3_executable)
    sys.exit(1)

  # Build the KTrace command
  if result_variable:
    with open("/tmp/catalog.txt", "w") as f:
      for line in resultFiles:
        f.write(line + "\n")

    ktrace_flags = "".join(["flat-result-var=", result_variable, ":", "files=/tmp/catalog.txt"])
    ktrace_args = ktrace_args + ["--ktrace-flags", ktrace_flags]
  full_args = [k3_executable] + includes + ktrace_args + [k3_source]
  command = " ".join(full_args) 
  sys.stderr.write(command + "\n")

  # Execute the command
  process = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
  (output, err) = process.communicate()
  exit_code = process.wait()
  if exit_code != 0:
    print(err)


  # Process the output
  lines = output.split("\n")
  result = stripKTraceHeader(lines)
  if len(result) == 0:
    print("Failed to parse ktrace output:")
    for line in lines:
      print(line)
    sys.exit(1)
  return result 

if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("usage: %s k3_source result_variable" % sys.argv[0])
    exit(1)
 
  k3_source = sys.argv[1]
  result_variable = None
  if (len(sys.argv) >= 3):
    result_variable = sys.argv[3]

  output = genKTraceSQL(k3_source, result_variable)
  for line in output:
    print(line)
