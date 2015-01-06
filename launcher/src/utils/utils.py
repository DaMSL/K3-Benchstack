import subprocess
import sys

class InterruptException(Exception):
  pass

# Wrapper around subprocess.check_output with some decent arguments
# Exceptions are left unhandled, Except for CTRL-C, which kills the launcher
def runCommand(command):
  try:
    return subprocess.check_output(command, shell=True)
  except KeyboardInterrupt:
    print("Recieved interrput. Stopping Command.")
    raise InterruptException("Received CTRL-C")
