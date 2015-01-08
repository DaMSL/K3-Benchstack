import subprocess
import sys

# Wrapper around subprocess.check_output with some decent arguments
# Exceptions are left unhandled, Except for CTRL-C, which kills the launcher
def runCommand(command):
  return subprocess.check_output(command, shell=True)
