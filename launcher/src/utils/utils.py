import subprocess
import sys

# Wrapper around subprocess.check_output with some decent arguments
# Exceptions are left unhandled, Except for CTRL-C, which kills the launcher

def runCommand(command):
  return subprocess.check_output(command, shell=True)

def runCommand_stderr(command):
  return subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)

def checkDir(path):
  runCommand("mkdir -p %s" % (path))
  return path
