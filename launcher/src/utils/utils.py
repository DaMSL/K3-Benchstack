import subprocess
import sys
import os

import signal


# Wrapper around subprocess.check_output with some decent arguments
# Exceptions are left unhandled, Except for CTRL-C, which kills the launcher
def runCommand(command):
  timeout = 60 * 60 # 1 hour timeout
  signal.signal(signal.SIGALRM, alarm_handler)
  signal.alarm(timeout)  
  proc = None
  try:
    proc = subprocess.Popen(command, shell=True, preexec_fn=os.setsid, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    signal.alarm(0)  # reset the alarm
    return stdout
  except KeyboardInterrupt as inst:
    proc.kill()
    os.killpg(proc.pid, signal.SIGTERM)
    raise KeyboardInterrupt
  except Alarm:
    proc.kill()
    os.killpg(proc.pid, signal.SIGTERM)
    timeout = 30 * 60
    return "Timed out after %d seconds!" % timeout
  

def runCommand_stderr(command):
  return subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)

class Alarm(Exception):
  pass

def alarm_handler(signum, frame):
  raise Alarm

def runCommandWithTimeout(command, timeout):
  try:
    stdoutdata, stderrdata = proc.communicate()
    signal.alarm(0)  # reset the alarm
  except Alarm:
    return "Timed out after %d seconds!" % timeout

def checkDir(path):
  runCommand("mkdir -p %s" % (path))
  return path

def buildIndex(path, title):
  index =  '<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 3.2 Final//EN"><html>'
  index += '<title>%s</title>' % title
  index += '<body><h2>%s</h2>' % title
  for img in sorted(os.listdir(path)):
    if img.startswith('index'):
      continue
    index += '<img src="%s" />' % img
  index += '</body></html>'
  indexfile = open(path + '/index.html', 'w')
  indexfile.write(index)
  indexfile.close()
