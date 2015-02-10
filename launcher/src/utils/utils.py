import subprocess
import sys
import os
# Wrapper around subprocess.check_output with some decent arguments
# Exceptions are left unhandled, Except for CTRL-C, which kills the launcher

def runCommand(command):
  return subprocess.check_output(command, shell=True)

def runCommand_stderr(command):
  return subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)

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
