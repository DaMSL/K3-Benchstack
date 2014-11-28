import sys
import os
from subprocess import Popen, PIPE

def validateFile(path, name):
  if not os.path.isfile(path):
    print("Could not find %s file at %s" % (name, path))
    sys.exit(1)

def runK3(hosts_ini, deploy_yml):
  cmd = "ansible-playbook -v -i %s %s --forks=5000" % (hosts_ini, deploy_yml)
  os.system(cmd) 


def runKTrace(hosts_ini, deploy_yml, k3_source, k3_base, outpath):
  logger = os.path.join(k3_base, "K3-Benchstack/engines/K3/ansible/logging/driver.py")
  cmd = "python %s %s %s %s %s" % (logger, hosts_ini, deploy_yml, k3_source, k3_base)
  process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
  (output, err) = process.communicate()
  exit_code = process.wait()
  if exit_code != 0:
    print(err)

  with open(outpath, "w") as f:
    f.write(output)

def loadK3Result(sql_file):
  cmd = "psql -f %s" % sql_file
  os.system(cmd)

def loadCorrectResult(sql_file):
  cmd = "psql -f %s" % sql_file
  os.system(cmd)

def computeDiff():
  cmd = 'psql -c "select * from compute_diff"'
  process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
  (output, err) = process.communicate()
  exit_code = process.wait()
  if "0 rows" in output:
    print("SUCCESS")
    exit(0)
  else:
    print("FAILURE")
    exit(1)


if __name__ == "__main__":
  if len(sys.argv) < 6:
    print("usage: %s hosts_ini deploy_yml k3_source k3_base_dir correct_result_sql" % sys.argv[0])
    exit(1)
 
  hosts_ini = sys.argv[1]
  deploy_yml = sys.argv[2]
  k3_source = sys.argv[3]
  k3_base_dir = sys.argv[4]
  correct_result_sql = sys.argv[5]

  validateFile(hosts_ini, "hosts")
  validateFile(deploy_yml, "deployment yaml")
  validateFile(k3_source, "k3 source")
  validateFile(correct_result_sql, "correct sql result")

  runK3(hosts_ini, deploy_yml)
  runKTrace(hosts_ini, deploy_yml, k3_source, k3_base_dir, "out.sql")
  loadK3Result("out.sql")
  loadCorrectResult(correct_result_sql)
  computeDiff();
  

