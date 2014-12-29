import subprocess
import os
import datetime

class Trial:
  def __init__(self, system, query, dataset, elapsed, trial, ts):
    self.system = system
    self.query = query
    self.dataset = dataset
    self.elapsed = elapsed
    self.trial = trial
    self.ts = ts

def runVertica(args):
  args = {}
  args['query_sql_dir'] = "../workloads/tpch/common/sql/queries"
  args['query_list_file'] = "../workloads/tpch/vertica/queries.txt"
  args['result_dir'] = "./vertica_results/"
  args['driver'] = "../workloads/vertica_driver.sh"
  args['schema'] = "tpch10g"
  args['trials'] = 1

  query_list = []
  with open(args['query_list_file'], "r") as f:
    # remove .sql extension and newline
    query_list = [ l[:-5] for l in f.readlines() ]

  command = "%s %s %s %s %s" % (args['driver'], args['query_sql_dir'], args['query_list_file'], args['schema'], args['trials'])
  subprocess.check_call(command, shell=True)

  trials = []
  ts = datetime.datetime.now()
  for q in query_list:
    path = os.path.join(args['result_dir'], q + ".sql")
    with open(path, "r") as f:
      i = 1
      for line in f.readlines():
        elapsed = line.split(" ")[-2]
        print(elapsed)
        trial = Trial("Vertica", q, args['schema'], elapsed, i, ts)
        print(trial)
        trials.append(trial)
        i = i + 1

  return trials 

if __name__ == "__main__":
  runVertica({})
  
