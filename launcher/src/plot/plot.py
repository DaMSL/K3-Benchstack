import sys
import os
import numpy as np

import db.db as db
import utils.utils as utils
import metric_plots as mplots
import matplotlib.pyplot as plt
import matplotlib.colors

# Create a bar entry from a row in the experiment_stats table
class Bar:
  def __init__(self, tup):
    (wkld, ds, qry, eid, sys, avg, err, num) = tup
    self.exp_name = "%s.%s.%s" % (wkld, ds, qry)
    self.workload = wkld
    self.dataset = ds
    self.query = qry
    self.exp_id = eid
    self.system = sys
    self.avg = avg
    self.err = err

def plotNewExperiments(conn):
  tups = db.getPlotData(conn)
  bars = [Bar(tup) for tup in tups]
  plotBarCharts(bars, conn)
  mplots.draw_all(conn)

def plotBarCharts(bars, conn):
  experiments = {}

  # Group all bars by experiment
  for bar in bars:
    if bar.exp_name not in experiments:
      experiments[bar.exp_name] = []
    experiments[bar.exp_name].append(bar)

  # One plot for each key 
  for key in experiments:
    directory = "../web/%s/%s/%s/experiment_%s/" % (experiments[key][0].workload, experiments[key][0].dataset, experiments[key][0].query, experiments[key][0].exp_id)
    utils.runCommand("mkdir -p %s" % (directory) )
    f = "%s.png" % (experiments[key][0].query)
    outfile = os.path.join(directory, f)
    plotBarChart(key, experiments[key], outfile, conn)

def plotMostRecentOps():
  conn = db.getConnection()
  query = "SELECT experiment_id, workload, dataset, query FROM most_recent;" 
  cur = conn.cursor()
  cur.execute(query)
  for row in cur.fetchall():
    expId, wkld, ds, qry = row
    plotOpBars(expId, True, "../web/%s.%s.%s.operators.absolutes.png" % (wkld, ds, qry))
    plotOpBars(expId, False, "../web/%s.%s.%s.operators.percents.png" % (wkld, ds, qry))



def plotOpBars(expId, absolutes, outfile):
  systems = ['Vertica', 'Oracle', 'Spark', 'Impala']
  inds = np.array([0, .3, .6, .9])
  width = .2
  
  conn = db.getConnection()
  query = "SELECT workload, query, dataset from experiments where experiment_id=%s" % (expId)
  cur = conn.cursor()
  cur.execute(query)
  (wkld, qry, ds) = cur.fetchone()

  fig = plt.figure()
  #plt.figure(figsize=(5,4))

  for ind, sys in zip(inds, systems):
    plotOpBar(ind, sys, width, absolutes, expId)

  ax = plt.gca()
  ax.set_xlim(0, 1.1)
 
  plt.xlabel("%s Query %s" % (wkld.upper(), qry)) 

  if not absolutes:
    ax.set_ylim(0,100)
 
  if absolutes: 
    plt.ylabel('Time (s)')
  else:
    plt.ylabel('Percent Time')
  plt.xticks(inds+width/2., systems)

  if absolutes:
    plt.title('Time by Operation')
  else:
    plt.title('Percent Time by Operation') 

  plt.show()
  plt.savefig(outfile)
  plt.close(fig)

def plotOpBar(ind, system, width, absolutes, expId):
  conn = db.getConnection()
  query = "SELECT * from operator_plots where experiment_id=%s and system='%s'" % (expId, system)
  cur = conn.cursor()
  cur.execute(query)
 
  data = [ (tup[2], tup[-1]) for tup in cur.fetchall() ]

  factor = 1 
  if absolutes:
    query = "select avg_time/1000 from experiment_stats where experiment_id =%s and system='%s';" % (expId, system)
    cur = conn.cursor()
    cur.execute(query) 
    time = float(cur.fetchone()[0])
    factor = time / 100.0

  bottom = 0
  colors = matplotlib.colors.cnames.values()
  i = 0
  for x in data: 
    val = factor * x[1]
    p = plt.bar(ind, val, width, color=colors[i], bottom=bottom)
    bottom += val
    i += 1

def plotAllTimes():
  systems = ['Vertica', 'Oracle', 'Spark', 'Impala']
  colors = ['r', 'g', 'b', 'c']
  wkld = 'tpch'
  queries = [1, 3, 5, 6, 11, 18, 22]
  width = 0.2 
  
  for sys in range(len(systems)):
    conn = db.getConnection()
    query = "SELECT avg_time, error from summary where system='%s' order by query" % systems[sys]
    cur = conn.cursor()
    cur.execute(query)
    data = zip(*[ (tup[0]/1000, tup[1]/1000) for tup in cur.fetchall() ])
    index = np.arange(len(data[0]))
    p = plt.bar(index + width*sys, data[0], width, color=colors[sys], yerr=data[1], label=systems[sys])

  ax = plt.gca()
  plt.title("%s Execution Times" % wkld.upper()) 
  plt.xlabel("Query") 
  plt.ylabel('Time (s)')
  plt.xticks(index + 2*width, queries)
  plt.legend()
  plt.tight_layout()
  plt.show()
  plt.savefig("../web/times.png")

if __name__ == "__main__":
  #plotMostRecentOps()
  plotAllTimes()
