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


def plotExpOps(expId):
  systems = ['Vertica', 'Oracle', 'Spark', 'Impala']
  operations = ['PreExec','TableScan','ExprEval','Join','PipelinedJoin','GroupBy','PipelinedGroupBy','NetIO']
  op_colors = ['gold', 'red', 'purple', 'greenyellow', 'darkgreen', 'cyan', 'blue', 'saddlebrown']
  memory = [[0 for s in systems] for o in operations]
  percent_time = [[0 for s in systems] for o in operations]
  abs_time = [[0 for s in systems] for o in operations]
  total_time = {}
  inds = np.array(range(4))
  width = 0.6
  

  conn = db.getConnection()
  query = "SELECT workload, query, dataset from experiments where experiment_id=%s" % (expId)
  cur = conn.cursor()
  cur.execute(query)
  (wkld, qry, ds) = cur.fetchone()

  path = '../web/operator_metrics/experiment_%s' % (expId)
  utils.runCommand("mkdir -p %s" % (path))

  for sys in systems:
    query = "select avg_time/1000 from experiment_stats where experiment_id =%s and system='%s';" % (expId, sys)
    cur.execute(query) 
    total_time[sys] = float(cur.fetchone()[0])
    
  query = "SELECT system, op_name, percent_time, memory from operator_stats where experiment_id=%s" % (expId)
  cur.execute(query)
  for row in cur.fetchall():
    (sys, op, percent, mem) = row
    i, j = (operations.index(op), systems.index(sys))
    percent_time[i][j] = percent
    memory[i][j] = mem
    abs_time[i][j] = (percent / 100.0) * total_time[sys]
  ax = plt.gca()

  bars = [None] * len(operations)

  # Plot Percent Graph
  plt.figure(1)
  bottom = [0]*len(systems)
  for i, op in enumerate(operations): 
    bars[i] = plt.bar(inds, percent_time[i], width, color=op_colors[i], bottom=bottom)
    bottom = [sum(x) for x in zip(percent_time[i], bottom)]
  plt.title('Percent Time by Operation') 
  plt.ylabel('Percent Time')
  plt.xticks(inds+width/2., systems)
  ax.set_ylim(0,100)
  lgd = plt.legend(bars[::-1], operations[::-1], loc='center left', bbox_to_anchor=(1, 0.5))
  plt.show()
  plt.savefig(path + '/percent.jpg', bbox_extra_artists=(lgd,), bbox_inches='tight')

  # Plot Memory Graph
  plt.figure(2)
  bottom = [0]*len(systems)
  for i, op in enumerate(operations): 
    plt.bar(inds, memory[i], width, color=op_colors[i], bottom=bottom)
    bottom = [sum(x) for x in zip(memory[i], bottom)]
  plt.title('Memory Allotted by Operation') 
  plt.ylabel('Mem (MB)')
  plt.xticks(inds+width/2., systems)
  lgd = plt.legend(bars[::-1], operations[::-1], loc='center left', bbox_to_anchor=(1, 0.5))
  plt.show()
  plt.savefig(path + '/memory.jpg', bbox_extra_artists=(lgd,), bbox_inches='tight')

  # Plot Time Graph
  plt.figure(3)
  bottom = [0]*len(systems)
  for i, op in enumerate(operations): 
    plt.bar(inds, abs_time[i], width, color=op_colors[i], bottom=bottom)
    bottom = [sum(x) for x in zip(abs_time[i], bottom)]
  plt.title('Time by Operation') 
  plt.ylabel('Time (s)')
  #ax.set_xlim(0, width*5.5)
  plt.xticks(inds+width/2., systems)
  lgd = plt.legend(bars[::-1], operations[::-1], loc='center left', bbox_to_anchor=(1, 0.5))
  plt.show()
  plt.savefig(path + '/time.jpg', bbox_extra_artists=(lgd,), bbox_inches='tight')

  plt.close()
  buildIndex(path, ds.upper(), expId, qry)


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
    query = "SELECT avg_time, error from summary where system='%s' order by query::int" % systems[sys]
    cur = conn.cursor()
    cur.execute(query)
    data = zip(*[ (tup[0]/1000.0, tup[1]/1000.0) for tup in cur.fetchall() ])
    index = np.arange(len(data[0]))
    plt.bar(index + width*sys, data[0], width, color=colors[sys], yerr=data[1], label=systems[sys])
    for x, y in zip (index, data[0]):
      if y < 5:
        plt.text(x + width*sys + width/2., y, '%.1f' % y, size='x-small', ha='center', va='bottom')

    

  ax = plt.gca()
  plt.title("%s Execution Times" % wkld.upper()) 
  plt.xlabel("Query") 
  plt.ylabel('Time (s)')
  plt.xticks(index + 2*width, queries)
  plt.legend()
  plt.tight_layout()
  plt.show()
  plt.savefig("../web/times.png")


def buildIndex(path, wkld, exp_id, qry):
  title = 'Workload: %s, Experiment: %s,  Query: %s</title>' % (wkld, exp_id, qry)
  index =  '<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 3.2 Final//EN"><html>'
  index += '<title>%s</title>' % title
  index += '<body><h2>%s</h2>' % title
  for img in sorted(os.listdir(path))[::-1]:
    index += '<img src="%s" />' % img
  index += '</body></html>'
  indexfile = open(path + '/index.html', 'w')
  indexfile.write(index)
  indexfile.close()



if __name__ == "__main__":
  #plotMostRecentOps()
  #plotAllTimes()
  for x in [15, 16, 33, 34, 37, 38, 39]:
    plotExpOps(x)
