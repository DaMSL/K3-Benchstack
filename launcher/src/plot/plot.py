import sys
import os
import numpy as np
import argparse

import db.db as db
import utils.utils as utils
import metric_plots as mplots
import matplotlib.pyplot as plt
import matplotlib.colors


systems = ['Vertica', 'Oracle', 'Spark', 'Impala']
operations = ['PreExec','TableScan','Join','GroupBy','Exchange']
op_colors = ['gold', 'red', 'green', 'cyan', 'saddlebrown']
TPCH_qlabels = {1:'Q1', 3:'Q3', 5:'Q5', 6:'Q6', 11:'Q11', 18:'Q18', 22:'Q22'}
TPCH_qlist = [1, 3, 5, 6, 11, 18, 22]
Amplab_qlabels = {1:'Q1', 2:'Q2', 3:'Q3'}
Amplab_qlist = [1, 2, 3]


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


def getExpInfo(expId):
  conn = db.getConnection()
  query = "SELECT workload, query, dataset from experiments where experiment_id=%s" % (expId)
  cur = conn.cursor()
  cur.execute(query)
  return cur.fetchone()


def plotSmallOpGraph(metric, vals, filename, percent=False):
  inds = np.array(range(4))
  width = 0.6
  bottom = [0]*len(systems)
  bars = [None] * len(operations)
  for i, op in enumerate(operations):
    bars[i] = plt.bar(inds, vals[i], width, color=op_colors[i], bottom=bottom)
    bottom = [sum(x) for x in zip(vals[i], bottom)]
  plt.title(metric['title'])
  plt.ylabel(metric['label'])
  plt.xticks(inds+width/2., systems)
  if percent:
    ax = plt.gca()
    ax.set_ylim(0,100)
  lgd = plt.legend(bars[::-1], operations[::-1], loc='center left', bbox_to_anchor=(1, 0.5))
  plt.show()
  plt.savefig(filename, bbox_extra_artists=(lgd,), bbox_inches='tight')
  return lgd

def getOperationStats(expId):
  memory = [[0 for s in systems] for o in operations]
  percent_time = [[0 for s in systems] for o in operations]
  abs_time = [[0 for s in systems] for o in operations]
  total_time = {}

  conn = db.getConnection()
  cur = conn.cursor()
  for sys in systems:
    query = "select avg_time/1000 from experiment_stats where experiment_id =%s and system='%s';" % (expId, sys)
    cur.execute(query)
    result = cur.fetchone()
    total_time[sys] = 0. if result == None else float(result[0])

  query = "SELECT system, op_name, avg(percent_time) as percent_time, avg(memory) as memory from operator_stats natural join trials where experiment_id=%s group by system, op_name" % (expId)
  cur.execute(query)
  for row in cur.fetchall():
    (sys, op, percent, mem) = row
    i, j = (operations.index(op), systems.index(sys))
    percent_time[i][j] = percent
    memory[i][j] = mem
    abs_time[i][j] = (percent / 100.0) * total_time[sys]

  return memory, percent_time, abs_time


def plotExpOps(expId):
  p_metric= {'title':'Percent Time by Operation', 'label':'Percent Time'}
  t_metric= {'title':'Time by Operation', 'label':'Time (ms)'}
  m_metric= {'title':'Memory Allotted by Operation', 'label':'Mem (MB)'}

  (wkld, qry, ds) = getExpInfo(expId)
  path = '../web/experiments/experiment_%s' % (expId)
  utils.runCommand("mkdir -p %s" % (path))

  memory, percent_time, abs_time = getOperationStats(expId)

  # Plot Percent Graph
  plt.figure(1)
  path = '../web/%s_OPNS_percent_time/' % ds
  utils.runCommand("mkdir -p %s" % (path) )
  plotSmallOpGraph(p_metric, percent_time, '/percent_q%s.jpg' % qry, percent=True)
  mplots.buildIndex(path, "Percent Time per Operation - %s" % (ds.upper()))

  # Plot Memory Graph
  plt.figure(2)
  path = '../web/%s_OPNS_memory/' % ds
  utils.runCommand("mkdir -p %s" % (path) )
  plotSmallOpGraph(m_metric, memory, path + '/memory_q%s.jpg' % qry)
  mplots.buildIndex(path, "Absolute Time per Operation - %s" % (ds.upper()))

  # Plot Time Graph
  plt.figure(3)
  path = '../web/%s_OPNS_abs_time/' % ds
  utils.runCommand("mkdir -p %s" % (path) )
  lgd = plotSmallOpGraph(t_metric, abs_time, path + '/time_q%s.jpg' % qry)
  mplots.buildIndex(path, "Experiment #%d: %s" % (expId, ds.upper()))

  # path = '../web/%s_time_per_operation' % ds
  # utils.runCommand("mkdir -p %s" % (path) )
  # plt.title('QUERY %s' % qry)
  # plt.savefig(path + '/query_%s.jpg' % qry, bbox_extra_artists=(lgd,), bbox_inches='tight')

  plt.close()
  title = "Time per Operation - %s" % (ds.upper())
  mplots.buildIndex(path, title)


def plotAllQueries(ds):
  conn = db.getConnection()
  cur = conn.cursor()
  query = "SELECT experiment_id, query FROM summary WHERE dataset='%s' ORDER BY query::int" % ds

  qlist = []
  qlabel = {}
  if ds in ['tpch10g', 'tpch100g']:
    qlist = TPCH_qlist
    qlabel = TPCH_qlabels
  else:
    print "Dataset, %s, not supported" % ds

  time = {}
  try:
    cur.execute(query)
    for row in cur.fetchall():
      m, p, time[row[1]] = getOperationStats(row[0])
  except Exception as ex:
    print "Failed to process all data for dataset, %s" % ds
    print (ex)
    sys.exit(0)

  inds = np.array(range(4))
  print "INDLIST: " + str(inds)
  width = 1
  spacing = 3

  fig = plt.figure(figsize=(12, 4))
  offset = 0
  bars = [None] * len(operations)
  for qry in qlist:
    print time[str(qry)]
    bottom = [0]*len(systems)
#    bars = [None] * len(operations)
    for i, op in enumerate(operations):
      bars[i] = plt.bar(inds+offset, time[str(qry)][i], width, color=op_colors[i], bottom=bottom)
      bottom = [sum(x) for x in zip(time[str(qry)][i], bottom)]
    offset += 4 + spacing

  plt.title("Operation Metrics for all Queries, %s" % ds.upper())
  plt.ylabel("Time (sec)")

  inds = np.array(range(len(qlabel)))
  xlabels = [qlabel[q] for q in sorted(qlabel.keys())]
  for x, q in enumerate(xlabels):
    plt.annotate(q, (x*7+2,-3), va='bottom', ha='center')
#  plt.xticks(inds*7+2.5, xlabels)
  syslabels = ['V', 'O', 'S', 'I', '', '', ''] * len(qlist)
  inds = np.array(range((4+spacing)*len(qlabel)))
  plt.xticks(inds+0.5, syslabels)

  plt.tick_params(axis='x', which='both', bottom='off', top='off')

  lgd = plt.legend(bars[::-1], operations[::-1], loc='center left', bbox_to_anchor=(1, 0.5))
  plt.show()
  plt.tight_layout()
  fig.savefig('../web/%s_opgraph.jpg' % ds, bbox_extra_artists=(lgd,), bbox_inches='tight')

def plotAllTimes(ds):
  systems = ['Vertica', 'Oracle', 'Spark', 'Impala']
  #systems = ['Vertica', 'Oracle', 'Impala']
  colors = ['r', 'g', 'b', 'c']
  wkld = 'tpch'
  queries = Amplab_qlist if ds == 'amplab' else TPCH_qlist
  width = 0.2

  for sys in range(len(systems)):
    conn = db.getConnection()

    query = "SELECT avg_time, error from summary where dataset='%s' and system='%s' order by query::int" % (ds, systems[sys])
    cur = conn.cursor()
    cur.execute(query)
    data = zip(*[ (tup[0]/1000.0, tup[1]/1000.0) for tup in cur.fetchall() ])
    if len(data) != 2:
      continue
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
  plt.savefig("../web/%s_timegraph.png" % ds)




def plotOperations(ds):
  query = "select experiment_id from most_recent where dataset='%s';" % (ds)
  conn = db.getConnection()
  cur = conn.cursor()
  cur.execute(query)
  for row in cur.fetchall():
    plotExpOps(row[0])

def plotExternalMetrics(ds):
  query = "select experiment_id from most_recent where dataset='%s';" % (ds)
  conn = db.getConnection()
  cur = conn.cursor()
  cur.execute(query)
  for row in cur.fetchall():
    mplots.draw_all_metrics(row[0])


def parseArgs():
  parser = argparse.ArgumentParser()
  parser.add_argument('-d', '--dataset', nargs='+', help='Plot specific dataset', required=False)
  parser.add_argument('-e', '--experiment', help='Plot specific experiment', required=False)
  parser.add_argument('-t', '--timegraph', help='Plot bar graph for all times for all queries', action='store_true')
  parser.add_argument('-c', '--consolidated', help='Plot bar graph of time per operation consolidated for all queries', action='store_true')
  parser.add_argument('-m', '--metrics', help='Plot individual line graphs of externally collected cadvisor metrics', action='store_true')
  parser.add_argument('-o', '--operations', help='Plot bar graphs of per-operation metrics for each query ', action='store_true')
  args = parser.parse_args()

  if args.dataset and args.experiment:
    print "Choose EITHER   --experiment   OR  --dataset"

  ds = ['tpch10g', 'tpch100g', 'amplab']
  if args.dataset:
    ds = []
    for d in args.dataset:
      ds.append(d)
  for d in ds:
    print d

  if args.timegraph:
    print 'Plotting All Times'
    for d in ds:
      plotAllTimes(d)

  if args.consolidated:
    print 'Plotting Consolidated operation metrics graphs for all Queries'
    for d in ds:
      plotAllQueries(d)


  if args.experiment:
    print 'Plotting graphs for experiment #%s' % args.experiment
    if args.metrics:
      mplots.plot_experiment_metrics(args.experiment)

    if args.operations:
      plotExpOps(args.experiment)

    return

  if args.metrics:
    for d in ds:
      print "Plot cadvisor metrics for %s"  % d
      mplots.plot_dset_metrics(d)
      #plotExternalMetrics(d)

  if args.operations:
    print "Plot Operator metrics"
    for d in ds:
      plotOperations(d)




if __name__ == "__main__":
  parseArgs()
