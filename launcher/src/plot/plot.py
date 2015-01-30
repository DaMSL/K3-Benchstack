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


def getExpInfo(expId_list):
  conn = db.getConnection()
  cur = conn.cursor()
  last = None
  for expId in expId_list:
    query = "SELECT workload, query, dataset from experiments where experiment_id=%s" % (expId)
    cur.execute(query)
    result = cur.fetchone()
    if last != None and result != last:
        print "ERROR! Cannot plot systems ran on different datasets/queries: %s vs %s" % (last, result)
        sys.exit(1)
    last = result
  return last


def plotSmallOpGraph(metric, vals, filename, percent=False):
  print "Drawing %s to %s" %(metric['title'], filename)
  inds = np.array(range(4))
  width = 0.6
  bottom = [0]*len(systems)
  bars = [None] * len(operations)

  for i, op in enumerate(operations):
#    print "plotting:  %d - " % i + str(op) + ": " + str(vals[i])
    bars[i] = plt.bar(inds, vals[i], width, color=op_colors[i], bottom=bottom)
    bottom = [sum(x) for x in zip(vals[i], bottom)]
  plt.title(filename)
  plt.ylabel(metric['label'])
  plt.xticks(inds+width/2., systems)
  if percent:
    ax = plt.gca()
    ax.set_ylim(0,100)
  lgd = plt.legend(bars[::-1], operations[::-1], loc='center left', bbox_to_anchor=(1, 0.5))
  plt.savefig(filename + ".jpg", bbox_extra_artists=(lgd,), bbox_inches='tight')
  plt.close()

def getOperationStats(ds, qry):  #expId_list):
  memory = [[0. for s in systems] for o in operations]
  percent_time = [[0. for s in systems] for o in operations]
  abs_time = [[0. for s in systems] for o in operations]
  total_time = {}

  conn = db.getConnection()
  cur = conn.cursor()
  for sys in systems:
#      query = "select avg_time/1000 from experiment_stats where experiment_id =%s and system='%s';" % (expId, sys)
    query = "SELECT avg_time/1000 from summary_by_system WHERE dataset='%s' and query='%s' and system='%s';" % (ds, qry, sys)
    cur.execute(query)
    result = cur.fetchone()
    if result != None:
        total_time[sys] = float(result[0])
#  print "\nSTATS for: %s, %s" % (ds, qry)
#  print total_time;


#  query = "SELECT system, op_name, avg(percent_time) as percent_time, avg(memory) as memory from operator_stats natural join trials where experiment_id=%s group by system, op_name" % (expId)
  query = "SELECT S.system, op_name, avg(percent_time) as percent_time, avg(memory) as memory FROM operator_stats O, trials T, summary_by_system S WHERE S.experiment_id = T.experiment_id AND S.system = T.system AND O.trial_id = T.trial_id  AND S.dataset='%s' and S.query='%s' GROUP BY S.system, op_name;" % (ds, qry)

  cur.execute(query)
  for row in cur.fetchall():
#    print row
    sys, op, percent, mem = row
    i, j = (operations.index(op.strip()), systems.index(sys))
    memory[i][j] = float(mem)
    if sys == 'Oracle':
        memory[i][j] = float(mem) / 1024 / 1024
    percent_time[i][j] = float(percent)
    abs_time[i][j] = (float(percent) / 100.0) * total_time[sys]

#  for i in range(5):
#      for j in range(4):
#          print operations[i] + ", " + systems[j] + " - " + str(memory[i][j])

  return memory, percent_time, abs_time


#def plotExpOps(expId):
def plotQueryOperationsGraph(ds, qry):
  p_metric= {'title':'Percent Time by Operation', 'label':'Percent Time'}
  t_metric= {'title':'Time by Operation', 'label':'Time (sec)'}
  m_metric= {'title':'Memory Allotted by Operation', 'label':'Mem (MB)'}

#  wkld, qry, ds = getExpInfo(expId)
  memory, percent_time, abs_time = getOperationStats(ds, qry)  #expId)

  # Plot Percent Graph
  plt.figure(1)
  path = utils.checkDir('../web/operations/percent_time_%s/' % ds)
#  print memory
  plotSmallOpGraph(p_metric, percent_time, path + 'percent_q%s' % qry, percent=True)
  mplots.buildIndex(path, "Percent Time per Operation - %s" % (ds.upper()))

  # Plot Memory Graph
  plt.figure(2)
  path = utils.checkDir('../web/operations/memory_%s/' % ds)
  plotSmallOpGraph(m_metric, memory, path + 'memory_q%s' % qry)
  mplots.buildIndex(path, "Memory per Operation - %s" % (ds.upper()))

  # Plot Time Graph
  plt.figure(3)
  path = utils.checkDir('../web/operations/absolute_time_%s/' % ds)
  plotSmallOpGraph(t_metric, abs_time, path + 'time_q%s' % qry)
  mplots.buildIndex(path, "Absolute Time Per Operation - %s" % (ds.upper()))

  # path = '../web/%s_time_per_operation' % ds
  # utils.runCommand("mkdir -p %s" % (path) )
  # plt.title('QUERY %s' % qry)
  # plt.savefig(path + '/query_%s.jpg' % qry, bbox_extra_artists=(lgd,), bbox_inches='tight')

  plt.close()


def plotAllOperationMetrics(ds):
  
  
  # TODO:  UPDATE ALL OPS GRAPH

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
  width = 1
  spacing = 3
  fig = plt.figure(figsize=(12, 4))
  offset = 0
  bars = [None] * len(operations)
  for qry in qlist:
    if qry not in time:
        continue
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
  path = utils.checkDir('../web/graphs')
  fig.savefig(path + '/time_per_operation_%s.jpg' % ds, bbox_extra_artists=(lgd,), bbox_inches='tight')

def plotAllTimes(ds):
  systems = ['Vertica', 'Oracle', 'Spark', 'Impala']
  #systems = ['Vertica', 'Oracle', 'Impala']
  colors = ['r', 'g', 'b', 'c']
  wkld = 'tpch'
  queries = Amplab_qlist if ds == 'amplab' else TPCH_qlist
  width = 0.2

  for sys in range(len(systems)):
    conn = db.getConnection()
    query = "SELECT avg_time, error from summary_by_system where dataset='%s' and system='%s' order by query::int;" % (ds, systems[sys])
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
  path = utils.checkDir("../web/graphs/")
  plt.savefig(path + "timegraph_%s.png" % ds)



def plotOperations(ds):
  query = "select experiment_id from most_recent where dataset='%s';" % (ds)
  conn = db.getConnection()
  cur = conn.cursor()
  cur.execute(query)

  qlist = Amplab_qlist if ds == 'amplab' else TPCH_qlist
  for qry in qlist:
    plotQueryOperationsGraph(ds, qry)

#      row in cur.fetchall():
#    plotExpOps(row[0])

def plotExternalMetrics(ds):
  query = "select experiment_id from most_recent where dataset='%s';" % (ds)
  conn = db.getConnection()
  cur = conn.cursor()
  cur.execute(query)
  for row in cur.fetchall():
    mplots.draw_all_metrics(row[0])


def parseArgs():
  parser = argparse.ArgumentParser()
  parser.add_argument('-d', '--dataset', nargs='+', help='Plot specific dataset (amplab, tpch10g, tpch100g)', required=False)
  parser.add_argument('-e', '--experiment', help='Plot specific experiment', required=False)
  parser.add_argument('-g', '--graphs', help='Plot consolidate bar graph of all system\'s results for given dataset', action='store_true')
  parser.add_argument('-c', '--cadvisor', help='Plot individual line graphs of externally collected cadvisor metrics', action='store_true')
  parser.add_argument('-o', '--operations', help='Plot bar graphs of per-operation metrics ', action='store_true')
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

  if args.graphs:
    print 'Plotting Consolidated Uber Graphs'
    for d in ds:
      plotAllTimes(d)
#      plotAllOperationMetrics(d)
      #plotAllMemory(d)

  if args.experiment:
    print 'Plotting graphs for experiment #%s' % args.experiment
    mplots.plot_experiment_metrics(args.experiment)
    plotExpOps(args.experiment)

    return

  if args.cadvisor:
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
