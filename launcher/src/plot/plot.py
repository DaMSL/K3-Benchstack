import sys
import os
import numpy as np
import argparse

import db.db as db
import utils.utils as utils
import metric_plots as mplots
import matplotlib.pyplot as plt
import matplotlib.colors
from mpl_toolkits.axes_grid1 import host_subplot
import mpl_toolkits.axisartist as AA

systems = ['Vertica', 'Oracle', 'Spark', 'Impala']
operations = ['Planning','TableScan','Join','GroupBy','Exchange']
op_colors = ['gold', 'red', 'green', 'cyan', 'saddlebrown']
sys_colors = ['r', 'g', 'b', 'c']

workload = {'tpch10g':'tpch', 'tpch100g':'tpch', 'amplab':'amplab'}
query_labels = {'tpch': {1:'Q1', 3:'Q3', 5:'Q5', 6:'Q6', 11:'Q11', 18:'Q18', 22:'Q22'},
                'amplab': {1:'Q1', 2:'Q2', 3:'Q3'}}
query_list   = {'tpch':[1, 3, 5, 6, 11, 18, 22], 'amplab': [1,2,3]}

Oracle_comprmem = {'tpch10g':  {1:3.55, 3:5.13, 5:5.15, 6:3.55, 11:1.56, 18:5.13, 22:1.58}, 
                   'tpch100g': {1:38.19, 3:53.82, 5:53.97, 6:38.19, 11:11.87, 18:53.82, 22:15.63},
                   'amplab':   {1:5.6, 2:5.25, 3:10.85} }

p_metric= {'title':'Percent Time by Operation', 'label':'Percent Time', 'fileprefix': 'percent_'}
t_metric= {'title':'Time by Operation', 'label':'Time (sec)', 'fileprefix': 'time_'}
m_metric= {'title':'Memory Allotted by Operation', 'label':'Mem (MB)', 'fileprefix': 'memory_'}



#---------------------------------------------------------------------------------
#  getOperationStats -- Returns mem, percent, time per op as matrix of values for each system
#--------------------------------------------------------------------------------
def getOperationStats(ds, qry):  #expId_list):
  memory = [[0. for s in systems] for o in operations]
  percent_time = [[0. for s in systems] for o in operations]
  abs_time = [[0. for s in systems] for o in operations]
  total_time = {}

  # Get avg time per system based on recorded ELAPSED time for given query
  conn = db.getConnection()
  cur = conn.cursor()
  for sys in systems:
    query = "SELECT avg_time/1000 from summary_by_system WHERE dataset='%s' and query='%s' and system='%s';" % (ds, qry, sys)
    cur.execute(query)
    result = cur.fetchone()
    if result != None:
        total_time[sys] = float(result[0])

  # Get percentage & memory data for each operation for each system for given query
  query = "SELECT S.system, op_name, avg(percent_time) as percent_time, avg(memory) as memory FROM operator_stats O, trials T, summary_by_system S WHERE S.experiment_id = T.experiment_id AND S.system = T.system AND O.trial_id = T.trial_id  AND S.dataset='%s' and S.query='%s' GROUP BY S.system, op_name;" % (ds, qry)
  cur.execute(query)
  for row in cur.fetchall():
    sys, op, percent, mem = row
    i, j = (operations.index(op.strip()), systems.index(sys))
    memory[i][j] = float(mem)
    if sys == 'Oracle':
        memory[i][j] = float(mem) / 1024 / 1024
    percent_time[i][j] = float(percent)

    #Calculate absolute time per operation based on measured percent time per operation out of reported elapsed time
    abs_time[i][j] = (float(percent) / 100.0) * total_time[sys]

  return memory, percent_time, abs_time



#---------------------------------------------------------------------------------
#  plotQueryOpGraph -- wrapper call to draw  operation graphs for ALL queries for both metrics
#--------------------------------------------------------------------------------
def plotAllOperationMetrics(ds):
  qlist = query_list[workload[ds]]
  qlabel = query_labels[workload[ds]]

  time = {}
  memory = {}
  try:
    for qry in qlist:
      memory[qry], p, time[qry] = getOperationStats(ds, qry)
  except Exception as ex:
    print "Failed to process all data for dataset, %s" % ds
    print (ex)
    sys.exit(0)

  plotBigOpGraph(ds, memory, m_metric)
  plotBigOpGraph(ds, time, t_metric)


#---------------------------------------------------------------------------------
#  plotBigOpGraph -- plotting call to draw large graph for given metric & data
#--------------------------------------------------------------------------------
def plotBigOpGraph(ds, data, metric):
  qlist = query_list[workload[ds]]
  qlabel = query_labels[workload[ds]]

  inds = np.array(range(4))
  width = 1
  spacing = 3
  fig = plt.figure(figsize=(12, 4))
  offset = 0
  bars = [None] * len(operations)

  # Plot TIME graph bars
  for qry in qlist:
    if qry not in data:
        continue
    bottom = [0]*len(systems)
    bars = [None] * len(operations)
    for i, op in enumerate(operations):
      bars[i] = plt.bar(inds+offset, data[qry][i], width, color=op_colors[i], bottom=bottom)
      bottom = [sum(x) for x in zip(data[qry][i], bottom)]
    for x, y in zip (inds, bottom):
      if y == 0:
        plt.text(x + offset + width/2., 15, 'X', ha='center', va='top', size='large', color='darkred')
    offset += 4 + spacing


  plt.title("%s for all Queries, %s" % (metric['title'], ds.upper()))
  plt.ylabel(metric['label'])
  inds = np.array(range(len(qlabel)))
  xlabels = [qlabel[q] for q in sorted(qlabel.keys())]
  for x, q in enumerate(xlabels):
    plt.annotate(q, (x*7+2,-4), va='bottom', ha='center')

  syslabels = ['V', 'O', 'S', 'I', '', '', ''] * len(qlist)
  inds = np.array(range((4+spacing)*len(qlabel)))
  plt.xticks(inds+0.5, syslabels)
  plt.tick_params(axis='x', which='both', bottom='off', top='off')
  lgd = plt.legend(bars[::-1], operations[::-1], loc='center left', bbox_to_anchor=(1, 0.5))
  plt.show()
  plt.tight_layout()
  path = utils.checkDir('../web/%sgraphs' % metric['fileprefix'])
  fig.savefig(path + '/%sper_operation_%s.jpg' % (metric['fileprefix'], ds), bbox_extra_artists=(lgd,), bbox_inches='tight')
  plt.close()


#---------------------------------------------------------------------------------
#  plotAllTimes -- draws all the operation graphs for each query
#--------------------------------------------------------------------------------
def plotAllTimes(ds):
  queries = query_list[workload[ds]]
  width = 0.2
  results = {}
  conn = db.getConnection()
  cur = conn.cursor()

  fig = plt.figure(figsize=(12, 5))
  for i, sys in enumerate(systems):

    # Initialize data to 0
    for q in queries:
        results[q] = (0.,0.)

    # Get data
    query = "SELECT query, avg_time, error from summary_by_system where dataset='%s' and system='%s';" % (ds, sys)
    cur.execute(query)
    for row in cur.fetchall():
        results[int(row[0])] = (row[1], row[2])

    # Unzip date into plot-able vectors & draw each bar
    data = zip(*[ (v[0]/1000.0, v[1]/1000.0) for k, v in results.items()] ) # in cur.fetchall() ])
    index = np.arange(len(data[0]))
    plt.bar(index + width*i, data[0], width, color=sys_colors[i], yerr=data[1], label=sys)

    # Check for annotations 
    for x, y in zip (index, data[0]):
      if y == 0:
        plt.text(x + width*i + width/2., 15, 'X', ha='center', va='top', size='large', color='darkred')
      elif y < 5:
        plt.text(x + width*i + width/2., y, '%.1f' % y, size='x-small', ha='center', va='bottom')

  plt.title("%s Execution Times" % ds.upper())
  plt.xlabel("Query")
  plt.ylabel('Time (s)')
  plt.xticks(index + 2*width, queries)
  plt.legend()
  plt.tight_layout()
  plt.show()
  path = utils.checkDir("../web/time_graphs/")
  plt.savefig(path + "timegraph_%s.png" % ds)
  plt.close()
  mplots.buildIndex(path, "Consoildated Time Graphs")


#---------------------------------------------------------------------------------
#  mem -- retrieves all memory data for dataset
#--------------------------------------------------------------------------------
def mem_query (ds, sys):
  query = '''
SELECT E.query, avg(max_mem) AS memory, COALESCE(stddev(max_mem), 0) AS error
FROM trials T,
 (SELECT experiment_id, system, query FROM most_recent_by_system M WHERE M.dataset = '%s') E,
  (SELECT trial_id, sum(time) AS time, sum(percent_time) AS percent_time, sum(memory) AS sum_mem, max(memory) AS max_mem FROM operator_stats GROUP BY trial_id) O
  WHERE E.experiment_id = T.experiment_id AND T.trial_id = O.trial_id AND T.system = '%s' 
  GROUP BY E.experiment_id, E.query
  ORDER BY query::int;
''' % (ds, sys)
  return query


#---------------------------------------------------------------------------------
#  plotQueryOpGraph -- draws all the operation graphs for each query
#--------------------------------------------------------------------------------
def plotAllMemory(ds):
  queries = query_list[workload[ds]]
  width = .5
  results = {}
  oracle_memory = Oracle_comprmem[ds]
  conn = db.getConnection()
  cur = conn.cursor()

  fig = plt.figure(figsize=(12, 5))

  for i, sys in enumerate(systems):

    # Init results to 0
    for q in queries:
      results[q] = (0., 0.)

    # Get Data (handle special case for Oracle)
    query = mem_query(ds, sys)
    cur.execute(query)
    for row in cur.fetchall():
      qry, mem, err = row
      if sys == 'Oracle':
        mem = mem/1024/1024 + oracle_memory[int(qry)] * 1024
        err = err/1024/1024
      results[int(qry)] = (mem, err)

    # Unzip data, convert to GB, & draw bars
    data = zip(*[ (v[0]/1024., v[1]/1024.) for k, v in results.items()] ) # in cur.fetchall() ])
    index = np.arange(len(queries))
    plt.bar(index * (width*5) + width*i, data[0], width, color=sys_colors[i], yerr=data[1], label=sys)

    # Check for annotations
    for x, y in zip (index, data[0]):
      if y == 0:
        plt.text(x * (width*5) + width*i + width/2., 15, 'X', size='large', ha='center', va='top', color='darkred')
      elif y < 7:
        plt.text(x * (width*5) + width*i + width/2., y, '%.0fGB' % y, size='xx-small', ha='center', va='bottom')

  
  plt.title("%s Memory" % ds.upper())
  plt.xlabel("Query")
  plt.ylabel('Memory (GB)')
  plt.xticks(index * (width*5) + 2*width, queries)
  plt.legend(loc='best')
  plt.tight_layout()
  plt.show()
  path = utils.checkDir("../web/memory_graphs/")
  plt.savefig(path + "memgraph_%s.png" % ds)
  plt.close()  
  mplots.buildIndex(path, "Consoildated Memory Graphs")


#---------------------------------------------------------------------------------
#  plotExernalMetrics --  wrapper call to draw cadvisor graphs
#--------------------------------------------------------------------------------
def plotExternalMetrics(ds):
  query = "select experiment_id from most_recent where dataset='%s';" % (ds)
  conn = db.getConnection()
  cur = conn.cursor()
  cur.execute(query)
  for row in cur.fetchall():
    mplots.draw_all_metrics(row[0])



#---------------------------------------------------------------------------------
#  parseArgs --  executor call to handle input & invoke appropriate methods
#--------------------------------------------------------------------------------
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

  if args.graphs:
    print 'Plotting Consolidated Uber Graphs'
    for d in ds:
      plotAllOperationMetrics(d)
      plotAllTimes(d)
      plotAllMemory(d)

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


#---------------------------------------------------------------------------------
#  SmallOpGraph --  Prints single Query Graph of all Operations
#--------------------------------------------------------------------------------
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

#---------------------------------------------------------------------------------
#  plotQueryOpGraph -- draws all the operation graphs for each query
#--------------------------------------------------------------------------------
def plotQueryOperationsGraph(ds, qry):
  memory, percent_time, abs_time = getOperationStats(ds, qry)  #expId)

  # Plot Percent Graph
  plt.figure(1)
  path = utils.checkDir('../web/operations/percent_time_%s/' % ds)
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

  plt.close()

#---------------------------------------------------------------------------------
#  plotOps -- wrapper call to draw all smaller graphs
#--------------------------------------------------------------------------------
def plotOperations(ds):
  query = "select experiment_id from most_recent where dataset='%s';" % (ds)
  conn = db.getConnection()
  cur = conn.cursor()
  cur.execute(query)

  qlist = query_list[workload[ds]]
  for qry in qlist:
    plotQueryOperationsGraph(ds, qry)



