import sys
import os
import numpy as np
import argparse

import db.db as db
import utils.utils as utils
import metric_plots as mplots
import matplotlib.pyplot as plt
import matplotlib.colors
from matplotlib.ticker import FuncFormatter
from benchdata import *
import metric

FIGURE_SIZE_PAPER = (12, 3.5)
FIGURE_SIZE_LG  = (12,4)
FIGURE_SIZE     = (8, 4)

DEFAULT_FIGURE_SIZE = FIGURE_SIZE_PAPER

VAL_LABEL_SIZE  = 'medium'

WEB_DIR = '/web/graphs/'
#systems         = ['Vertica', 'Oracle', 'Spark', 'Impala', 'K3']
systems         = ['Spark', 'Impala', 'K3']
systems_mem     = ['Spark', 'Impala', 'K3-NoFusion', 'K3-Fusion']

system_labels   = {'Vertica': 'DB X', 'Oracle':'DB Y', 'Spark': 'Spark', 
        'Impala': 'Impala', 'K3': 'K3', 'Impala-p': 'Impala (P)', 'K3-Fusion':'K3-Fusion', 'K3-NoFusion':'K3-NoFusion'}

sys_colors      = {'Vertica':'lightcoral', 'Oracle':'goldenrod', 'Spark':'orangered', 'Impala':'yellowgreen', 
                    'Impala-p':'cyan', 'K3':'cornflowerblue',  'K3-Fusion':'gold', 'K3-NoFusion':'cornflowerblue'}
sys_greyscale   = {'Vertica':'dimgrey', 'Oracle':'darkgrey', 'Spark':'whitesmoke', 'Impala':'darkgrey', 
        'Impala-p':'silver', 'K3':'black', 'K3-Fusion':'silver', 'K3-NoFusion':'black'}
sys_pattern     = {'Vertica':'', 'Oracle':'', 'Spark':'', 'Impala':'', 'Impala-p':'--', 'K3':'', 'K3-Fusion':'--', 'K3-NoFusion':''}

operations      = ['Planning','TableScan','Join','GroupBy','Exchange', 'FilterProject']
op_colors       = ['saddlebrown', 'firebrick', 'goldenrod', 'seagreen', 'slateblue', 'midnightblue']
op_greyscale    = {'Planning':'black','TableScan':'whitesmoke','Join':'dimgrey','GroupBy':'silver','Exchange':'darkgrey', 'FilterProject':'gainsboro'}
op_pattern      = {'Planning':'','TableScan':'','Join':'','GroupBy':'','Exchange':'xxx', 'FilterProject':'|||'}

all_datasets        = ['tpch10g', 'tpch100g', 'amplab', 'twitter', 'sgd10g', 'sgd100g']
workload        = {'tpch10g':'tpch', 'tpch100g':'tpch', 'amplab':'amplab', 'sgd10g':'ml', 'sgd100g':'ml', 'sgd':'ml', 'twitter': 'graph'}

#query_labels    = {'tpch':   {1:'Q1', 3:'Q3', 5:'Q5', 6:'Q6', 11:'Q11', 18:'Q18', 22:'Q22'},
#                   'amplab': {1:'Q1', 2:'Q2', 3:'Q3'}}

query_labels    = {'tpch':   ['Q1', 'Q3', 'Q5', 'Q6', 'Q18', 'Q22'],
                   'amplab': ['Q1', 'Q2', 'Q3'],
                   'ml': ['KM', 'SGD'],
                   'graph': ['PR']}

query_list      = {'tpch':['1', '3', '5', '6', '18', '22'], 'amplab': ['1','2','3'], 'graph': ['pagerank'], 'ml': ['k_means','sgd']}


manual_ymax     = {'tpch10g': 40, 'tpch100g': 250, 'amplab': 100, 'time':100, 'memory': 40}

cpu_total = metric.Metric(label="cpu_usage_total", convert=(lambda t: t/1000000.), title="CPU_Total", axis="Time (ms)", delta=True)
#cpu_system = Metric(label="cpu_usage_system", convert=conv_cpu, title="CPU_System", axis="Time (ms)", delta=True)
mem_usage = metric.Metric(label="memory_usage", convert=(lambda m: m / (1024. * 1024. * 1024)), title="MEM_Usage", axis="Mem (GB)")

timeM = metric.Metric(title='Execution Time', convert=(lambda t: t/1000.), label='time', axis='Time (sec)', query=time_query, systems=systems)
memoryM = metric.Metric(title='Peak Memory', convert=(lambda m: m/1024.), label='memory', axis='Mem (GB)', query=mem_query, systems=systems_mem)
ipcM = metric.Metric(title='IPC', convert=(lambda m: m), label='ipc', axis='IPC', query=ipc_query, systems=['K3-NoFusion', 'K3-Fusion'])
cachel2M = metric.Metric(title='L2 Cache', convert=(lambda m: m), label='L2-Cache', axis='L2 Cache', query=cache_l2_query, systems=['K3-NoFusion', 'K3-Fusion'])
cachel3M = metric.Metric(title='L3 Cache', convert=(lambda m: m), label='L3-Cache', axis='L3 Cache', query=cache_l3_query, systems=['K3-NoFusion', 'K3-Fusion'])

p_metric= {'title':'Percent Time by Operation', 'label':'Percent Time', 'fileprefix': 'percent_', 'type': 'percent'}
t_metric= {'title':'Time by Operation', 'label':'Time (sec)', 'fileprefix': 'time_', 'type': 'time'}
m_metric= {'title':'Peak Memory Allocated by Operation', 'label':'Mem (GB)', 'fileprefix': 'memory_', 'type': 'mem'}

def normalizeData(data, norm_from=0, norm_to=100):
    to = np.linspace(norm_from, norm_to, len(data))
    tn = np.linspace(norm_from, norm_to, norm_to)
    dn = np.interp(tn, to, data)
    return dn

#---------------------------------------------------------------------------------
#  plotAlldataSets -- draws all the operation graphs for each query
#--------------------------------------------------------------------------------
def plotLargeBarGraph(metric, **kwargs):

  isColor   = kwargs.get('isColor', True)
  norm      = kwargs.get('norm', False)
  logscale  = kwargs.get('logscale', False)
  datasets  = kwargs.get('datasets', all_datasets)
  fileprefix = kwargs.get('fileprefix', '')
  systems   = kwargs.get('systems', None)
  excludeWorkloads = kwargs.get('excludeWorkloads', [])
  print "Drawing %s bar graph for following datasets: " % (metric.title), datasets

  xlabels = [] 
  datasets = [d for d in datasets if workload[d] not in excludeWorkloads]
  for ds in datasets:
      xlabels.extend(query_labels[workload[ds]])

  if systems == None:
    systems = metric.systems #systems_mem  if metric.label == 'memory' else ['Spark', 'Impala', 'K3'] 

  num_queries = len(xlabels)

  width = 1./(len(systems) + 2)
  spacing = width*2
  offset = width
  conn = db.getConnection()
  cur = conn.cursor()

  index = np.arange(num_queries)
  fig = plt.figure(figsize=DEFAULT_FIGURE_SIZE)

  vals = {}
  errs = {}

  for i, sys in enumerate(systems):
    results = []

    for ds in datasets:
      queries = query_list[workload[ds]]
      ds_results = {}

      # Get data
      query = metric.query(ds, sys)
      cur.execute(query)
      for row in cur.fetchall():
        qry, val, err = row
        if qry not in queries:
            continue
        ds_results[qry] = (val, err)

      for q in queries:
        if str(q) not in ds_results:
          ds_results[str(q)] = (0, 0)
      
      results.extend([ds_results[str(q)] for q in queries])

    vals[sys] = [ metric.convert(v[0]) for v in results]
    errs[sys] = [ metric.convert(v[1]) for v in results]
    print sys, vals[sys]

  # Check if data needs to be normalized  TODO: move to function
  if norm:
    for i in range(num_queries):
        maxval = float(max([vals[sys][i] for sys in systems]))
        for sys in systems:
            if maxval != 0:
              vals[sys][i] /= maxval

  
  for i, sys in enumerate(systems):
    pattern = None if isColor else sys_pattern[sys]
    barcolor = sys_colors[sys] if isColor else sys_greyscale[sys]
    plt.bar((len(systems)*width + spacing)*index + offset + (width)*i, vals[sys], width, color=barcolor, label=system_labels[sys], hatch=pattern)

  ax = plt.gca()
  ax.xaxis.set_major_locator(plt.MultipleLocator(1.0 - width/2.))
  ax.xaxis.set_minor_locator(plt.MultipleLocator(width))
  ax.get_xaxis().set_tick_params(pad=2)
  plt.xlim(xmax=num_queries)

  plt.ylabel(metric.axis)
  plt.grid(which='major', axis='y', linewidth=0.75, linestyle='--', color='0.75')
  if logscale:
    ax.set_yscale('log')
    plt.yscale('log', nonposy='clip')
  else:

    if metric.label in manual_ymax or norm:
      ymax = 1.0 if norm else manual_ymax[metric.label]
      plt.ylim(ymin=0, ymax=ymax)

  ax.xaxis.set_minor_locator(plt.MultipleLocator(width*len(systems)+spacing))
  plt.grid(which='minor', axis='x', linewidth=0.75, linestyle='-', color='0.75')
  plt.xticks(index + .5, xlabels, va='top')
  plt.tick_params(axis='x', which='both', top='off', bottom='off')

  plt.gca().set_axisbelow(True)
  plt.legend(loc='upper left', fontsize='medium')
  plt.tight_layout()
  path = utils.checkDir(WEB_DIR + '%s_graphs/' % metric.label)
#  path = utils.checkDir("../web/%s_graphs/" % metric.label)

  flabel = "%sgraph" % fileprefix
  if norm:
      flabel += "_norm"
  if logscale:
      flabel += "_log"
  filename = path + "%s_%s.jpg" % (flabel, metric.label)
  plt.savefig(filename)
  plt.close()
  utils.buildIndex(path, "Consoildated Graphs, %s" % metric.axis)
  print ' Saved to %s' % filename


#---------------------------------------------------------------------------------
#  plotBigOpGraph -- plotting call to draw large graph for given metric & data
#--------------------------------------------------------------------------------
def plotStackedOperationGraph(metric, **kwargs):

  isColor   = kwargs.get('isColor', True)
  datasets  = kwargs.get('datasets', all_datasets)
  error     = kwargs.get('error', None)
  fileprefix = kwargs.get('fileprefix', '')


  print "Drawing %s stack per-operations graph datasets: " % (metric.title), datasets

  xlabels = []
  for ds in datasets:
      xlabels.extend(query_labels[workload[ds]])

  systems = systems_mem  if metric.label == 'memory' else ['Spark', 'Impala', 'K3'] 

  num_queries = len(xlabels)

  syslabels = ['S', 'I', 'K3', ''] * num_queries  
  if metric.label == 'memory':
      syslabels = ['S', 'I', 'K3-F', 'K3-NF']

  inds = np.array(range(len(systems)))
  width = 1
  spacing = 1
  fig = plt.figure(figsize=DEFAULT_FIGURE_SIZE)
  offset = spacing / 2.
  time = {}
  memory = {}
  error = {}

  for ds in datasets:
    qlist = query_list[workload[ds]]
    try:
      for qry in qlist:
        memory[(ds,qry)], p, time[(ds,qry)], error[(ds,qry)] = getOperationStats(ds, qry, systems, operations)
    except Exception as ex:
      print "Failed to process all data for dataset, %s" % ds
      print (ex)
      sys.exit(0)

  bars = [None] * len(operations)
 
  data_list = []
  for ds in datasets:
    qlist = query_list[workload[ds]]
    data_list.extend ([time[(ds,q)] for q in qlist])


  # Plot graph bars
  for data in data_list:
    bottom = [0]*len(systems)
    for i, op in enumerate(operations):
      barcolor = op_colors[i] if isColor else op_greyscale[op]
      pattern = None if isColor else op_pattern[op]
      bars[i] = plt.bar(inds+offset, data[i], width, color=barcolor, bottom=bottom, hatch=pattern) 
      bottom = [sum(x) for x in zip(data[i], bottom)]

    offset += len(systems) + spacing

  ax = plt.gca()
  ax.set_axisbelow(True)

  plt.ylabel(metric.label) #['label'])
  plt.ylim(ymin=0, ymax=100)
  plt.grid(which='major', axis='y', linewidth=0.75, linestyle='--', color='0.75')
  plt.tick_params(axis='y', which='both', left='on', right='on')

  plt.xlim(xmax=(width*(len(systems)+spacing)*num_queries))
  inds = np.array(range((+spacing)*len(syslabels)))

  plt.xticks(inds+spacing, syslabels, fontsize='small')
  plt.tick_params(axis='x', which='both', bottom='off', top='off')
  ax.xaxis.set_minor_locator(plt.MultipleLocator(width*len(systems)+spacing))
  plt.grid(which='minor', axis='x', linewidth=0.75, linestyle='-', color='0.75')

  plt.legend(bars[::-1], operations[::-1], loc='upper left', fontsize='medium')
  plt.tight_layout()
  path = utils.checkDir(WEB_DIR + "%s_graphs/" % metric.label)
  flabel = "%sop_graph" % fileprefix
  filename = path + "%s_%s.jpg" % (flabel, metric.label)
  fig.savefig(filename)
  utils.buildIndex(path, "Consoildated Graphs")
  plt.close()





#---------------------------------------------------------------------------------
#  plotQueryResult -- draws simple results for a single query
#--------------------------------------------------------------------------------
def plotQueryResults(ds, metric, isColor=True):
  print "Drawing %s for %s..." % (metric.title, ds), 
  queries = query_list[workload[ds]]
  qlabels = query_labels[workload[ds]]
  width = 1
  spacing = width
  offset = width / 2.
  results = {q: {sys: (0.,0.) for sys in systems} for q in queries}
  conn = db.getConnection()
  cur = conn.cursor()

  index = np.arange(len(systems))

  fig = plt.figure(figsize=FIGURE_SIZE)
  for i, sys in enumerate(systems):

    # Get data
    query = metric.query(ds, sys)
    cur.execute(query)
    for row in cur.fetchall():
      qry, val, err = row

      if metric.label == 'memory' and sys == 'Oracle':
        val += Oracle_comprmem[ds][int(qry)] * 1024
      results[int(qry)][sys] = (metric.convert(val), metric.convert(err))

  # TODO:  Update color/pattern list
  pattern = None if isColor else sys_pattern[sys]
  barcolor = [sys_colors[s] for s in systems]
  barlabel = [system_labels[s] for s in systems]

  for qry in queries:
  # Unzip date into plot-able vectors & draw each bar
#  data = zip(*[ (metric.convert(v[0]), metric.convert(v[1])) for k, v in results.items()] ) # in cur.fetchall() ])
    data = zip(* [(v[0], v[1]) for k, v in results[qry].items()] )
    plt.bar(index, data[0], width, color=barcolor, yerr=data[1], label=barlabel, hatch=pattern)

    ax = plt.gca()
    ax.xaxis.set_major_locator(plt.MultipleLocator(1.0 - width/2.))
    ax.xaxis.set_minor_locator(plt.MultipleLocator(width))
    ax.get_xaxis().set_tick_params(pad=2)

    plt.ylabel(metric.axis)
    plt.ylim(ymin=0, ymax=manual_ymax[ds])
    plt.grid(which='major', axis='y', linewidth=0.75, linestyle='--', color='0.75')
    ax.xaxis.set_minor_locator(plt.MultipleLocator(width*len(systems)+spacing))
    plt.grid(which='minor', axis='x', linewidth=0.75, linestyle='-', color='0.75')

    plt.xticks(index + .5, qlabels, va='top')

    plt.title("%s, %s" % (metric.title, ds.upper()))
    plt.gca().set_axisbelow(True)
    plt.legend(loc='upper left', fontsize='small')
    plt.tight_layout()
    path = utils.checkDir(WEB_DIR + "small_%s_graphs/" % metric.label)
    filename = path + "%s_graph_%s_q%s.jpg" % (metric.label, ds, qry)
    plt.savefig(filename) 
    plt.close()
    print ' Saved %s' % filename

  utils.buildIndex(path, "Consoildated Graphs, %s" % metric.axis)


#---------------------------------------------------------------------------------
#  plotScalability
#--------------------------------------------------------------------------------
def plotTimePerCore(dic,outfile, keyFn, displayFn):
  fig, ax = plt.subplots()
  locator = plt.MaxNLocator(nbins=4)
  ax.yaxis.set_major_locator(locator)
  plt.ylabel('Time per Core (ms)')
  plt.xlabel('Number of Cores')
  plt.grid(which='major', axis='y', linewidth=0.75, linestyle='--', color='0.75')
  plt.grid(which='major', axis='x', linewidth=0.75, linestyle='--', color='0.75')
  scalabilityLineGraph(dic, keyFn, displayFn)
  m = max([max(dic[key]) for key in dic])

  plt.ylim(ymin=0, ymax=m+(m/15))
  plt.xlim(xmin=0,xmax=272)
  plt.legend(loc='upper right', fontsize='medium', markerscale=.8)
  plt.savefig(outfile) 
  plt.close()

def plotRawTimes(dic,outfile,keyFn,displayFn):
  # Plot raw times
  fig, ax = plt.subplots()
  locator = plt.MaxNLocator(nbins=4)
  ax.yaxis.set_major_locator(locator)
  plt.ylabel('Time (s)')
  plt.xlabel('Number of Cores')

  scalabilityLineGraph(dic, keyFn, displayFn)
  
  plt.grid(which='major', axis='y', linewidth=0.75, linestyle='--', color='0.75')
  plt.grid(which='major', axis='x', linewidth=0.75, linestyle='--', color='0.75')

  plt.xlim(xmin=0,xmax=272)
  plt.legend(loc='upper left', fontsize='medium', markerscale=.8)
  plt.savefig(outfile) 
  plt.close()

def getScalabilityData(tableName):
  time_per_cores = {}
  avg_times = {}
  
  conn = db.getConnection()
  cur = conn.cursor()
  query = "SELECT query, dataset, avg_time, time_per_core from %s" % (tableName,)
  cur.execute(query)
  for row in cur.fetchall():
    query = row[0]
    dataset = int(row[1])
    avg_time = row[2]
    time_per_core = row[3]

    if query not in time_per_cores:
      time_per_cores[query] = []
    time_per_cores[query].append(time_per_core)
    
    if query not in avg_times:
      avg_times[query] = []
    avg_times[query].append(avg_time/1000.0)

  conn.close()
  return time_per_cores, avg_times

def plotScalability(isColor=True):
  utils.checkDir(WEB_DIR + 'scalability_graphs')
  utils.buildIndex(WEB_DIR + 'scalability_graphs', 'Scalability')

  # Plot TPCH Scalability
  time_per_cores, avg_times = getScalabilityData("mostRecentK3Scalability")

  keyFn = lambda x: int(x)
  displayFn = lambda x: "TPCH Q" + str(x)
  plotTimePerCore(time_per_cores, WEB_DIR + 'scalability_graphs/tpch_per_core.jpg', keyFn, displayFn)
  plotRawTimes(avg_times, WEB_DIR + 'scalability_graphs/tpch.jpg', keyFn, displayFn)
 
  # Plot ML Scalability
  time_per_cores, avg_times = getScalabilityData("mostRecentK3MLScalability")

  keyFn = lambda x: x
  displayFn = lambda x: "SGD" if x == "sgd" else ("K Means" if x == "k_means" else x) 
  plotTimePerCore(time_per_cores, WEB_DIR + 'scalability_graphs/ml_per_core.jpg', keyFn, displayFn)
  plotRawTimes(avg_times, WEB_DIR + 'scalability_graphs/ml.jpg', keyFn, displayFn)
  
def scalabilityLineGraph(dic, sortKeyFn, displayFn):
  xs = [16, 32, 64, 128, 256]
  markers = ["v", "^", "o", "d", "s", "p", "8", "h"]
  colors = ['saddlebrown', 'firebrick', 'goldenrod', 'seagreen', 'slateblue', 'midnightblue', 'orangered', 'yellowgreen']
  i = 0
  for (_,q) in sorted([(sortKeyFn(x), x) for x in dic.keys()]):
    query = displayFn(q)
    plt.plot(xs, dic[q], color=colors[i], linewidth=3.75, marker=markers[i], markersize=10, mew=1, label= query)
    i = i + 1 

#---------------------------------------------------------------------------------
#  plotExernalMetrics --  wrapper call to draw cadvisor graphs
#--------------------------------------------------------------------------------
def draw_cadvisor_graph(ds, qry, data, metric):
  fig  = plt.figure()
  x_range = np.arange(100)
  for sys, vals in data.items():
    print vals
    if len(vals) == 0:
        continue
    elif len(vals) == 1:
        vals.append(vals[0])
    rel_data = vals
    if metric.delta:
        rel_data = [(vals[i] - vals[i-1]) for i in range(1, len(vals))] 
    normdata = normalizeData(rel_data)
    bar_label = sys
    if metric.label == 'cpu_usage_total':
        bar_label ="%s, %d sec" %(sys, len(data[sys]))
    if metric.label == 'memory_usage':
        bar_label ="%s, %d GB (max)" % (sys, max(data[sys]))

    plt.plot(x_range, normdata, label=bar_label, color=sys_colors[sys])

  percent_formatter = FuncFormatter(lambda x, pos: str(x) + r'$\%$' if matplotlib.rcParams['text.usetex'] else str(x))
  plt.title("%s - Query # %s: %s" % (ds.upper(), qry, metric.title))
  plt.legend(loc='best')
  plt.xlabel("Execution (%)")
  plt.gca().xaxis.set_major_formatter(percent_formatter)
  plt.ylabel(metric.axis)
  plt.ylim(ymin=0)
  path = utils.checkDir(WEB_DIR + "cadvisor/%s_%s" % (metric.title, ds))
  filename = path + '/%s_%s_%s' %(metric.title, ds, qry)
  plt.savefig(filename)
  print "Saving file to %s" % (filename)
  plt.close(fig)
  utils.buildIndex(path, "CADVISOR Metrics for %s - %s" % (metric.title, ds.upper()))



#---------------------------------------------------------------------------------
#  plotExernalMetrics --  wrapper call to draw cadvisor graphs
#--------------------------------------------------------------------------------
def plotExternalMetrics(ds, color=True):
  cpu, mem = getCadvisorMetrics(ds, systems, query_list[workload[ds]])
  for qry in query_list[workload[ds]]:
    draw_cadvisor_graph(ds, qry, cpu[qry], cpu_total)
    draw_cadvisor_graph(ds, qry, mem[qry], mem_usage)




#---------------------------------------------------------------------------------
#  SmallOpGraph --  Prints single Query Graph of all Operations
#--------------------------------------------------------------------------------
def plotSmallOpGraph(metric, vals, filename, percent=False, isColor=True):
  print "Drawing %s to %s" %(metric['title'], filename)
  inds = np.array(range(len(systems)))
  width = 0.6
  bottom = [0]*len(systems)
  bars = [None] * len(operations)

  for i, op in enumerate(operations):
    bars[i] = plt.bar(inds, vals[i], width, color=op_colors[i], bottom=bottom)
    bottom = [sum(x) for x in zip(vals[i], bottom)]
  plt.title(filename)
  plt.ylabel(metric['label'])
  slabels = [system_labels[s] for s in systems]
  plt.xticks(inds+width/2., slabels)
  if percent:
    plt.ylim(0, 100)
  lgd = plt.legend(bars[::-1], operations[::-1], loc='center left', bbox_to_anchor=(1, 0.5))
  plt.savefig(filename + ".jpg", bbox_extra_artists=(lgd,), bbox_inches='tight')
  plt.close()



#---------------------------------------------------------------------------------
#  plotQueryOpGraph -- draws all the operation graphs for each query
#--------------------------------------------------------------------------------
def plotQueryOperationsGraph(ds, qry):
  memory, percent_time, abs_time, err = getOperationStats(ds, qry, systems, operations) 

  # Plot Percent Graph
  plt.figure(1)
  path = utils.checkDir('../web/operations/percent_time_%s/' % ds)
  plotSmallOpGraph(p_metric, percent_time, path + 'percent_q%s' % qry, percent=True)
  utils.buildIndex(path, "Percent Time per Operation - %s" % (ds.upper()))

  # Plot Memory Graph
  plt.figure(2)
  path = utils.checkDir('../web/operations/memory_%s/' % ds)
  plotSmallOpGraph(m_metric, memory, path + 'memory_q%s' % qry)
  utils.buildIndex(path, "Memory per Operation - %s" % (ds.upper()))

  # Plot Time Graph
  plt.figure(3)
  path = utils.checkDir('../web/operations/absolute_time_%s/' % ds)
  plotSmallOpGraph(t_metric, abs_time, path + 'time_q%s' % qry)
  utils.buildIndex(path, "Absolute Time Per Operation - %s" % (ds.upper()))

  plt.close()


#---------------------------------------------------------------------------------
#  parseArgs --  executor call to handle input & invoke appropriate methods
#--------------------------------------------------------------------------------
def parseArgs():
  parser = argparse.ArgumentParser()
  parser.add_argument('-d', '--dataset', nargs='+', help='Plot specific dataset (amplab, tpch10g, tpch100g)', required=False)
 # parser.add_argument('-g', '--graphs', help='Plot individual bar graphs for each query for given dataset', action='store_true')
  parser.add_argument('-r', '--results', help='Plot consolidated results for all queries for given dataset', action='store_true')
  parser.add_argument('-a', '--allinone', help='Plot all results in one big Uber Graph', action='store_true')
  parser.add_argument('-c', '--cadvisor', help='Plot individual line graphs of externally collected cadvisor metrics', action='store_true')
#  parser.add_argument('-o', '--operations', help='Plot bar graphs of per-operation metrics ', action='store_true')
  parser.add_argument('-b', '--blackwhite', help='Plot graphs for non-color (black & white) display', action='store_true')
  parser.add_argument('-s', '--scalability', help='Plot graphs for Scalability on K3', action='store_true')
  args = parser.parse_args()

  ds = all_datasets
  if args.dataset:
    ds = []
    for d in args.dataset:
      if d not in all_datasets:
          print "UNKOWN DATASET: %s" % d
          continue
      ds.append(d)

  isColor = False if args.blackwhite else True

  if args.results:
    print 'Plotting Dataset-Consolidated Graphs'
    for d in ds:
      plotLargeBarGraph(timeM, datasets=[d], fileprefix='set_%s'%d)
      plotLargeBarGraph(memoryM, datasets=[d], fileprefix='set_%s'%d)
      plotStackedOperationGraph(timeM, isColor=isColor, datasets=[d], fileprefix='set_%s'%d)
  

  if args.allinone:
    print 'Plotting All-Dataset-In-One t-Consolidated Uber Graphs'
    plotLargeBarGraph(timeM)
    plotLargeBarGraph(timeM, logscale=True)
    plotLargeBarGraph(memoryM, excludeWorkloads=['ml'])
    plotLargeBarGraph(memoryM, norm=True,excludeWorkloads=['ml'])
    plotLargeBarGraph(memoryM, logscale=True, excludeWorkloads=['ml'])
    plotStackedOperationGraph(timeM, isColor=isColor)

    plotLargeBarGraph(ipcM, fileprefix='ipc_')
    plotLargeBarGraph(cachel2M, fileprefix='cachel2_')
    plotLargeBarGraph(cachel3M, fileprefix='cachel3_')
    plotScalability()
 #   plotStackedOperationGraph(m_metric, isColor)

  if args.cadvisor:
    for d in ds:
      print "Plot cadvisor metrics for %s"  % d
      plotExternalMetrics(d)


  if args.scalability:
    plotScalability()


if __name__ == "__main__":
  parseArgs()


