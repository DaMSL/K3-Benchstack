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

systems         = ['Vertica', 'Oracle', 'Spark', 'Impala', 'Impala-p', 'K3']

system_labels   = {'Vertica': 'DB X', 'Oracle':'DB Y', 'Spark': 'Spark', 
                        'Impala': 'Impala', 'K3': 'K3', 'Impala-p': 'Impala (P)'}

sys_colors      = {'Vertica':'red', 'Oracle':'green', 'Spark':'blue', 'Impala':'royalblue', 'Impala-p':'cyan', 'K3':'gold'}
sys_greyscale   = {'Vertica':'dimgrey', 'Oracle':'darkgrey', 'Spark':'whitesmoke', 'Impala':'lightgrey', 'Impala-p':'silver', 'K3':'black'}
sys_pattern     = {'Vertica':'', 'Oracle':'', 'Spark':'', 'Impala':'--', 'Impala-p':'--', 'K3':''}

operations      = ['Planning','TableScan','Join','GroupBy','Exchange', 'FilterProject']
op_colors       = ['gold', 'crimson', 'royalblue', 'olivedrab', 'darkmagenta', 'saddlebrown']
op_greyscale    = {'Planning':'black','TableScan':'whitesmoke','Join':'dimgrey','GroupBy':'silver','Exchange':'darkgrey', 'FilterProject':'gainsboro'}
op_pattern      = {'Planning':'','TableScan':'','Join':'','GroupBy':'','Exchange':'xxx', 'FilterProject':'|||'}

workload        = {'tpch10g':'tpch', 'tpch100g':'tpch', 'amplab':'amplab'}

query_labels    = {'tpch':   {1:'Q1', 3:'Q3', 5:'Q5', 6:'Q6', 11:'Q11', 18:'Q18', 22:'Q22'},
                   'amplab': {1:'Q1', 2:'Q2', 3:'Q3'}}

query_list      = {'tpch':[1, 3, 5, 6, 11, 18, 22], 'amplab': [1,2,3]}


manual_ymax     = {'tpch10g': 40, 'tpch100g': 250, 'amplab': 100}

cpu_total = metric.Metric(label="cpu_usage_total", convert=(lambda t: t/1000000.), title="CPU_Total", axis="Time (ms)", delta=True)
#cpu_system = Metric(label="cpu_usage_system", convert=conv_cpu, title="CPU_System", axis="Time (ms)", delta=True)
mem_usage = metric.Metric(label="memory_usage", convert=(lambda m: m / (1024. * 1024. * 1024)), title="MEM_Usage", axis="Mem (GB)")

timeM = metric.Metric(title='Execution Time', convert=(lambda t: t/1000.), label='time', axis='Time (sec)', query=time_query)
memoryM = metric.Metric(title='Peak Memory', convert=(lambda m: m/1024.), label='memory', axis='Mem (GB)', query=mem_query)

p_metric= {'title':'Percent Time by Operation', 'label':'Percent Time', 'fileprefix': 'percent_'}
t_metric= {'title':'Time by Operation', 'label':'Time (sec)', 'fileprefix': 'time_'}
m_metric= {'title':'Peak Memory Allocated by Operation', 'label':'Mem (GB)', 'fileprefix': 'memory_'}

def normalizeData(data, norm_from=0, norm_to=100):
    to = np.linspace(norm_from, norm_to, len(data))
    tn = np.linspace(norm_from, norm_to, norm_to)
    dn = np.interp(tn, to, data)
    return dn


#---------------------------------------------------------------------------------
#  plotQueryOpGraph -- wrapper call to draw  operation graphs for ALL queries for both metrics
#--------------------------------------------------------------------------------
def plotAllOperationMetrics(ds, isColor=True):
  qlist = query_list[workload[ds]]
  qlabel = query_labels[workload[ds]]

  time = {}
  memory = {}
  error = {}
  try:
    for qry in qlist:
      memory[qry], p, time[qry], error[qry] = getOperationStats(ds, qry, systems, operations)
  except Exception as ex:
    print "Failed to process all data for dataset, %s" % ds
    print (ex)
    sys.exit(0)

  plotBigOpGraph(ds, memory, m_metric, None, isColor)
  plotBigOpGraph(ds, time, t_metric, error, isColor)


#---------------------------------------------------------------------------------
#  plotBigOpGraph -- plotting call to draw large graph for given metric & data
#--------------------------------------------------------------------------------
def plotBigOpGraph(ds, data, metric, error=None, isColor=True):
  qlist = query_list[workload[ds]]
  qlabel = query_labels[workload[ds]]

  inds = np.array(range(len(systems)))
  width = 1
  spacing = 1
  fig = plt.figure(figsize=(12, 4))
  offset = spacing / 2.
  bars = [None] * len(operations)
  val_labels = []
  

  # Plot graph bars
  for qry in qlist:
    if qry not in data:
        continue
    bottom = [0]*len(systems)
    bars = [None] * len(operations)
    for i, op in enumerate(operations):
      barcolor = op_colors[i] if isColor else op_greyscale[op]
      pattern = None if isColor else op_pattern[op]
      bars[i] = plt.bar(inds+offset, data[qry][i], width, color=barcolor, bottom=bottom, hatch=pattern) 
      bottom = [sum(x) for x in zip(data[qry][i], bottom)]
      if i == len(operations)-1 and error!=None:
        plt.errorbar(inds+offset+width/2., bottom, linestyle='None', color='black', yerr=error[qry])

    for x, y in zip (inds, bottom):
      if y == 0:
        plt.text(x + offset + width/2., 0, 'X', ha='center', size='large', color='darkred')
      elif y < (manual_ymax[ds] / 10):
        plt.text(x + offset + width/2., y, '%.1f' % y, size='xx-small', ha='center', va='bottom')
      elif y > manual_ymax[ds]:  
        plt.text(x + offset, manual_ymax[ds]- min(10, manual_ymax[ds]/12.), '%.0f' % y, size='xx-small', ha='right', va='bottom')
#      elif y < 100:
#        label_align = 'center' if error == None or error[qry][x] < .03*y else 'left'
#        plt.text(x + offset + width/2., y, ' %.0f' % y, size='xx-small', ha=label_align, va='bottom')
    val_labels.extend([str(b) for b in bottom])
    offset += len(systems) + spacing

  ax = plt.gca()
  ax.set_axisbelow(True)

  plt.ylabel(metric['label'])
  plt.ylim(ymin=0, ymax=manual_ymax[ds])
  plt.grid(which='major', axis='y', linewidth=0.75, linestyle='--', color='0.75')
  plt.tick_params(axis='y', which='both', left='on', right='on')

  plt.xlim(xmax=(width*(len(systems)+spacing)*len(qlist)))
  syslabels = ['X', 'Y', 'S', 'I', 'Ip', 'K', ''] * len(qlist)  
  inds = np.array(range((+spacing)*len(syslabels)))

  plt.xticks(inds+spacing, syslabels)
  plt.tick_params(axis='x', which='both', bottom='off', top='off')
  ax.xaxis.set_minor_locator(plt.MultipleLocator(width*len(systems)+spacing))
  plt.grid(which='minor', axis='x', linewidth=0.75, linestyle='-', color='0.75')

  plt.title("%s for all Queries, %s" % (metric['title'], ds.upper()))
  plt.legend(bars[::-1], operations[::-1], loc='best')
  plt.show()
  plt.tight_layout()
  path = utils.checkDir('../web/%sgraphs' % metric['fileprefix'])
  fig.savefig(path + '/%sper_operation_%s.jpg' % (metric['fileprefix'], ds))
  plt.close()


#---------------------------------------------------------------------------------
#  plotAllTimes -- draws all the operation graphs for each query
#--------------------------------------------------------------------------------
def plotConsolidated(ds, metric, isColor=True):
  print "Drawing %s for %s..." % (metric.title, ds), 
  queries = query_list[workload[ds]]
  width = 1./(len(systems) + 1)
  spacing = width
  offset = width / 2.
  results = {}
  conn = db.getConnection()
  cur = conn.cursor()

  index = np.arange(len(queries))

  fig = plt.figure(figsize=(12, 5))
  for i, sys in enumerate(systems):

    # Initialize data to 0
    for q in queries:
        results[q] = (0.,0.)

    # Get data
    query = metric.query(ds, sys)
    cur.execute(query)
    for row in cur.fetchall():
      qry, val, err = row

      if metric.label == 'memory' and sys == 'Oracle':
        val += Oracle_comprmem[ds][int(qry)] * 1024
      results[int(qry)] = (val, err)

    # Unzip date into plot-able vectors & draw each bar
    data = zip(*[ (metric.convert(v[0]), metric.convert(v[1])) for k, v in results.items()] ) # in cur.fetchall() ])
    pattern = None if isColor else sys_pattern[sys]
    barcolor = sys_colors[sys] if isColor else sys_greyscale[sys]
    plt.bar(offset + index + (width)*i, data[0], width, color=barcolor, yerr=data[1], label=system_labels[sys], hatch=pattern)

    # Check for annotations 
    for x, y in zip (index, data[0]):
      if y == 0:
        plt.text(offset + x + (width)*i + width/2., 0, 'X', ha='center', size='large', color='darkred')
      elif y < 10:
        plt.text(offset + x + (width)*i + width/2., y, '%.1f' % y, size='x-small', ha='center', va='bottom')
      elif y < manual_ymax[ds]:
        label_align = 'center' if data[1][x] < .03*y else 'left'
        plt.text(offset + x + (width)*i + width/2., y, '%.0f' % y, size='xx-small', ha=label_align, va='bottom')
      else:
        plt.text(offset + x + (width)*i, manual_ymax[ds]-5, '%.0f' % y, size='xx-small', ha='right', va='top')

  ax = plt.gca()
  ax.xaxis.set_major_locator(plt.MultipleLocator(1.0 - width/2.))
  ax.xaxis.set_minor_locator(plt.MultipleLocator(width))
  ax.get_xaxis().set_tick_params(pad=2)

  plt.ylabel(metric.axis)
  plt.ylim(ymin=0, ymax=manual_ymax[ds])
  plt.grid(which='major', axis='y', linewidth=0.75, linestyle='--', color='0.75')
  ax.xaxis.set_minor_locator(plt.MultipleLocator(width*len(systems)+spacing))
  plt.grid(which='minor', axis='x', linewidth=0.75, linestyle='-', color='0.75')

  plt.xlabel("Query")
  plt.xticks(index + .5, queries)

  plt.title("%s, %s" % (metric.title, ds.upper()))
  plt.gca().set_axisbelow(True)
  plt.legend(loc='best')
  plt.tight_layout()
  plt.show()
  path = utils.checkDir("../web/%s_graphs/" % metric.label)
  filename = path + "%s_graph_%s.png" % (metric.label, ds)
  plt.savefig(path + "%s_graph_%s.png" % (metric.label, ds))
  plt.close()
  utils.buildIndex(path, "Consoildated Graphs, %s" % metric.axis)
  print ' Saved to %s' % filename





#---------------------------------------------------------------------------------
#  plotExernalMetrics --  wrapper call to draw cadvisor graphs
#--------------------------------------------------------------------------------
def draw_cadvisor_graph(ds, qry, data, metric):
  fig  = plt.figure()
  x_range = np.arange(100)
  for sys, vals in data.items():
    if len(vals) == 0:
        continue
    rel_data = [(vals[i] - vals[i-1]) for i in range(1, len(vals))] if metric.delta else vals
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
  path = utils.checkDir("../web/cadvisor/%s_%s" % (metric.title, ds))
  filename = path + '/%s_%s_%s' %(metric.title, ds, qry)
  plt.savefig(filename)
  print "Saving file to %s" % (filename)
  plt.close(fig)
  utils.buildIndex(path, "CADVISOR Metrics for %s - %s" % (metric.title, ds.upper()))



#---------------------------------------------------------------------------------
#  plotExernalMetrics --  wrapper call to draw cadvisor graphs
#--------------------------------------------------------------------------------
def plotExternalMetrics(ds, color=True):
  cpu, mem = getCadvisorMetrics(ds, query_list[workload[ds]])
  for qry in query_list[workload[ds]]:
    draw_cadvisor_graph(ds, qry, cpu[qry], cpu_total, color)
    draw_cadvisor_graph(ds, qry, mem[qry], mem_usage, color)




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
  parser.add_argument('-g', '--graphs', help='Plot consolidate bar graph of all system\'s results for given dataset', action='store_true')
  parser.add_argument('-c', '--cadvisor', help='Plot individual line graphs of externally collected cadvisor metrics', action='store_true')
  parser.add_argument('-o', '--operations', help='Plot bar graphs of per-operation metrics ', action='store_true')
  parser.add_argument('-b', '--blackwhite', help='Plot graphs for non-color (black & white) display', action='store_true')
  args = parser.parse_args()

  ds = ['tpch10g', 'tpch100g', 'amplab']
  if args.dataset:
    ds = []
    for d in args.dataset:
      ds.append(d)

  isColor = False if args.blackwhite else True

  if args.graphs:
    print 'Plotting Consolidated Uber Graphs'
    for d in ds:
      plotAllOperationMetrics(d, isColor)
      plotConsolidated(d, timeM, isColor)
      plotConsolidated(d, memoryM, isColor)
#      plotAllMemory(d)

  if args.cadvisor:
    for d in ds:
      print "Plot cadvisor metrics for %s"  % d
      plotExternalMetrics(d)

  if args.operations:
    print "Plot Operator metrics"
    for d in ds:
      for qry in query_list[workload[d]]:
        plotQueryOperationsGraph(d, qry)




if __name__ == "__main__":
  parseArgs()


