import psycopg2
import sys
import os
import datetime as dt
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt

import db.db as db
import utils.utils as utils

def conv_ts(ts):
    time = ts.split('.')
    return dt.datetime.strptime(time[0], "%Y-%m-%dT%X")

def conv_mem(m):
    return int(m / (1024 * 1024))

def conv_cpu(c):
    return float(c / 1000000)


class Metric:
    def __init__(self, label, convert, title, axis, delta=False):
        self.label = label
        self.convert = convert
        self.title = title
        self.axis = axis
        self.delta = delta

cpu_total = Metric("cpu_usage_total", conv_cpu, "CPU_Total", "Time (ms)", True)
cpu_system = Metric("cpu_usage_system", conv_cpu, "CPU_System", "Time (ms)", True)
mem_usage = Metric("memory_usage", conv_mem, "MEM_Usage", "Mem (MB)")
sys_color = {'Vertica':'r', 'Oracle':'g', 'Spark':'b', 'Impala':'c'}

distro_sys = ['Impala', 'Spark']


def get_cadvdata_single (con, metric, id, delta=False):
    cur = con.cursor()
    # Get data 
    cur.execute("SELECT %s FROM cadvisor WHERE trial_id= %d ORDER BY timestamp;" % (metric.label, id))
    raw = [metric.convert(row[0]) for row in cur.fetchall()]
    if not delta:
        return (range(len(raw)), raw)
    incr = [(raw[i] - raw[i-1]) for i in xrange(1, len(raw))]
    cur.close()
    return (range(len(incr)), incr)


def get_cadvdata_dist(con, metric, id, delta=False, average=False, debug=False):
    cur = con.cursor()
    
    # Get machine & data value 
    cur.execute("SELECT machine, %s FROM cadvisor WHERE trial_id= %d ORDER BY timestamp;" % (metric.label, id))
    raw = {}
    for row in cur.fetchall():
        if (not row[0] in raw):
           raw[row[0]] = []
        raw[row[0]].append(metric.convert(row[1]))
    instance = {}
    for m in raw:
        instance[m] = [(raw[m][i] - raw[m][i-1]) for i in xrange(1, len(raw[m]))] if delta else raw[m]

    if len(instance) == 0:
      print("Got zero results for %s plot trial: %s" % (metric.label, id))
      return (0, [])

    # First, find min data points
    num_vals = sys.maxint
    num_mach = len(instance)
    for m in instance:
        if len(instance[m]) < num_vals:
            num_vals = len(instance[m])

    # Then, find average for each data point among all machines
    result = []
    for ts in xrange(num_vals):
        total = 0.0
        for m in instance:
            total += instance[m][ts]
        if average:
            result.append(total / float(num_mach))
        else:
            result.append(total)
    cur.close()
    return (range(len(result)), result)

def draw_graph(con, trial, metric, consolidate_graphs=False):
    exp_id, run, tnum, sys, qry, dset, wkld = trial
    cur = con.cursor()
    fig= plt.figure(figsize=(5,4))
    x, y = (get_cadvdata_dist(con, metric, run, metric.delta, average=True)
        if sys in distro_sys
        else get_cadvdata_single(con, metric, run, metric.delta))

    if x == 0:
      return 

    plt.plot(x, y, label=sys, color=sys_color[sys])
    plt.title(metric.title)
    plt.legend(loc='best')
    plt.xlabel("Time (sec)")
    plt.ylabel(metric.axis)
    directory = "../web/cadvisor_graphs/experiment_%d" % exp_id if consolidate_graphs else "../web/%s/%s/%s/experiment_%s/%s/%s/" % (wkld, dset, qry, exp_id, sys, metric.label)
    utils.runCommand("mkdir -p %s" % (directory))
    f = os.path.join(directory, "%s_%s_%s.jpg" % (metric.title, sys, tnum))
    fig.savefig(f, dpi=100)
    plt.close(fig)
    db.registerMetricPlot(con,run)
    if consolidate_graphs:
      buildIndex(directory, dset.upper(), exp_id, qry)

def draw_all(conn):
  results = db.getMetricPlotData(conn)
  for row in results:
    draw_graph(conn, row, cpu_total)
    draw_graph(conn, row, mem_usage)

def getExperimentMetrics(conn, expid):
  try:
    query = "SELECT T.experiment_id,trial_id, trial_num, system, query, dataset, workload FROM trials as T, experiments AS E WHERE T.experiment_id = E.experiment_id and T.experiment_id = %d order by system, trial_num;" % expid
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    return results
  except Exception as inst:
      print("Failed to get metric plot data for experiment %d: " % expid)
      print(inst)
      sys.exit(1)

def buildIndex(path, wkld, exp_id, qry):
  title = 'Workload: %s, Experiment: %s,  Query: %s</title>' % (wkld, exp_id, qry)
  index =  '<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 3.2 Final//EN"><html>'
  index += '<title>%s</title>' % title
  index += '<body><h2>%s</h2>' % title
  for img in sorted(os.listdir(path)):
    index += '<img src="%s" />' % img  
  index += '</body></html>'
  indexfile = open(path + '/index.html', 'w')
  indexfile.write(index)
  indexfile.close()

if __name__ == "__main__":
  conn = db.getConnection()
  for x in [2, 15, 16, 33, 37, 34, 38, 39, 40]:
    results = getExperimentMetrics(conn, x)
    for row in results:
      draw_graph(conn, row, cpu_total, True)
      draw_graph(conn, row, mem_usage, True) 
