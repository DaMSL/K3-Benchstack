import psycopg2
import sys
import os
import datetime as dt
import numpy as np
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt

import db.db as db
import utils.utils as utils

def conv_ts(ts):
    time = ts.split('.')
    return dt.datetime.strptime(time[0], "%Y-%m-%dT%X")

def conv_mem(m):
    return int(m / (1024 * 1024 * 1024))

def conv_cpu(c):
    return float(c / 1000000)


class Metric:
    def __init__(self, label, convert, title, axis, delta=False):
        self.label = label
        self.convert = convert
        self.title = title
        self.axis = axis
        self.delta = delta

all_systems = ['Vertica', 'Oracle', 'Spark', 'Impala']
cpu_total = Metric("cpu_usage_total", conv_cpu, "CPU_Total", "Time (ms)", True)
cpu_system = Metric("cpu_usage_system", conv_cpu, "CPU_System", "Time (ms)", True)
mem_usage = Metric("memory_usage", conv_mem, "MEM_Usage", "Mem (GB)")
sys_color = {'Vertica':'r', 'Oracle':'g', 'Spark':'b', 'Impala':'c'}

distro_sys = ['Impala', 'Spark']

def buildIndex(path, title):
  index =  '<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 3.2 Final//EN"><html>'
  index += '<title>%s</title>' % title
  index += '<body><h2>%s</h2>' % title
  for img in sorted(os.listdir(path)):
    index += '<img src="%s" />' % img  
  index += '</body></html>'
  indexfile = open(path + '/index.html', 'w')
  indexfile.write(index)
  indexfile.close()

def getExperimentMetrics(conn, expid):
  try:
    query = "SELECT T.experiment_id,trial_id, trial_num, system, query, dataset, workload FROM trials as T, experiments AS E WHERE T.experiment_id = E.experiment_id and T.experiment_id = %s order by system, trial_num;" % expid
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    return results
  except Exception as inst:
      print("Failed to get metric plot data for experiment %d: " % expid)
      print(inst)
      sys.exit(1)

def get_cadvdata (con, metric, run_id, system='', consolidate_trials=True):
    cur = con.cursor()

    if consolidate_trials:
      cur.execute("select %s from cadvisor_baselined where experiment_id=%d AND system='%s';" % (metric.label, run_id, system))
    else:
      cur.execute("SELECT %s FROM cadvisor_collected WHERE trial_id=%d ORDER BY interval;" % (metric.label, run_id))
 
    # Get data, throw out 1st interval
    raw = [metric.convert(row[0]) for row in cur.fetchall()]
#    return (range(len(raw)-1), raw[1:])
#    return (range(len(raw)), raw)

    if not metric.delta:
        return (range(len(raw)), raw)
    incr = [(raw[i] - raw[i-1]) for i in xrange(1, len(raw))]
    cur.close()
    return (range(len(incr)), incr)
      


def normalizeData(data, norm_from=0, norm_to=100):
    to = np.linspace(norm_from, norm_to, len(data))
    tn = np.linspace(norm_from, norm_to, norm_to)
    dn = np.interp(tn, to, data)
    return dn

def draw_experiment_graph(con, dset, qry, expid, metric):
#    x, y = get_cadvdata(con, metric, expid, sys, consolidate_trials=True)

    raw_data = {}
    for s in all_systems:
      raw_data[s] = []

    query = "SELECT system, %s FROM cadvisor_experiment_stats WHERE experiment_id=%d ORDER BY interval" % (metric.label, expid)
    cur = con.cursor()
    cur.execute(query)
    for row in cur.fetchall():
        sys, val = row
        raw_data[sys].append(metric.convert(val))
    fig  = plt.figure()
 #   plt.plot(x, y, label=sys, color=sys_color[sys])

    x_range = np.arange(100)

    for sys, vals in raw_data.items():
        if len(vals) == 0:
            continue
        rel_data = [(vals[i] - vals[i-1]) for i in range(1, len(vals))] if metric.delta else vals
        normdata = normalizeData(rel_data)
        print sys
        print normdata
        plt.plot(x_range, normdata, label="%s, %d sec" %(sys, len(raw_data[sys])), color=sys_color[sys])

    plt.title("%s - Query # %s: %s" % (dset.upper(), qry, metric.title))
    plt.legend(loc='best')
    plt.xlabel("Execution")
    plt.ylabel(metric.axis)
    path = utils.checkDir("../web/cadvisor/%s_%s" % (metric.title, dset))
#    utils.runCommand("mkdir -p %s" % (directory))
#    f = os.path.join(directory, "%s_%s_%s_%s.jpg" % (metric.title, dset, qry, sys))
    
    fig.savefig(path + '/%s_%s_%s' %(metric.title, dset, qry))
    plt.close(fig)
    buildIndex(path, "CADVISOR Metrics for %s - %s" % (metric.title, dset.upper()))


'''
def draw_trial_graph(con, trial, metric):
    exp_id, run, tnum, sys, qry, dset, wkld = trial
    x, y = get_cadvdata(con, metric, run, sys, False)
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
    directory = "../web/trial_graphs/experiment_%s_%d" % (metric.title, exp_id)
    utils.runCommand("mkdir -p %s" % (directory))
    f = os.path.join(directory, "%s_%s_%s.jpg" % (metric.title, sys, tnum))
    fig.savefig(f, dpi=100)
    plt.close(fig)
    title = 'Workload: %s, Experiment: %s,  Query: %s, Metric: %s' % (dset.upper(), exp_id, qry, metric.title)
    buildIndex(directory, title)
'''


def plot_dset_metrics(dset):
    conn = db.getConnection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT experiment_id, query FROM summary WHERE dataset='%s';" % dset)
    for row in cur.fetchall():
      expid, qry = row
      print "Plotting graphs for %s - query %s" % (dset, qry)
      draw_experiment_graph(conn, dset, qry, expid, cpu_total)
      draw_experiment_graph(conn, dset, qry, expid, mem_usage)

'''
def plot_experiment_metrics(expId):
  conn = db.getConnection()
  results = getExperimentMetrics(conn, expId)
  for row in results:
    draw_experiment_graph(conn, row, cpu_total)
    draw_experiment_graph(conn, row, mem_usage)


def plot_trial_metrics(expId):
  conn = db.getConnection()
  results = getExperimentMetrics(conn, expId)
  for row in results:
    draw_trial_graph(conn, row, cpu_total)
    draw_trial_graph(conn, row, mem_usage)
'''
if __name__ == "__main__":
  conn = db.getConnection()
  draw_experiment_graph(conn, 'tpch10g', '1', 144, mem_usage)
  print 'Run from plot.py using -c flag for cadvisor plots.'
  








def draw_all(conn):
  results = db.getMetricPlotData(conn)
  for row in results:
    draw_graph(conn, row, cpu_total)
    draw_graph(conn, row, mem_usage)



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




