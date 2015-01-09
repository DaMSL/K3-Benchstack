import sys
import os
import numpy as np
import matplotlib.pyplot as plt

import db.db as db
import utils.utils as utils
import metric_plots as mplots

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

def plotBarChart(name, bars, outfile, conn):
  title = name
  numSections = 1
  
  # Start plotting
  ind = np.arange(numSections)
  width = 0.25
  fig, ax = plt.subplots()

  # Add Bars
  mpbars = []
  i = 0
  colors = ['r', 'y', 'g', 'b' ]
  for bar in bars:
    means = [bar.avg]
    stds  = [bar.err]
    mpbar = ax.bar(ind + (i * width), means, width, color=colors[i], yerr=stds)
    mpbars.append(mpbar)
    i = i + 1

  ax.set_ylabel("Time (ms)")
  ax.set_title(title)
  ax.set_xticks(ind+width)
  ax.legend([mpbar[0] for mpbar in mpbars], [bar.system for bar in bars], loc=2)
  ax.set_xticklabels([]) 
  plt.savefig(outfile)
  plt.close(fig)

  eid = bars[0].exp_id
  db.registerPlot(conn, eid)

