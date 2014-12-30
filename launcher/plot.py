import sys
import numpy as np
import matplotlib.pyplot as plt

class Point:
  def __init__(self, tup):
    (sys, dataset, query, avg, err) = tup
    self.system = sys
    self.dataset = dataset
    self.query = query
    self.avg = avg
    self.err = err

def getLatestPoints(conn):
  result = []
  cur = conn.cursor()
  cur.execute("SELECT * FROM latest_trials_stats");
  return [Point(t) for t in cur.fetchall() ] 

def plotLatest(conn):
  points = getLatestPoints(conn)
  plotData(points, "web/latest.png")

# Group Points by system (assume same set of queries, in same order)
def groupPoints(points):
  data = {}
  queries = []
  for point in points:
    if point.system not in data:
      data[point.system] = {'means': [], 'stds': []}
    data[point.system]['means'].append(float(point.avg))
    data[point.system]['stds'].append(float(point.err))
    key = (point.dataset, point.query)
    if key not in queries:
      queries.append(key)
  return (data,queries)

def plotData(points, outfile):
  (data,queries) = groupPoints(points)
  title = "Benchmarks" 
  numSections = len(queries)
  xLabels = [ ds + "." + q for (ds, q) in queries ]

  # Start plotting
  ind = np.arange(numSections)
  width = 0.25
  fig, ax = plt.subplots()

  # Add Bars
  bars = []
  i = 0
  colors = ['r', 'y', 'g', 'b' ]

  for sys in sorted(data):
    means = data[sys]['means']
    stds  = data[sys]['stds']
    bar = ax.bar(ind + (i * width), means, width, color=colors[i], yerr=stds)
    bars.append(bar)
    i = i + 1

  ax.set_ylabel("Time (ms)")
  ax.set_title(title)
  ax.set_xticks(ind+width)
  ax.set_xticklabels(xLabels)
  ax.legend([bar[0] for bar in bars], sorted([sys for sys in data]), loc=2)
  plt.savefig(outfile)

if __name__ == "__main__":
  t = ("Vertica", "tpch10g", "1", "100", "10")
  t2 = ("Impala", "tpch10g", "1", "120", "15")  
  t3 = ("Impala", "tpch10g", "2", "121", "25")  
  t4 = ("Vertica", "tpch10g", "2", "110", "5")  
  points = [Point(t), Point(t2), Point(t3), Point(t4)]
  plotData(points, "foo.png")
