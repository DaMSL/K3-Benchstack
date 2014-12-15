import sys
import yaml
import numpy as np
import matplotlib.pyplot as plt

def loadData(filepath):
  with open(filepath, "r") as f:
    return yaml.load(f.read())

def plotData(d, outfile):
  title = d['title']
  numSections = len(d['sections'])
  xLabels = [ x['name'] for x in d['sections'] ]

  # Group times by System
  systems = {}
  # Initialize empty lists
  for x in d['sections'][0]['data']:
    systems[x['system']] = {'means': [], 'stds': []}

  # Populate lists
  for x in d['sections']:
    for i in range(len(x['data'])):
      sys   = x['data'][i]['system']
      times = x['data'][i]['times']
      mean  = np.mean(times)
      std   = np.std(times)
      systems[sys]['means'].append(mean)
      systems[sys]['stds'].append(std)

  # Start plotting
  ind = np.arange(numSections)
  width = 0.25
  fig, ax = plt.subplots()

  # Add Bars
  bars = []
  i = 0
  colors = ['r', 'y', 'g', 'b' ]

  for sys in sorted(systems):
    means = systems[sys]['means']
    stds  = systems[sys]['stds']
    bar = ax.bar(ind + (i * width), means, width, color=colors[i], yerr=stds)
    bars.append(bar)
    i = i + 1

  ax.set_ylabel("Time (ms)")
  ax.set_title(title)
  ax.set_xticks(ind+width)
  ax.set_xticklabels(xLabels)
  ax.legend([bar[0] for bar in bars], sorted([sys for sys in systems]), loc=2)
  plt.savefig(outfile)

if __name__ == "__main__":

  if len(sys.argv) < 3:
    print("Usage: %s input_yml output_file" % sys.argv[0])
    sys.exit(1)

  infile = sys.argv[1]
  outfile = sys.argv[2]
  d = loadData(infile)
  plotData(d, outfile) 
