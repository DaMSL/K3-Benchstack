import sys
import os
import inspect
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt

# Convert bytes to gigabytes
def toGB(byts):
  return byts / 1024.0 / 1024.0 / 1024.0

# Open XML file and remove all entries except for 'Heap Summary'
def pruneXML(file_path):
  s = ""
  in_event = False
  with open(file_path, 'r') as f:
    for line in f:
      if '<event name="Heap Summary"' in line:
        in_event = True

      if in_event:
        s += line

      if "</event>" in line:
        in_event = False

  return s

# Exctract the timestamp and used-heap-size for each heap summary event
def extractHeapSummaries(file_path):
  s = pruneXML(file_path)
  soup = BeautifulSoup(s)
  events = soup.find_all('event')
  heap_summaries = [x for x in events if x['name'] == 'Heap Summary']
  points = []

  for summary in heap_summaries:
    ts = summary.text.split("(startTime)")[0].strip()
    heap_used = int(summary.heapused.string)
    tup = (int(ts), heap_used)
    points.append(tup)
  return points

# Assuming that each adjacent pair of points is connected by a line
# Return the y value for the given timestamp.
def line(points, ts):
  min_x = points[0][0]
  max_x = points[-1][0]
  for i in range(len(points)):
    curr_ts = points[i][0]
    curr_val = points[i][1]
    if ts >= curr_ts and (i + 1) < len(points) and ts <= points[i+1][0]:
      next_ts = points[i+1][0]
      next_val = points[i+1][1]
      full_width = next_ts - curr_ts
      used_width = ts - curr_ts
      x_frac = 1. * used_width / full_width
      full_height = next_val - curr_val
      return curr_val + (x_frac * full_height)
  return 0

# Open each xml file in a directory and extract the heap summary data, returning a list of (x, y) pairs for each file.
def processXMLDir(dir_path):
  if not os.path.exists(dir_path):
    print("ERROR. Directory not found: %s" % dir_path)
    sys.exit(1)

  all_points = []
  for file_path in [os.path.join(dir_path, f) for f in os.listdir(dir_path)]:
    print("Processing %s" % file_path)
    all_points.append(extractHeapSummaries(file_path))
  return all_points

# Find the global min and max timestamps across all lines
def findTsBounds(all_points):
  min_ts = None
  max_ts = None
  for points in all_points:
    low = points[0][0]
    if min_ts == None or low < min_ts:
      min_ts = low
    high = points[-1][0]
    if max_ts == None or high > max_ts:
      max_ts = high
  return (min_ts, max_ts)

# Create a line with num_samples points. Between min_ts and max_ts.
# Y value is the sum of all y values for the given ts
def interpolate(all_points, num_samples):
  (min_ts, max_ts) = findTsBounds(all_points)
  step_size = float(max_ts - min_ts) / num_samples

  x = min_ts
  result = []
  for _ in range(num_samples):
    y = 0
    for points in all_points:
      y += line(points, x)
    result.append((x, y))
    x += step_size
  return result

if __name__ == "__main__":
  all_points = processXMLDir("xml")
  (min_ts, _) = findTsBounds(all_points)
  res = interpolate(all_points, 100)
  for points in all_points:
    plt.plot([x[0] - min_ts for x in points], [toGB(x[1]) for x in points])

  plt.plot([x[0] - min_ts for x in res], [toGB(x[1]) for x in res])
  plt.savefig('out.png')
