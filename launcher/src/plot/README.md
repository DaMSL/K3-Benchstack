# Graph Plotting

Graph plots are run via the ``python -m plot.plot``` command. Graphs are written to the /web folder on the launcher images. Ensure that a python simpleHTTPServer is running to view via wen browser. 

```
usage: plot.py [-h] [-d DATASET [DATASET ...]] [-g] [-c] [-o]

optional arguments:
  -h, --help            show this help message and exit
  -d DATASET [DATASET ...], --dataset DATASET [DATASET ...]
                        Plot specific dataset (amplab, tpch10g, tpch100g)
  -g, --graphs          Plot consolidate bar graph of all system's results for
                        given dataset
  -c, --cadvisor        Plot individual line graphs of externally collected
                        cadvisor metrics
  -o, --operations      Plot bar graphs of per-operation metrics
```
