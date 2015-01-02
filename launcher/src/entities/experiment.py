# Experiment class. Wraps 3 strings: (workload, query, dataset)
# For example: ("tpch", "1", "tpch10g") specifies TPCH Query 1 on the 10GB dataset.
class Experiment:
  # TODO validate that query and dataset are compatible with workload
  def __init__(self, workload, query, dataset):
    self.workload = workload
    self.query = query
    self.dataset = dataset

  def name(self):
    return ("(Workload: %s, Query: %s, Dataset: %s)" % (self.workload, self.query, self.dataset))
