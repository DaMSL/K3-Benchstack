class Experiment:
  # TODO validate that query and dataset are compatible with workload
  def __init__(self, workload, query, dataset):
    self.workload = workload
    self.query = query
    self.dataset = dataset

  def name(self):
    return ("workload: %s. query: %s. dataset: %s" % (self.workload, self.query, self.dataset))

class Failure:
  def __init__(self, message):
    self.message = message

class Trial:
  def __init__(self, system, query, dataset, elapsed, trial, ts):
    self.system = system
    self.query = query
    self.dataset = dataset
    self.trial = trial
    self.elapsed = elapsed
    self.ts = ts

  def tup(self):
    return (self.system, self.query, self.dataset, self.trial, self.elapsed, self.ts) 
