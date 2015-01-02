# The Trial class represents a single row in the trials table.

class Trial:
  def __init__(self, system, query, dataset, trial, ts):
    self.system = system
    self.query = query
    self.dataset = dataset
    self.trial = trial
    self.ts = ts

  def tup(self):
    return (self.system, self.query, self.dataset, self.trial, self.ts) 

# The Result class represents a single row in the results table.
class Result:
  def __init__(self, run_id, status, elapsed):
    self.run_id = run_id
    self.status = status
    self.elapsed = elapsed

  def tup(self):
    return (self.run_id, self.status, self.elapsed)
