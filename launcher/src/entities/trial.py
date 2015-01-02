# Successfuly experiments will be entered into a database.
# The Trial class represents a single row in the database. 

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
