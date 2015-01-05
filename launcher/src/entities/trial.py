# The Trial class represents a single row in the trials table.
class Trial:
  def __init__(self, experiment_id, trial_num, system, ts):
    self.experiment_id = experiment_id
    self.trial_num = trial_num
    self.system = system
    self.ts = ts

  def tup(self):
    return (self.experiment_id, self.trial_num, self.system, self.ts) 

