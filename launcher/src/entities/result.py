# Results of an exeriment.
# 3 Possibilities: 
#   Failure => Query failed to run. 
#   Skipped => The system can't run the query, so it was skipped. 
#   Success => Query ran successfully, elapsed time was captured

# The Result class represents a single row in the results table.
class Result:
  def __init__(self, trial_id, status, elapsed, notes):
    self.trial_id = trial_id
    self.status = status
    self.elapsed = elapsed
    self.notes = notes

  def tup(self):
    return (self.trial_id, self.status, self.elapsed, self.notes)
