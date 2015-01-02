# Results of an exeriment.
# 3 Possibilities: 
#   Failure => Query failed to run. 
#   Skipped => The system can't run the query, so it was skipped. 
#   Success => Query ran successfully, elapsed time was captured

class Failure:
  def __init__(self, message):
    self.message = message

class Skipped:
  def __init__(self, message):
    self.message = message  

class Success:
  def __init__(self, elapsed):
    self.elapsed = elapsed
