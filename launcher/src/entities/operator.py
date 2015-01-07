class Operator:
  def __init__(self, trial_id, operator_num, operator_name, elapsed_ms, mem_allocated):
    self.trial_id = trial_id
    self.operator_num = operator_num
    self.operator_name = operator_name
    self.elapsed_ms = elapsed_ms
    self.mem_allocated = mem_allocated

  def tup(self):
    return (self.trial_id, self.operator_num, self.operator_name, self.elapsed_ms, self.mem_allocated)
