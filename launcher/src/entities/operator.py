class Operator:
  def __init__(self, trial_id, operator_num, operator_name, time, percent_time, mem_reserved, objects=""):
    self.trial_id = trial_id
    self.operator_num = operator_num
    self.operator_name = operator_name
    self.time = time
    self.percent_time = percent_time
    self.mem_reserved = mem_reserved
    self.objects = objects

  def tup(self):
    return (self.trial_id, self.operator_num, self.operator_name, self.time, self.percent_time, self.mem_reserved, self.objects) 
