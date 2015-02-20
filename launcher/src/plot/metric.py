
#----------------------------------------------------------
#  Metric Class -- defines meta data for given metrics
#---------------------------------------------------------
class Metric:
    def __init__(self, **kwargs):
        self.label      = kwargs.get('label', None)
        self.convert    = kwargs.get('convert', None)
        self.title      = kwargs.get('title', None)
        self.axis       = kwargs.get('axis', None)
        self.delta      = kwargs.get('delta', False)
        self.query      = kwargs.get('query', None)

# Conversion function retained for reference
def conv_ts(ts):    
  time = ts.split('.')
  return dt.datetime.strptime(time[0], "%Y-%m-%dT%X")

def conv_mem(m):
  return int(m / (1024 * 1024 * 1024))

def conv_cpu(c):
  return float(c / 1000000)
