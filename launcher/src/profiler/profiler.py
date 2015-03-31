import json
import urllib2
import string
import psycopg2
import time
import sys
import threading
from docker import Client

class Profiler(threading.Thread):
    
    def __init__(self, machines, engine, trial_id):
      super(Profiler, self).__init__()
      self.machines = machines
      self.engine = engine
      self.containers = {}
       
      for m in self.machines:
        if engine == 'K3':
          self.containers[m] = None
        else:
          self.containers[m] = engine

      self.finished = False
      self.trial_id = trial_id
      self.counter = {}
      for m in self.machines:
        self.counter[m] = 0
    
    # Flatten JSON system data for easier processing
    def flatten(self, dd, separator='_', prefix=''):
        return { prefix + separator + k if prefix else k : v
                 for kk, vv in dd.items()
                 for k, v in self.flatten(vv, separator, kk).items()
                 } if isinstance(dd, dict) else { prefix : dd }
    
    # Hook function to convert raw data into json
    def get_data(self):
        jsons = []
        for machine in self.machines:
            container = self.containers[machine]
            try:
    	      raw=urllib2.urlopen("http://" + machine + ":20080/api/v1.2/docker/" + container).read()
    	      jsons.append(json.loads(raw))
            except Exception:
              return None
        return jsons
    
    # Extract fields from json & insert into data table
    def parse_data(self, cur, con, id, poll, machine):
    	tick = self.flatten(poll)
    	cur.execute("SELECT COUNT(timestamp) FROM cadvisor WHERE timestamp = '" + tick['timestamp'] + "';")
    	# print "Insert:  " + tick['timestamp']
    	if cur.fetchone()[0] == 0:
                self.counter[machine] += 1
    		cur.execute("INSERT INTO cadvisor VALUES ('" + \
    					str(id) + "', '" + \
                                        machine + "', '" + \
    					tick['timestamp'] + "', '" + \
    					str(tick['memory_usage']) + "', '" + \
    					str(tick['memory_working_set']) + "', '" + \
    					str(tick['cpu_usage_system']) + "', '" + \
    					str(tick['cpu_usage_total']) + "', '" + \
    					str(tick['cpu_usage_user']) + "', '" + \
    					str(tick['network_rx_bytes']) + "', '" + \
    					str(tick['network_tx_bytes']) + "', '" + \
                                        str(self.counter[machine]) + "')")
    	con.commit()

    def containersKnown(self):
      for m in self.machines:
        if self.containers[m] == None:
          return False
      return True

    def run(self):
        if self.engine == "K3":
            return

        # For K3: poll docker to find the mesos container. 
        while not self.containersKnown() and not self.finished:
          for m in self.machines:

            if self.containers[m] == None:
              c = Client(base_url='tcp://' + m + ':41000', version='1.15')
              allContainers = c.containers()
              allNames = [c['Names'][0] for c in allContainers]
              mesos = [n for n in allNames if 'mesos' in n]
              if len(mesos) > 0:
                self.containers[m] = mesos[0]
                #print("Container named %s found on %s" % (mesos[0], m))
          time.sleep(1)


        #print("\tCollecting '" + self.engine + "' container on '" + str(self.machines) + "'")
        con = psycopg2.connect(host="qp1", database="postgres", user="postgres", password="password")
        cur = con.cursor()
       
        while not self.finished:		
            myobjs = self.get_data()
            if myobjs:
              for (myobj, machine) in zip(myobjs, self.machines):
                  self.parse_data(cur, con, self.trial_id, myobj.values()[0]['stats'][0], machine)
            time.sleep(1)

def test():
  return Profiler(["mddb"], "orcl", 5)
