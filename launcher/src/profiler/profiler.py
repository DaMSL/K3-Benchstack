import json
import urllib2
import string
import psycopg2
import time
import sys
import threading

class Profiler(threading.Thread):
    
    def __init__(self, machines, engine, trial_id):
      super(Profiler, self).__init__()
      self.machines = machines
      self.engine = engine
      self.finished = False
      self.trial_id = trial_id
    
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
    	    raw=urllib2.urlopen("http://" + machine + ":20080/api/v1.2/docker/" + self.engine).read()
    	    jsons.append(json.loads(raw))
        return jsons
    
    # Extract fields from json & insert into data table
    def parse_data(self, cur, con, id, poll, machine):
    	tick = self.flatten(poll)
    	cur.execute("SELECT COUNT(timestamp) FROM cadvisor WHERE timestamp = '" + tick['timestamp'] + "';")
    	# print "Insert:  " + tick['timestamp']
    	if cur.fetchone()[0] == 0:
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
    					str(tick['network_tx_bytes']) + "')")
    	con.commit()

    def run(self):
        print("\tCollecting '" + self.engine + "' container on '" + str(self.machines) + "'")
        con = psycopg2.connect(host="mddb", database="postgres", user="postgres", password="password")
        cur = con.cursor()
       
        while not self.finished:		
            myobjs = self.get_data()
            for (myobj, machine) in zip(myobjs, self.machines):
                self.parse_data(cur, con, self.trial_id, myobj.values()[0]['stats'][0], machine)
            time.sleep(1)

def test():
  return Profiler(["mddb"], "orcl", 5)
