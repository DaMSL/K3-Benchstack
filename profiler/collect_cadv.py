import json
import urllib2
import string
import psycopg2
import time
import sys

if (len(sys.argv) < 3):
    print "usage:  python collect_cadv.py <host> <engine>"
    sys.exit(0)

machine = sys.argv[1]
engine = sys.argv[2]

# Flatten JSON system data for easier processing
def flatten(dd, separator='_', prefix=''):
    return { prefix + separator + k if prefix else k : v
             for kk, vv in dd.items()
             for k, v in flatten(vv, separator, kk).items()
             } if isinstance(dd, dict) else { prefix : dd }

# Hook function to convert  raw data into json
def get_data():
	raw=urllib2.urlopen("http://" + machine + ":8080/api/v1.2/docker/" + engine).read()
	return json.loads(raw)

# Extract fields from json & insert into data table
def parse_data(cur, con, id, poll):
	tick = flatten(poll)
	cur.execute("SELECT COUNT(timestamp) FROM cadvisor WHERE timestamp = '" + tick['timestamp'] + "';")
	# print "Insert:  " + tick['timestamp']
	if cur.fetchone()[0] == 0:
		cur.execute("INSERT INTO cadvisor VALUES ('" + \
					str(id) + "', '" + \
					tick['timestamp'] + "', '" + \
					str(tick['memory_usage']) + "', '" + \
					str(tick['memory_working_set']) + "', '" + \
					str(tick['cpu_usage_system']) + "', '" + \
					str(tick['cpu_usage_total']) + "', '" + \
					str(tick['cpu_usage_user']) + "', '" + \
					str(tick['network_rx_bytes']) + "', '" + \
					str(tick['network_tx_bytes']) + "')")
	con.commit()

if __name__ == '__main__':


	print "Collecting '" + engine + "' container on '" + machine + "'"

	con = psycopg2.connect(host="mddb", database="postgres", user="postgres", password="password")
	cur = con.cursor()
	
	cur.execute("INSERT INTO trials (system) VALUES ('" + engine + "');")
	cur.execute("SELECT MAX(run_id) FROM trials;")
	run_id = cur.fetchone()[0]
	print "Your RUN ID is  " + str(run_id)

	while True:		
		myobj = get_data()
		parse_data(cur, con, run_id, myobj.values()[0]['stats'][0])
		time.sleep(1)
	
