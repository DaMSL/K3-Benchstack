import json
import urllib2
import string
import psycopg2
import time
import sys
import atexit
# import mysql.connector
# from mysql.connector import errorcode

engine = sys.argv[1]

def flatten(dd, separator='_', prefix=''):
    return { prefix + separator + k if prefix else k : v
             for kk, vv in dd.items()
             for k, v in flatten(vv, separator, kk).items()
             } if isinstance(dd, dict) else { prefix : dd }

def get_data():
	raw=urllib2.urlopen("http://mddb2:8080/api/v1.2/docker/" + engine).read()
	return json.loads(raw)

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

@atexit.register
def on_exit():
	print "Terminating data collection...."
	myobj = get_data()
	parse_data(cur, con, myobj.values()[0]['stats'][x])
			 
'''
CREATE TABLE metad (
	run_id serial primary key,
	engine text);
'''
			 
'''
CREATE TABLE cadvisor (
	run_id	int,
	timestamp text unique, 
	memory_usage bigint, 
	memory_working_set bigint,
	cpu_usage_system bigint, 
	cpu_usage_total bigint, 
	cpu_usage_user bigint, 
	network_rx_bytes bigint, 
	network_tx_bytes bigint);"
'''	
	
	
	
if __name__ == '__main__':

	print "Collecting on '" + engine + "' container."

#	con = mysql.connector.connect(user='root', password='Mysql1!', database=DB_NAME, host='localhost', buffered=True)
	con = psycopg2.connect(host="mddb2", database="data", user="postgres", password="Postgres1!")
	cur = con.cursor()
	
	cur.execute("INSERT INTO metad (engine) VALUES ('" + engine + "');")
	cur.execute("SELECT MAX(run_id) FROM metad;")
	run_id = cur.fetchone()[0]
	print "Your RUN ID is  " + str(run_id)

	myobj = get_data()
	for x in range(60):
		parse_data(cur, con, run_id, myobj.values()[0]['stats'][x])
	while True:		
		myobj = get_data()
		parse_data(cur, con, run_id, myobj.values()[0]['stats'][0])
		time.sleep(1)
	
