import psycopg2
import sys
import datetime as dt
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt

run = int(sys.argv[1] )

def conv_ts(ts):
    time = ts.split('.')
    return dt.datetime.strptime(time[0], "%Y-%m-%dT%X")

def conv_mem(m):
    return int(m / (1024 * 1024))


def get_timedata(con, data, id):
    cur = con.cursor()
    cur.execute("SELECT timestamp, %s FROM cadvisor WHERE run_id= %d ORDER BY timestamp;" % (data,id))
    row = cur.fetchone()
    start = conv_ts(row[0])
    x = [0]
    y = [conv_mem(row[1])]    
    for row in cur.fetchall():
        x.append(int((conv_ts(row[0])-start).total_seconds()))
        y.append(conv_mem(row[1]))
    return x, y

    

if __name__ == '__main__':
    con = psycopg2.connect(host="mddb", database="postgres", user="postgres", password="password")
    x, y = get_timedata(con, "memory_usage", run)


    fig= plt.figure(figsize=(5,4))
    plt.plot(x,y)
    plt.title("Memory Usage")
    plt.xlabel("Time (sec)")
    plt.ylabel("Memory (MB)")
    fig.savefig("mem.jpg", dpi=100)

