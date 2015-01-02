import psycopg2
import sys
import datetime as dt
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt

#run = int(sys.argv[1] )

def conv_ts(ts):
    time = ts.split('.')
    return dt.datetime.strptime(time[0], "%Y-%m-%dT%X")

def conv_mem(m):
    return int(m / (1024 * 1024))

def conv_cpu(c):
    return int(c / 1000000)


def get_timedata(con, data, id):
    cur = con.cursor()
    cur.execute("SELECT timestamp, %s FROM cadvisor WHERE run_id= %d ORDER BY timestamp;" % (data,id))
    row = cur.fetchone()
    start = conv_ts(row[0])
    x = [0]
    y = [conv_cpu(row[1])]    
    for row in cur.fetchall():
        x.append(int((conv_ts(row[0])-start).total_seconds()))
        y.append(conv_cpu(row[1]))
    return x, y

    

if __name__ == '__main__':
    con = psycopg2.connect(host="mddb", database="postgres", user="postgres", password="password")
    cur = con.cursor()

    fig= plt.figure(figsize=(5,4))

    for run in range(2,4):
        cur.execute("SELECT system FROM trials WHERE run_id=%d;" % (run))
        engine = cur.fetchone()[0]
        x, y = get_timedata(con, "cpu_usage_total", run)
        ymin = min(y)
        cpu = [(val - ymin) for val in y]
        plt.plot(x,cpu, label=engine)
    plt.title("CPU Usage - Total")
    plt.legend()
    plt.xlabel("Time (sec)")
    plt.ylabel("Time (ms)")
    fig.savefig("cpu.jpg", dpi=100)

