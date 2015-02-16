import db as db


def create_opname():
    conn = db.getConnection()
    cur = conn.cursor()

    all_operations = '''
        Exchange| EXCHANGE
        Exchange| Exchange
        Exchange| MERGING-EXCHANGE
        Exchange| PX BLOCK ITERATOR
        Exchange| PX COORDINATOR
        Exchange| PX RECEIVE
        Exchange| PX SEND BROADCAST
        Exchange| PX SEND QC (RANDOM)
        FilterProject| ExprEval
        FilterProject| FILTER
        FilterProject| Filter
        FilterProject| VIEW
        FilterProject| SELECT STATEMENT
        FilterProject| TopK
        FilterProject| TOP-N
        GroupBy| AGGREGATE
        GroupBy| BUFFER SORT
        GroupBy| GROUPBY
        GroupBy| GroupBy
        GroupBy| GroupByHash
        GroupBy| GroupByPipe
        GroupBy| HASH GROUP BY
        GroupBy| HASH JOIN, EXCHANGE, AGGREGATE, EXCHANGE, AGGREGATE, SCAN HDFS
        GroupBy| PX SEND HASH
        GroupBy| PX SEND HYBRID HASH
        GroupBy| SORT AGGREGATE
        GroupBy| SORT GROUP BY STOPKEY
        GroupBy| STATISTICS COLLECTOR
        GroupBy| Sort
        Join| CROSS JOIN, EXCHANGE, SCAN HDFS
        Join| HASH JOIN
        Join| HASH JOIN ANTI
        Join| HASH JOIN BUFFERED
        Join| HASH JOIN RIGHT SEMI BUFFERED
        Join| HASH JOIN SEMI BUFFERED
        Join| HASH JOIN, AGGREGATE, EXCHANGE, AGGREGATE, SCAN HDFS
        Join| HASH JOIN, EXCHANGE, CROSS JOIN
        Join| HASH JOIN, EXCHANGE, SCAN HDFS
        Join| JOIN
        Join| JOIN FILTER CREATE
        Join| JOIN FILTER USE
        Join| Join
        Join| MERGE JOIN CARTESIAN
        Join| ParallelUnion
        Planning| NewEENode
        Planning| Pre-Execution
        Planning| Root
        Planning| WINDOW BUFFER
        TableScan| COUNT STOPKEY
        TableScan| INDEX UNIQUE SCAN
        TableScan| NESTED LOOPS
        TableScan| SCAN HDFS
        TableScan| SCAN HDFS, SCAN HDFS
        TableScan| Scan
        TableScan| TABLE ACCESS BY INDEX ROWID
        TableScan| TABLE ACCESS INMEMORY FULL
        TableScan| TableScan
          '''

    olist = []
    for o in all_operations.split('\n'):
        x = o.split('|')
        if len(x) == 2:
            olist.append((x[0], x[1].strip()))
    cur.execute("DELETE FROM operator_names;")
    conn.commit()

    for op in olist:
        print "Inserting:  %s, %s" % ((op))
        qry = "INSERT INTO operator_names VALUES ('%s', '%s');" % ((op))
        cur.execute(qry)
        conn.commit()

if __name__ == "__main__":
    create_opname()

