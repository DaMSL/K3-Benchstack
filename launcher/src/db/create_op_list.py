import db as db


def create_opname():
    conn = db.getConnection()
    cur = conn.cursor()

    all_operations = '''
          GroupBy| ParallelUnion, GroupByPipe, GroupByHash
          GroupBy| HASH GROUP BY,HASH JOIN,JOIN FILTER CREATE
          GroupBy| SELECT STATEMENT,VIEW,WINDOW BUFFER,HASH GROUP BY,HASH JOIN,JOIN FILTER CREATE,TABLE ACCESS INMEMORY FULL,JOIN FILTER USE
          TableScan| ExistingRdd,
          Join| Join
          GroupBy| Aggregate,Project
          GroupBy| GroupByPipe
          Exchange| COMM / OTHER
          TableScan| Root, Sort, NewEENode
          GroupBy| Aggregate,Project,Filter,InMemoryColumnarTableScan,
          Join| Project,ExistingRdd,ShuffledHashJoin
          GroupBy| Aggregate
          GroupBy| Aggregate, Project, Filter, ExistingRdd
          GroupBy| ExprEval, GroupByPipe
          TableScan| ExprEval, Scan
          GroupBy| TABLE ACCESS INMEMORY FULL,SORT AGGREGATE
          Join| Join, ExprEval
          GroupBy| ExprEval, Filter, GroupByPipe
          Join| HASH JOIN,JOIN FILTER CREATE
          TableScan| Scan, ExprEval
          GroupBy| ,Aggregate
          TableScan| Root, NewEENode
          GroupBy| Project,Filter,Aggregate,ShuffledHashJoin
          GroupBy| AGGREGATE
          TableScan| InMemoryColumnarTableScan,Project,Filter
          Join| CROSS JOIN
          GroupBy| GroupByPipe, ParallelUnion, GroupByHash
          Join| Join, ParallelUnion, ExprEval
          Join| JOIN FILTER USE,TABLE ACCESS INMEMORY FULL
          TableScan| Project,ExistingRdd,
          TableScan| Scan
          GroupBy| GroupByHash, GroupByPipe, ParallelUnion, ExprEval
          Exchange| EXCHANGE
          TableScan| Project,Filter,InMemoryColumnarTableScan
          GroupBy| HASH GROUP BY,HASH JOIN
          GroupBy| SELECT STATEMENT,HASH GROUP BY
          GroupBy| GroupByPipe, ParallelUnion
          TableScan| Root, NewEENode, Sort
          Join| ExprEval, Join
          TableScan| Project, ExistingRdd
          TableScan| Project,ExistingRdd
          TableScan| Filter, ExistingRdd
          Planning| PRE-EXECUTION
          TableScan| SCAN HDFS
          GroupBy| Aggregate,Project,ExistingRdd
          GroupBy| Aggregate,Project,ShuffledHashJoin
          TableScan| STATISTICS COLLECTOR,TABLE ACCESS INMEMORY FULL
          TableScan| Sort, Root, NewEENode
          Join| Project,ShuffledHashJoin,Filter,InMemoryColumnarTableScan,
          GroupBy| HASH GROUP BY,NESTED LOOPS,HASH JOIN,JOIN FILTER CREATE
          Join| JOIN FILTER USE,TABLE ACCESS INMEMORY FULL,INDEX RANGE SCAN,TABLE ACCESS BY INDEX ROWID
          GroupBy| GroupByPipe, ExprEval
          GroupBy| VIEW,FILTER,HASH GROUP BY
          GroupBy| HASH GROUP BY,TABLE ACCESS INMEMORY FULL
          Join| HASH JOIN
          GroupBy| GroupByPipe, GroupByHash
          GroupBy| HASH GROUP BY,HASH JOIN ANTI
          GroupBy| ,Filter,Aggregate
          Join| Project,ShuffledHashJoin,Filter,ExistingRdd,
          Join| STATISTICS COLLECTOR,HASH JOIN BUFFERED,JOIN FILTER CREATE
          GroupBy| SELECT STATEMENT,VIEW,WINDOW BUFFER,HASH GROUP BY
          Join| HASH JOIN BUFFERED,JOIN FILTER CREATE
          TableScan| ExprEval
          Join| HASH JOIN,JOIN FILTER CREATE,TABLE ACCESS INMEMORY FULL,JOIN FILTER USE
          GroupBy| SELECT STATEMENT,HASH GROUP BY,HASH JOIN,JOIN FILTER CREATE,NESTED LOOPS,STATISTICS COLLECTOR,TABLE ACCESS INMEMORY FULL,JOIN FILTER USE,INDEX RANGE SCAN,TABLE ACCESS BY INDEX ROWID
          Planning| Pre-Execution
          Join| Project,ShuffledHashJoin,InMemoryColumnarTableScan
          TableScan| ,Filter,InMemoryColumnarTableScan
          GroupBy| ,TakeOrdered,Project,Aggregate
          TableScan| InMemoryColumnarTableScan,Project,Filter,
          GroupBy| PX SEND HASH,PX BLOCK ITERATOR,TABLE ACCESS INMEMORY FULL,SORT AGGREGATE,PX COORDINATOR,PX SEND QC (RANDOM)
          GroupBy| Aggregate,InMemoryColumnarTableScan,
          Join| CROSS JOIN, EXCHANGE, SCAN HDFS
          Exchange| EXCHANGE Pipelined
          TableScan| ExprEval, Sort, Root, NewEENode
          GroupBy| HASH GROUP BY,HASH JOIN ANTI,TABLE ACCESS INMEMORY FULL
          GroupBy| HASH GROUP BY,HASH JOIN ANTI,TABLE ACCESS INMEMORY FULL,SORT AGGREGATE
          GroupBy| HASH GROUP BY,HASH JOIN,TABLE ACCESS INMEMORY FULL
          Join| HASH JOIN ANTI
          Join| HASH JOIN ANTI,TABLE ACCESS INMEMORY FULL
          Join| HASH JOIN ANTI,TABLE ACCESS INMEMORY FULL,SORT AGGREGATE
          Join| HASH JOIN BUFFERED
          GroupBy| HASH JOIN, EXCHANGE, AGGREGATE, EXCHANGE, AGGREGATE, SCAN HDFS
          Join| HASH JOIN, EXCHANGE, SCAN HDFS
          Join| JOIN FILTER CREATE
          Join| JOIN FILTER USE,TABLE ACCESS INMEMORY FULL,HASH JOIN,JOIN FILTER CREATE
          Exchange| MERGING-EXCHANGE
          Planning| NewEENode, Root
          GroupBy| ParallelUnion, GroupByPipe
          GroupBy| ParallelUnion, GroupByPipe, GroupByHash, ExprEval
          GroupBy| ParallelUnion, Join, ExprEval
          GroupBy| PX SEND HASH,HASH GROUP BY,HASH JOIN ANTI
          TableScan| SCAN HDFS Pipelined
          TableScan| SELECT STATEMENT
          GroupBy| SELECT STATEMENT,HASH GROUP BY,HASH JOIN,FILTER
          GroupBy| SELECT STATEMENT,HASH GROUP BY,HASH JOIN,JOIN FILTER CREATE
          GroupBy| SELECT STATEMENT,HASH GROUP BY,HASH JOIN,JOIN FILTER CREATE,TABLE ACCESS INMEMORY FULL
          GroupBy| SELECT STATEMENT,HASH GROUP BY,TABLE ACCESS INMEMORY FULL
          Join| SELECT STATEMENT,HASH JOIN ANTI,TABLE ACCESS INMEMORY FULL
          Join| SELECT STATEMENT,HASH JOIN ANTI,TABLE ACCESS INMEMORY FULL,SORT AGGREGATE
          Join| SELECT STATEMENT,HASH JOIN,FILTER,HASH GROUP BY
          Join| SELECT STATEMENT,HASH JOIN,HASH GROUP BY
          GroupBy| SELECT STATEMENT,PX COORDINATOR,PX SEND QC (RANDOM),HASH GROUP BY
          GroupBy| SELECT STATEMENT,SORT AGGREGATE,TABLE ACCESS INMEMORY FULL
          GroupBy| SELECT STATEMENT,SORT ORDER BY,COUNT STOPKEY,VIEW,SORT GROUP BY STOPKEY
          TableScan| SELECT STATEMENT,TABLE ACCESS INMEMORY FULL
          GroupBy| SELECT STATEMENT,VIEW,WINDOW BUFFER,HASH GROUP BY,HASH JOIN,TABLE ACCESS INMEMORY FULL
          GroupBy| SELECT STATEMENT,WINDOW BUFFER,HASH GROUP BY,HASH JOIN,TABLE ACCESS INMEMORY FULL
          GroupBy| SELECT STATEMENT,WINDOW BUFFER,HASH GROUP BY,TABLE ACCESS INMEMORY FULL
          Exchange| Sort, NewEENode, Root
          Join| STATISTICS COLLECTOR,HASH JOIN,TABLE ACCESS INMEMORY FULL
          Join| STATISTICS COLLECTOR,TABLE ACCESS INMEMORY FULL,HASH JOIN BUFFERED
          TableScan| TABLE ACCESS INMEMORY FULL
          Join| TABLE ACCESS INMEMORY FULL,JOIN FILTER USE
          TableScan| TopK
          TableScan| TopK, ExprEval
          TableScan| TOP-N
          GroupBy| VIEW,HASH GROUP BY
          GroupBy| VIEW,HASH GROUP BY,TABLE ACCESS INMEMORY FULL
          GroupBy| VIEW,WINDOW BUFFER,HASH GROUP BY
          GroupBy| VIEW,WINDOW BUFFER,HASH GROUP BY,HASH JOIN
          GroupBy| VIEW,WINDOW BUFFER,HASH GROUP BY,TABLE ACCESS INMEMORY FULL
          GroupBy| WINDOW BUFFER,HASH GROUP BY,HASH JOIN,TABLE ACCESS INMEMORY FULL
          GroupBy| WINDOW BUFFER,HASH GROUP BY,TABLE ACCESS INMEMORY FULL
          Planning|
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

