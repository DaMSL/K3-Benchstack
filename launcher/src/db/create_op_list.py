conn = db.getConnection()
cur = conn.cursor()
    
all_operations = '''
GroupBy| ParallelUnion, GroupByPipe, GroupByHash
GroupBy-Pipe| HASH GROUP BY,HASH JOIN,JOIN FILTER CREATE
GroupBy-Pipe| SELECT STATEMENT,VIEW,WINDOW BUFFER,HASH GROUP BY,HASH JOIN,JOIN FILTER CREATE,TABLE ACCESS INMEMORY FULL,JOIN FILTER USE
TableScan| ExistingRdd,
Join| Join
GroupBy-Pipe| Aggregate,Project
GroupBy| GroupByPipe
NetIO| COMM / OTHER
ExprEval| Root, Sort, NewEENode
GroupBy-Pipe| Aggregate,Project,Filter,InMemoryColumnarTableScan,
Join-Pipe| Project,ExistingRdd,ShuffledHashJoin
GroupBy| Aggregate
GroupBy-Pipe| Aggregate, Project, Filter, ExistingRdd
GroupBy-Pipe| ExprEval, GroupByPipe
TableScan| ExprEval, Scan
GroupBy-Pipe| TABLE ACCESS INMEMORY FULL,SORT AGGREGATE
Join-Pipe| Join, ExprEval
GroupBy-Pipe| ExprEval, Filter, GroupByPipe
Join-Pipe| HASH JOIN,JOIN FILTER CREATE
TableScan| Scan, ExprEval
GroupBy| ,Aggregate
ExprEval| Root, NewEENode
GroupBy-Pipe| Project,Filter,Aggregate,ShuffledHashJoin
Join| HASH JOIN
GroupBy| AGGREGATE
TableScan| InMemoryColumnarTableScan,Project,Filter
Join| CROSS JOIN
GroupBy| GroupByPipe, ParallelUnion, GroupByHash
Join-Pipe| Join, ParallelUnion, ExprEval
Join-Pipe| JOIN FILTER USE,TABLE ACCESS INMEMORY FULL
TableScan| Project,ExistingRdd,
TableScan| Scan
GroupBy-Pipe| GroupByHash, GroupByPipe, ParallelUnion, ExprEval
NetIO| EXCHANGE
TableScan| Project,Filter,InMemoryColumnarTableScan
GroupBy-Pipe| HASH GROUP BY,HASH JOIN
GroupBy| SELECT STATEMENT,HASH GROUP BY
GroupBy| GroupByPipe, ParallelUnion
ExprEval| Root, NewEENode, Sort
Join| ExprEval, Join
TableScan| Project, ExistingRdd
TableScan| Project,ExistingRdd
TableScan| Filter, ExistingRdd
PreExec| PRE-EXECUTION
TableScan| SCAN HDFS
GroupBy-Pipe| Aggregate,Project,ExistingRdd
GroupBy-Pipe| Aggregate,Project,ShuffledHashJoin
TableScan| STATISTICS COLLECTOR,TABLE ACCESS INMEMORY FULL
ExprEval| Sort, Root, NewEENode
Join-Pipe| Project,ShuffledHashJoin,Filter,InMemoryColumnarTableScan,
GroupBy-Pipe| HASH GROUP BY,NESTED LOOPS,HASH JOIN,JOIN FILTER CREATE
Join-Pipe| JOIN FILTER USE,TABLE ACCESS INMEMORY FULL,INDEX RANGE SCAN,TABLE ACCESS BY INDEX ROWID
GroupBy-Pipe| GroupByPipe, ExprEval
GroupBy-Pipe| VIEW,FILTER,HASH GROUP BY
GroupBy-Pipe| HASH GROUP BY,TABLE ACCESS INMEMORY FULL
Join| HASH JOIN
GroupBy| GroupByPipe, GroupByHash
GroupBy-Pipe| HASH GROUP BY,HASH JOIN ANTI
GroupBy| ,Filter,Aggregate
Join-Pipe| Project,ShuffledHashJoin,Filter,ExistingRdd,
Join-Pipe| STATISTICS COLLECTOR,HASH JOIN BUFFERED,JOIN FILTER CREATE
GroupBy-Pipe| SELECT STATEMENT,VIEW,WINDOW BUFFER,HASH GROUP BY
Join| HASH JOIN BUFFERED,JOIN FILTER CREATE
ExprEval| ExprEval
Join-Pipe| HASH JOIN,JOIN FILTER CREATE,TABLE ACCESS INMEMORY FULL,JOIN FILTER USE
GroupBy-Pipe| SELECT STATEMENT,HASH GROUP BY,HASH JOIN,JOIN FILTER CREATE,NESTED LOOPS,STATISTICS COLLECTOR,TABLE ACCESS INMEMORY FULL,JOIN FILTER USE,INDEX RANGE SCAN,TABLE ACCESS BY INDEX ROWID
PreExec| Pre-Execution
Join-Pipe| Project,ShuffledHashJoin,InMemoryColumnarTableScan
'''

olist = []
for o in all_operations.split('\n'):
  x = o.split('|')
  if len(x) == 2:
    olist.append((x[0], x[1].strip()))

for op in olist:
  qry = "INSERT INTO operator_names VALUES ('%s', '%s');" % ((op))
  cur.execute(qry)
  conn.commit()
