conn = db.getConnection()
cur = conn.cursor()
    
all_operations = '''
GroupBy| ParallelUnion, GroupByPipe, GroupByHash
PipelinedGroupBy| HASH GROUP BY,HASH JOIN,JOIN FILTER CREATE
PipelinedGroupBy| SELECT STATEMENT,VIEW,WINDOW BUFFER,HASH GROUP BY,HASH JOIN,JOIN FILTER CREATE,TABLE ACCESS INMEMORY FULL,JOIN FILTER USE
TableScan| ExistingRdd,
Join| Join
PipelinedGroupBy| Aggregate,Project
GroupBy| GroupByPipe
NetIO| COMM / OTHER
ExprEval| Root, Sort, NewEENode
PipelinedGroupBy| Aggregate,Project,Filter,InMemoryColumnarTableScan,
PipelinedJoin| Project,ExistingRdd,ShuffledHashJoin
GroupBy| Aggregate
PipelinedGroupBy| Aggregate, Project, Filter, ExistingRdd
PipelinedGroupBy| ExprEval, GroupByPipe
TableScan| ExprEval, Scan
PipelinedGroupBy| TABLE ACCESS INMEMORY FULL,SORT AGGREGATE
PipelinedJoin| Join, ExprEval
PipelinedGroupBy| ExprEval, Filter, GroupByPipe
PipelinedJoin| HASH JOIN,JOIN FILTER CREATE
TableScan| Scan, ExprEval
GroupBy| ,Aggregate
ExprEval| Root, NewEENode
PipelinedGroupBy| Project,Filter,Aggregate,ShuffledHashJoin
GroupBy| AGGREGATE
TableScan| InMemoryColumnarTableScan,Project,Filter
Join| CROSS JOIN
GroupBy| GroupByPipe, ParallelUnion, GroupByHash
PipelinedJoin| Join, ParallelUnion, ExprEval
PipelinedJoin| JOIN FILTER USE,TABLE ACCESS INMEMORY FULL
TableScan| Project,ExistingRdd,
TableScan| Scan
PipelinedGroupBy| GroupByHash, GroupByPipe, ParallelUnion, ExprEval
NetIO| EXCHANGE
TableScan| Project,Filter,InMemoryColumnarTableScan
PipelinedGroupBy| HASH GROUP BY,HASH JOIN
GroupBy| SELECT STATEMENT,HASH GROUP BY
GroupBy| GroupByPipe, ParallelUnion
ExprEval| Root, NewEENode, Sort
Join| ExprEval, Join
TableScan| Project, ExistingRdd
TableScan| Project,ExistingRdd
TableScan| Filter, ExistingRdd
PreExec| PRE-EXECUTION
TableScan| SCAN HDFS
PipelinedGroupBy| Aggregate,Project,ExistingRdd
PipelinedGroupBy| Aggregate,Project,ShuffledHashJoin
TableScan| STATISTICS COLLECTOR,TABLE ACCESS INMEMORY FULL
ExprEval| Sort, Root, NewEENode
PipelinedJoin| Project,ShuffledHashJoin,Filter,InMemoryColumnarTableScan,
PipelinedGroupBy| HASH GROUP BY,NESTED LOOPS,HASH JOIN,JOIN FILTER CREATE
PipelinedJoin| JOIN FILTER USE,TABLE ACCESS INMEMORY FULL,INDEX RANGE SCAN,TABLE ACCESS BY INDEX ROWID
PipelinedGroupBy| GroupByPipe, ExprEval
PipelinedGroupBy| VIEW,FILTER,HASH GROUP BY
PipelinedGroupBy| HASH GROUP BY,TABLE ACCESS INMEMORY FULL
Join| HASH JOIN
GroupBy| GroupByPipe, GroupByHash
PipelinedGroupBy| HASH GROUP BY,HASH JOIN ANTI
GroupBy| ,Filter,Aggregate
PipelinedJoin| Project,ShuffledHashJoin,Filter,ExistingRdd,
PipelinedJoin| STATISTICS COLLECTOR,HASH JOIN BUFFERED,JOIN FILTER CREATE
PipelinedGroupBy| SELECT STATEMENT,VIEW,WINDOW BUFFER,HASH GROUP BY
Join| HASH JOIN BUFFERED,JOIN FILTER CREATE
ExprEval| ExprEval
PipelinedJoin| HASH JOIN,JOIN FILTER CREATE,TABLE ACCESS INMEMORY FULL,JOIN FILTER USE
PipelinedGroupBy| SELECT STATEMENT,HASH GROUP BY,HASH JOIN,JOIN FILTER CREATE,NESTED LOOPS,STATISTICS COLLECTOR,TABLE ACCESS INMEMORY FULL,JOIN FILTER USE,INDEX RANGE SCAN,TABLE ACCESS BY INDEX ROWID
PreExec| Pre-Execution
PipelinedJoin| Project,ShuffledHashJoin,InMemoryColumnarTableScan
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
