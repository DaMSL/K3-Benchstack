OPTIONS(DIRECT=TRUE,ROWS=1000000)
load data
infile "/mnt/data/1024/rankings/*"
 into table rankings
 fields terminated by ","
 (pageURL, pageRank, avgDuration)
