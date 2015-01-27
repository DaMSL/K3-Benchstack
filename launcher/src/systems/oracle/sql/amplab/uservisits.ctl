OPTIONS(DIRECT=TRUE,ROWS=1000000)
load data
infile "/mnt/data/1024/uservisits/*"
 into table uservisits
 fields terminated by ","
 (sourceIP, destURL, visitDate date "YYYY-MM-DD", adRevenue, userAgent, coutryCode, languageCode, searchWord, duration)
