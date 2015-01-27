SELECT /* MONITOR PARALLEL */sourceIP, totalRevenue, avgPageRank
FROM
  (SELECT sourceIP,
          AVG(pageRank) as avgPageRank,
          SUM(adRevenue) as totalRevenue
   FROM rankings R, uservisits UV
   WHERE R.pageURL = UV.destURL
     AND UV.visitDate > Date '1980-01-01'
     AND UV.visitDate < Date '1980-04-01'
   GROUP BY UV.sourceIP) T
WHERE
  rownum = 1
ORDER BY totalRevenue DESC

