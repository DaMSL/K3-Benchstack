load data
infile "/mddb/data/tpch100g/nation/*"
 into table nation
 fields terminated by "|"
 (n_nationkey, n_name, n_regionkey, n_comment)
