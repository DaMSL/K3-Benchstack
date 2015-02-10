LOAD DATA
infile "/local/data/tpch10g/region/region*"
  append into table region
  fields terminated by "|"
  (r_regionkey, r_name, r_comment)
