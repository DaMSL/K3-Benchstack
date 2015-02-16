load data
infile "/mddb/data/tpch100g/part/*"
 into table part
 fields terminated by "|"
 (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)
