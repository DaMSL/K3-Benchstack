CREATE TABLE IF NOT EXISTS region
(
   r_regionkey integer NOT NULL,
   r_name character(25),
   r_comment character varying(152),
   CONSTRAINT pkey_region PRIMARY KEY (r_regionkey)
) INMEMORY;


CREATE TABLE IF NOT EXISTS nation
(
  n_nationkey integer NOT NULL,
  n_name character(25),
  n_regionkey integer,
  n_comment character varying(152),
  CONSTRAINT pkey_nation PRIMARY KEY (n_nationkey)
) INMEMORY;

CREATE TABLE IF NOT EXISTS part
(
  p_partkey integer NOT NULL,
  p_name character varying(55),
  p_mfgr character(25),
  p_brand character(10),
  p_type character varying(25),
  p_size integer,
  p_container character(10),
  p_retailprice numeric,
  p_comment character varying(23),
  CONSTRAINT pkey_part PRIMARY KEY (p_partkey)
) INMEMORY;


CREATE TABLE IF NOT EXISTS supplier
(
  s_suppkey integer NOT NULL,
  s_name character(25),
  s_address character varying(40),
  s_nationkey integer,
  s_phone character(15),
  s_acctbal numeric,
  s_comment character varying(101),
  CONSTRAINT pkey_supplier PRIMARY KEY (s_suppkey)
) INMEMORY;

CREATE TABLE IF NOT EXISTS customer
(
  c_custkey integer NOT NULL,
  c_name character varying(25),
  c_address character varying(40),
  c_nationkey integer,
  c_phone character(15),
  c_acctbal numeric,
  c_mktsegment character(10),
  c_comment character varying(117),
  CONSTRAINT pkey_customer PRIMARY KEY (c_custkey)
) INMEMORY;


CREATE TABLE IF NOT EXISTS orders
(
  o_orderkey integer NOT NULL,
  o_custkey integer,
  o_orderstatus character(1),
  o_totalprice numeric,
  o_orderdate date,
  o_orderpriority character(15),
  o_clerk character(15),
  o_shippriority integer,
  o_comment character varying(79),
  CONSTRAINT pkey_orders PRIMARY KEY (o_orderkey)
) INMEMORY;


CREATE TABLE IF NOT EXISTS partsupp
(
  ps_partkey integer NOT NULL,
  ps_suppkey integer NOT NULL,
  ps_availqty integer,
  ps_supplycost numeric,
  ps_comment character varying(199),
  CONSTRAINT pkey_partsupp PRIMARY KEY (ps_partkey, ps_suppkey)
) INMEMORY;

