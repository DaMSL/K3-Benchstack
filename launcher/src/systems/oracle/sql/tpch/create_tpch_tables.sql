CREATE TABLE region
(
   r_regionkey integer NOT NULL,
   r_name character(25),
   r_comment character varying(152)
) INMEMORY;


CREATE TABLE nation
(
  n_nationkey integer NOT NULL,
  n_name character(25),
  n_regionkey integer,
  n_comment character varying(152)
) INMEMORY;

CREATE TABLE part
(
  p_partkey integer NOT NULL,
  p_name character varying(55),
  p_mfgr character(25),
  p_brand character(10),
  p_type character varying(25),
  p_size integer,
  p_container character(10),
  p_retailprice number(*,2),
  p_comment character varying(23)
) INMEMORY;


CREATE TABLE supplier
(
  s_suppkey integer NOT NULL,
  s_name character(25),
  s_address character varying(40),
  s_nationkey integer,
  s_phone character(15),
  s_acctbal number(*,2),
  s_comment character varying(101)
) INMEMORY;

CREATE TABLE customer
(
  c_custkey integer NOT NULL,
  c_name character varying(25),
  c_address character varying(40),
  c_nationkey integer,
  c_phone character(15),
  c_acctbal number(*,2),
  c_mktsegment character(10),
  c_comment character varying(117)
) INMEMORY;


CREATE TABLE orders
(
  o_orderkey integer NOT NULL,
  o_custkey integer,
  o_orderstatus character(1),
  o_totalprice number(*,2),
  o_orderdate date,
  o_orderpriority character(15),
  o_clerk character(15),
  o_shippriority integer,
  o_comment character varying(79)
) INMEMORY;


CREATE TABLE partsupp
(
  ps_partkey integer NOT NULL,
  ps_suppkey integer NOT NULL,
  ps_availqty integer,
  ps_supplycost number(*,2),
  ps_comment character varying(199)
) INMEMORY;

CREATE TABLE lineitem
(
  l_orderkey integer NOT NULL,
  l_partkey integer,
  l_suppkey integer,
  l_linenumber integer NOT NULL,
  l_quantity number,
  l_extendedprice number(*,2),
  l_discount number(*,2),
  l_tax number(*,2),
  l_returnflag character(1),
  l_linestatus character(1),
  l_shipdate date,
  l_commitdate date,
  l_receiptdate date,
  l_shipinstruct character(25),
  l_shipmode character(10),
  l_comment character varying(44)
) INMEMORY;
