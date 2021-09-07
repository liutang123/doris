select count(distinct lo_partkey) from lineorder where lo_year=1994 and lo_discount between 5 and 7 and lo_quantity between 26 and 35
select count(distinct lo_partkey_bitmap) from lineorder where lo_year=1994 and lo_discount between 5 and 7 and lo_quantity between 26 and 35
select count(distinct lo_partkey_hll) from lineorder where lo_year=1994 and lo_discount between 5 and 7 and lo_quantity between 26 and 35
