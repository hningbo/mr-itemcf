# mr-itemcf


hive:



1)查询2014年12月10日到2014年12月13日有多少人浏览了商品
 select count(*) from full_shopping where time >= to_date('2014-12-10') and time <= to_date('2014-12-13');
 OK
 3855684
 
2)以天为统计单位，依次显示每天网站卖出去的商品的个数
 select time,count(*) count from full_shopping group by time order by time;
 
 OK
 2014-11-18      684628
 2014-11-19      687528
 2014-11-20      672189
 2014-11-21      634122
 2014-11-22      668509
 2014-11-23      722978
 2014-11-24      718217
 2014-11-25      699413
 2014-11-26      679323
 2014-11-27      689855
 2014-11-28      658806
 2014-11-29      684442
 2014-11-30      751093
 2014-12-01      744363
 2014-12-02      753810
 2014-12-03      788689
 2014-12-04      745391
 2014-12-05      693593
 2014-12-06      732821
 2014-12-07      763498
 2014-12-08      753138
 2014-12-09      767838
 2014-12-10      788712
 2014-12-11      944979
 2014-12-12      1344980
 2014-12-13      777013
 2014-12-14      779285
 2014-12-15      764085
 2014-12-16      751370
 2014-12-17      734520
 2014-12-18      711839