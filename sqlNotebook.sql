-- Databricks notebook source exported at Sun, 13 Mar 2016 19:55:00 UTC
-- MAGIC %md sql notebook

-- COMMAND ----------

DROP TABLE IF EXISTS allData0_400;
CREATE TABLE allData0_400 ( sensor_id VARCHAR(100), timeseries VARCHAR(100), values VARCHAR(100), room VARCHAR(100), template VARCHAR(100));
insert into allData0_400 select * from data0_100 union select * from data100_200 union select * from data200_300 union select * from data300_400;

-- COMMAND ----------

select count(*) from alldatabuildingsensors;

-- COMMAND ----------

DROP TABLE IF EXISTS rm4126;
CREATE TABLE rm4126 ( sensor_id VARCHAR(100), timeseries VARCHAR(100), values VARCHAR(100), room VARCHAR(100), template VARCHAR(100));
insert into rm4126 select * from alldatatemplatecorrected where room='RM-4126' and values is not null;

-- COMMAND ----------

DROP TABLE IF EXISTS rm4256;
CREATE TABLE rm4256 ( sensor_id VARCHAR(100), timeseries VARCHAR(100), values VARCHAR(100), room VARCHAR(100), template VARCHAR(100));
insert into rm4256 select * from alldatatemplatecorrected where room='Rm-4256';

-- COMMAND ----------

DROP TABLE IF EXISTS rm3214;
CREATE TABLE rm3214 ( sensor_id VARCHAR(100), timeseries VARCHAR(100), values VARCHAR(100), room VARCHAR(100), template VARCHAR(100));
insert into rm3214 select * from alldatatemplatecorrected where room='Rm-3214' and values is not null;

-- COMMAND ----------

DROP TABLE IF EXISTS rm3208;
CREATE TABLE rm3208 ( sensor_id VARCHAR(100), timeseries VARCHAR(100), values VARCHAR(100), room VARCHAR(100), template VARCHAR(100));
insert into rm3208 select * from alldatatemplatecorrected where room='Rm-3208' and values is not null;

-- COMMAND ----------

DROP TABLE IF EXISTS rm2234;
CREATE TABLE rm2234 ( sensor_id VARCHAR(100), timeseries VARCHAR(100), values VARCHAR(100), room VARCHAR(100), template VARCHAR(100));
insert into rm2234 select * from alldatatemplatecorrected where room='Rm-2234' and values is not null;

-- COMMAND ----------

DROP TABLE IF EXISTS rm2238;
CREATE TABLE rm2238 ( sensor_id VARCHAR(100), timeseries VARCHAR(100), values VARCHAR(100), room VARCHAR(100), template VARCHAR(100));
insert into rm2238 select * from alldatatemplatecorrected where room='Rm-2238' and values is not null;

-- COMMAND ----------

DROP TABLE IF EXISTS rm1117;
CREATE TABLE rm1117 ( sensor_id VARCHAR(100), timeseries VARCHAR(100), values VARCHAR(100), room VARCHAR(100), template VARCHAR(100));
insert into rm1117 select * from alldatatemplatecorrected where room='RM-1117' and values is not null;

-- COMMAND ----------

select * from alldatatemplatecorrected where room='AH1-1' and template='Discharge Air Temp 1' and timeseries between '2014-04-01' and '2014-04-14';

-- COMMAND ----------

select * from summary4226;

-- COMMAND ----------

insert into data100_200 select * from data0_100;

-- COMMAND ----------

insert into data100_200 select * from data400_500;

-- COMMAND ----------

DROP TABLE IF EXISTS allDataTemplateCorrected;
CREATE TABLE allDataTemplateCorrected ( sensor_id VARCHAR(100), timeseries VARCHAR(100), values VARCHAR(100), room VARCHAR(100), template VARCHAR(100));
insert into allDataTemplateCorrected select d.sensor_id, d.timeseries, d.values, d.room, t.template from data100_200 as d join tags as t on d.sensor_id = t.sensor_id; 

-- COMMAND ----------

select *, row_number() over(partition by room,template order by timeseries) as rn from rm2118;

-- COMMAND ----------

DROP TABLE IF EXISTS testSummary;
CREATE TABLE testSummary
(
  room varchar(100),
  template varchar(100),
  time_st varchar(100),
  time_end varchar(100)
);
with cte as (select *, row_number() over(partition by room,template order by timeseries) as rn from allbuildingsensordata)
insert into testSummary
select
  C1.room,
  C1.template,
  C1.timeseries as time_st,
  C2.timeseries as time_end
from cte as C1
  inner join cte as C2
    on C1.rn = C2.rn-1 and
       C1.room = C2.room and
       C1.template = C2.template and
       datediff(C2.timeseries, C1.timeseries) > 1;

-- COMMAND ----------

select datediff('2013-07-04T17:00:06+00:00','2013-07-11T13:05:03+00:00')

-- COMMAND ----------

select * from testSummary;

-- COMMAND ----------

DROP TABLE IF EXISTS summary5days;
CREATE TABLE summary5days
(
  room varchar(100),
  template varchar(100),
  time_st varchar(100),
  time_end varchar(100)
);
with cte as 
(
  select *, row_number() over(partition by room,template order by timeseries) as rn
  from alldatatemplatecorrected
)
insert into summary5days
select
  C1.room,
  C1.template,
  C1.timeseries as time_st,
  C2.timeseries as time_end
from cte as C1
  inner join cte as C2
    on C1.rn = C2.rn-1 and
       C1.room = C2.room and
       C1.template = C2.template and
       datediff(C2.timeseries, C1.timeseries) > 5

-- Build islands from gaps in @T
;

with cte1 as
(
  -- Add first and last timestamp to gaps
  select room, template, time_end, time_st from summary5days
  union all
  select room, template, max(timeseries) as time_end, null as time_st from alldatatemplatecorrected group by room, template
  union all
  select room, template, null as time_end, min(timeseries) as time_st from alldatatemplatecorrected group by room, template
),
cte2 as
(
  select *,
    row_number() over(partition by room,template order by time_end) as rn
  from cte1
)
insert into summary5days
select
  C1.room,
  C1.template,
  C2.time_end as PeriodEnd,
  C1.time_st as PeriodStart
from cte2 as C1
  inner join cte2 as C2
    on C1.room = C2.room and
      C1.template = C2.template and
       C1.rn = C2.rn-1
order by C1.room, C1.time_st;

-- COMMAND ----------

select * from summary5days;

-- COMMAND ----------

select * from summary;

-- COMMAND ----------

select count(*) from summary;

-- COMMAND ----------

select distinct room, template from allDataBuildingSensorsNew order by room;

-- COMMAND ----------

select room, template, COUNT(*) from AllBuildingSensorData group by room, template;

-- COMMAND ----------

select COUNT(DISTINCT room) from alldatabuildingsensorsnew;

-- COMMAND ----------

select distinct room from alldatabuildingsensors;

-- COMMAND ----------

select COUNT(DISTINCT template) from alldatabuildingsensorsnew2;

-- COMMAND ----------

select DISTINCT template from alldatabuildingsensorsnew2;

-- COMMAND ----------

select * from alldatabuildingsensorsnew where room='HW-SYS';

-- COMMAND ----------

select count(distinct sensor_id) from alldatabuildingsensorsNew2;

-- COMMAND ----------

select * from alldatabuildingsensors where room='RM-1145' and template='Common Setpoint' and timeseries between '2014-01-01T00:00:00+00:00' and '2014-01-14T00:00:00+00:00';

-- COMMAND ----------

select count(*) from AllBuildingSensorData;

-- COMMAND ----------

show alldatabuildingsensors schema;

-- COMMAND ----------


