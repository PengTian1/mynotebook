# Hive

## Overview

Hive is a SQL like abstraction that allows for interactive queries on top of Hadoop data.

Benefits:

- Familiar language
- No Java necessary
- Tool support

Limitations: 

- Not full ANSI SQL
- Slow
- Fragile

Other Options:

- Apache Spark(SQL, python, scale..)
- Impala(Cloudera) (All SQL, fast)
- Hive on Tez (fast)
- Presto (Full ansi sql)
- Third-party database(e.g. Vertica)

How Hive works?

 HDFS data -> (transform) HIve DW

| HDFS         | Hive DW     |
| ------------ | ----------- |
| Raw data     | Processed   |
| Unstructured | Structured  |
| Compressed   | Aggregated  |
| Partitioned  | Partitioned |



## Setup for demo

Download

- Virtual Box

​         https://www.virtualbox.org/wiki/Downloads

- CDH

​        https://downloads.cloudera.com/demo_vm/virtualbox/cloudera-quickstart-vm-5.12.0-0-virtualbox.zip

Install

1. install virtual box

2. import CDH

3. attach share folder

4. startup box

5. grant access to "cloudera"

   terminal -> sudo gedit /etc/group

   edit and save: 

   ```shell
   vboxsf:x:474:cloudera
   ```

   system->log out cloudera

   Language Manual

6. launch HUE UI

7. login with cloudera/cloudera



## Working with Data in Hiva

### Table Structure

Hive table is a defined table-like structure we can use to run SQL queries against; a table available in 2 main types: managed and external.

| Managed                             | External                |
| ----------------------------------- | ----------------------- |
| Hive owns data                      | Definition only         |
| Data no longer in original location | Points to files in HDFS |
| Format follows SQL conventions      | More fragile            |

### Create tables 

#### create with file

1. create table
2. import file

#### create with script

1. create table with HIVE QL

   ```sql
   create table sales_all_years (RowID smallint, OrderID int, OrderDate date, OrderMonthYear date, Quantity int, Quote float, DiscountPct float, Rate float, SaleAmount float, CustomerName string, CompanyName string, Sector string, Industry string, City string, ZipCode string, State string, Region string, ProjectCompleteDate date, DaystoComplete int, ProductKey string, ProductCategory string, ProductSubCategory string, Consultant string, Manager string, HourlyWage float, RowCount int, WageMargin float)
   partitioned by (yr int)
   row format serde 'com.bizo.hive.serde.csv.CSVSerde'
   stored as textfile;
   ```

   

2. upload files

   ![image-20210519135313758](C:\Users\tiapeng\AppData\Roaming\Typora\typora-user-images\image-20210519135313758.png)

   /user/hive/warehouse/<table_name>/<partition_name>/files

3. add partitions

   ```sql
   alter table sales_all_years
   add partition (yr=2009)
   location '2009/';
   
   alter table sales_all_years
   add partition (yr=2010)
   location '2010/';
   
   alter table sales_all_years
   add partition (yr=2011)
   location '2011/';
   
   alter table sales_all_years
   add partition (yr=2012)
   location '2012/';
   ```

   

4. browse table



## HIVE DDL

### Create 

```sql
-- create database
create database if not exists demo;

-- create table
create table clients (
    Name                string,
    Symbol              string,
    LastSale            double,
    MarketCapLabel      string,
    MarketCapAmount     bigint,
    IPOyear             int,
    Sector              string,
    industry            string,
    SummaryQuote        string
)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
stored as textfile;
```

### Alter

### Drop

### Truncate

### Comment

### Rename

### Show

```sql
show databases;

use <database>
show tables
```



## HIVE DML

reference:

https://cwiki.apache.org/confluence/display/Hive/LanguageManual

https://www.linkedin.com/learning/analyzing-big-data-with-hive

https://data-flair.training/blogs/apache-hive-tutorial/

### Aggregate Data

#### avg(col)

#### count([DISTINCT] col)

```sql
select count(1)
from sales_all_years;
```

#### sum(col)

#### max(col)

#### min(col)

stddev_pop(col)

stedev_samp(col)

corr(col1,col2)

covar_pop(col1,col2)

covar_samp(col1,col2)

collect_set(col)

collect_list(col)

array<struct{'x','y'}>histogram_numeric(col, b)

ntile(INT x)

### Filtering Results

#### WHERE Clause

equals

```sql
select  *
from    sales_all_years
where   yr=2009;

select  *
from    sales_all_years
where   yr!=2009;
```

limit

```sql
select  *
from    sales_all_years
where   yr = 2009
and     lower(rowid) != 'rowid'
limit   1000;
```

between.. and ..

```sql
select  *
from    sales_all_years
where   orderdate between '2010-01-01' and '2010-12-31'
```



#### HAVING clause

HAVING allows you to focus your result set, after you have performed an aggregation.

```sql
select 
    productcategory,
    productsubcategory,
    productkey,
    sum(saleamount) as TotalSales
from sales_all_years
where
    Yr=2009
group by 
    productcategory, 
    productsubcategory, 
    productkey
having
    sum(saleamount) > 100000
limit 1000;
```



#### LIKE

```sql
SELECT DISTINCT order_items.name
FROM customers c
LATERAL VIEW EXPLODE(c.orders) o AS ords
LATERAL VIEW EXPLODE(ords.items) i AS order_items
WHERE lower(order_items.name) like '%table%'
LIMIT 1000;
```



### Joining Tables

Hive only support equality joins, EQUI JOIN, using the equals(=) operator.

#### Join Types

- Inner Join (JOIN)

```sql
select count(1)
from sales_all_years s
join clients c on s.companyname = c.name
```

- Left Join

```sql
SELECT c.ID, c.NAME, o.AMOUNT, o.DATE
FROM CUSTOMERS c
LEFT OUTER JOIN ORDERS o
ON (c.ID = o.CUSTOMER_ID);
```

- Right join

```sql
SELECT c.ID, c.NAME, o.AMOUNT, o.DATE 
FROM CUSTOMERS c 
RIGHT OUTER JOIN ORDERS o ON (c.ID = o.CUSTOMER_ID);
```

- Full Outer Join

```sql
SELECT c.ID, c.NAME, o.AMOUNT, o.DATE
FROM CUSTOMERS c
FULL OUTER JOIN ORDERS o
ON (c.ID = o.CUSTOMER_ID);
```



#### EXISTS = LEFF SEMI JOIN

```SQL
-- get info about vip customers only

select *
from sales_all_years s
where exists(
    select *
    from vip_clients v
    where s.companyname = v.name);

select 
    s.companyname,
    s.productcategory,
    count(distinct s.orderid) OrderCount,
    sum(s.saleamount) as TotalSales
from sales_all_years s
left semi join vip_clients v on (s.companyname = v.name) 
group by
    s.companyname,
    s.productcategory
```



#### Join Multiple Tables

```sql
select a.val, b.val, c.val
from a
join b on (a.key = b.key)
left outer join c on (a.key=c.key)
```



#### Issue

Error: [FAILED: Execution Error, return code 2 from org.apache.hadoop.hive.ql.exec.mr.MapredLocalTask during join operation in hive

solution:  https://stackoverflow.com/questions/50602163/failed-execution-error-return-code-2-from-org-apache-hadoop-hive-ql-exec-mr-ma/50619601

```sql
set hive.auto.convert.join=false;
select count(1)
from sales_all_years s
join clients c on s.companyname = c.name;
```



## Hive Functions

### String Functions

#### concat()

combine strings



#### lower()

convert the case of string to lower case

```sql
select 
    lower(name), 
    regexp_replace(name, '[^a-zA-Z0-9]+', ''),
    lower(regexp_replace(name, '[^a-zA-Z0-9]+', '')) as CustKey
from clients
```

![image-20210519171010226](C:\Users\tiapeng\AppData\Roaming\Typora\typora-user-images\image-20210519171010226.png)

#### substr()

pull out sections of a string

#### trim()

remove the leading or trailing space

#### regexp_replace()

```sql
select 
    lower(name), 
    regexp_replace(name, '[^a-zA-Z0-9]+', ''),
    lower(regexp_replace(name, '[^a-zA-Z0-9]+', '')) as CustKey
from clients
```



### Math Functions

#### round()

reduce that down to the specification provided

```sql
-- basic calculations using mathmatical operators
select
    quantity,
    rate,
    discountpct,
    quantity*rate*(1-discountpct) as QuoteAmt,
    round(quantity*rate*(1-discountpct)) as QuoteAmtRound
from sales_all_years
where yr=2009
```

| quantity | rate | discountpct | QuoteAmt       | QuoteAmtTound |
| -------- | ---- | ----------- | -------------- | ------------- |
| 3        | 160  | 0.06        | 451.1999999999 | 451           |

##### floor()

round down: 0.7 -> 0

##### ceiling()

round up: 0.7 -> 1

#### rand()

generate a random number between 0 and 1

```sql
select 
    rand(), 
    saleamount,
    wagemargin,
    round(saleamount*rand()) as RandSaleAmount, -- a seperate random number
    floor(wagemargin) as WageMarginFlr,
    ceiling(wagemargin) as WageMarginCl
from sales_all_years
where yr=2009
```

| _c0                 | saleamount | wagemargin | RandSaleAmount | WageMarginFlr | WageMarginCl |
| ------------------- | ---------- | ---------- | -------------- | ------------- | ------------ |
| 0.85810111181854798 | 1656       | 0.7        | 842            | 0             | 1            |
| 0.2594255353710806  | 5760       | 0.35       | 3116           | 0             | 1            |



#### sqrt()



#### abs()



### Date Functions

#### to_date()

convert a string that looks like a date into an actual date value 

#### year(), month(), day()

pull out parts of the date. retrieve just year, month or day

```sql
select
    productcategory,
    productsubcategory,
    year(orderdate) y,
    month(orderdate) m,
    avg(datediff(projectcompletedate, orderdate)) duration
from sales_all_years
group by    
    productcategory,
    productsubcategory,
    year(orderdate),
    month(orderdate)
order by
    3,4
```

| productcategory | productsubcategory | y    | m    | duration |
| --------------- | ------------------ | ---- | ---- | -------- |
| Training        | Tableau            | 2012 | 12   | 2        |
| Training        | Tableau            | 2012 | 10   | 2        |
| Training        | Tableau            | 2011 | 10   | 1        |



#### datediff()

calculate days between 2 dates

```sql
select
    orderdate,
    projectcompletedate,
    datediff(projectcompletedate, orderdate) duration
from sales_all_years
```

| orderdate  | projectcompletedate | duration |
| ---------- | ------------------- | -------- |
| 2009-01-01 | 2009-01-03          | 2        |



#### date_add(), date_sub()

add or subtract days from a date value

```sql
-- find the last day of the month
select distinct
	date_sub(
		to_date(
			concat(cast(year(orderdate) as string),"-",cast(month(orderdate)+1 as string),"-01")
		)
	,1 ),
	orderdate
from sales_all_years
limit 100;
```

| _c0        | orderdate  |
| ---------- | ---------- |
| 2009-01-31 | 2009-01-10 |
| 2009-02-28 | 2009-02-10 |



### Conditional Functions

#### if()

```sql
-- identify large sales with IF
-- if saleamount > 5000, return 1; else return 0
select 
    orderid,
    saleamount,
    if(saleamount > 5000, 1, 0) as LargeSale 
from sales_all_years
limit 1000;
```

| orderid | saleamount | LargeSale |
| ------- | ---------- | --------- |
| 13729   | 1656       | 0         |
| 13730   | 6080.75    | 1         |



#### coalesce()

returns the first non-null value in a list of values



#### case...when

```sql
-- create sales size categories
select 
    orderid,
    saleamount,
    case 
        when saleamount > 5000 then 'large'
        when saleamount > 1000 then 'medium'
        else 'small'
    end as SalesSize
from sales_all_years
limit 1000;
```

| orderid | saleamount | SalesSize |
| ------- | ---------- | --------- |
| 1       | 6000       | large     |
| 2       | 3000       | medium    |
| 3       | 451.2      | small     |



```sql
-- perform what-if analysis by reassigning regions
select 
    case lower(region)
        when 'west' then 'Southwest'
        when 'south' then 'Southwest'
        else region
    end as new_region,
    year(orderdate) as y,
    sum(saleamount) as TotalSales
from sales_all_years

group by
    case lower(region)
        when 'west' then 'Southwest'
        when 'south' then 'Southwest'
        else region
    end,
    year(orderdate)

limit 100;
```

| new_region | y    | TotalSales         |
| ---------- | ---- | ------------------ |
| Central    | 2009 | 2355350.6000000015 |
| East       | 2009 | 1583738.5000000005 |
| Southwest  | 2009 | 3421296.0000000007 |



## Next Course

Foundations of Programming: Databases

SQL Essential Training

SQL for Data Reporting and Analysis
