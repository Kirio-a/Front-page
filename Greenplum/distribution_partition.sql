
-- distribution - разделение данных по сегментам
-- partition - разбитие данных на куски по определенному признаку

truncate table std11_26.table4;

insert into std11_26.table4
select 1
	, md5(1::text);

-- Constraint (ограничения)

select gp_segment_id
	, count(1)
from std11_26.table4
group by 1;

create table std11_26.table5
(
	field1 int
	, field2 text not null
)
distributed by (field1);

insert into std11_26.table5
select a
	, null
from generate_series(1, 2000) a;

create table std11_26.table6
(
	field1 int
	, field2 text default 'hello'
)
distributed by (field1);

insert into std11_26.table6
select a
from generate_series(1, 2000) a;

select * from std11_26.table6;

create table std11_26.table7
(
	field1 int primary key
	, field2 text
)
distributed by (field1);

insert into std11_26.table7(field1, field2)
values(1, 'PK test');

insert into std11_26.table7(field1, field2)
values(1, 'PK test 2');

create table std11_26.table8
(
	field1 int check(field1 > 10)
	, field2 text
)
distributed by (field1);

insert into std11_26.table7(field1, field2)
values(1, 'check test');

-- Partition (партиционирование)

-- Partition by range
-- date

create table std11_26.sales
(
	id int
	, dt date
	, amt decimal(10,2)
)
distributed by (id)
partition by range (dt)
(
	start (date '2016-01-01')inclusive
	end (date '2017-01-01')exclusive
	every (interval '1 month')
);

insert into std11_26.sales
values(1, '2016-01-02'::date, 134);

insert into std11_26.sales
values(2, '2016-10-02'::date, 145);

select * from std11_26.sales;

select * from std11_26.sales_1_prt_1;

select * from std11_26.sales_1_prt_10;

insert into std11_26.sales
values(3, '2018-01-02'::date, 205);

drop table std11_26.sales;

create table std11_26.sales
(
	id int
	, dt date
	, amt decimal(10,2)
)
distributed by (id)
partition by range (dt)
(
	start (date '2016-01-01')inclusive
	end (date '2017-01-01')exclusive
	every (interval '1 month'),
	default partition def
);

insert into std11_26.sales
values(3, '2018-01-02'::date, 205);

select * from std11_26.sales_1_prt_def;

-- Вырезать партицию из дефолтной в общий список партиций

alter table std11_26.sales
split default partition
	start('2018-01-01')
	end('2018-02-01') exclusive;

-- Полный список партиций

select
	partitiontablename
	, partitionrangestart
	, partitionrangeend
from pg_partitions
where 
	tablename = 'sales'
	and schemaname = 'std11_26';

-- Создание партиции с помощью временной таблицы
create table std11_26.sales_2016
(
	id int
	, dt date
	, amt decimal(10,2)
)
distributed by (id);

truncate table std11_26.sales;

insert into std11_26.sales_2016
values(1, '2016-10-02'::date, 145);

alter table std11_26.sales
exchange partition for (date '2016-10-01' )
with table std11_26.sales_2016
with validation;

select * from std11_26.sales;

-- partition by range integer

create table std11_26.rank
(
	id int
	, rank int
	, year int
	, gender char(1)
	, count int
)
distributed by (id)
partition by range (year)
(
	start (2006)
	end (2016)
	every (1)
	, default partition extra
);

select
	partitiontablename
	, partitionrangestart
	, partitionrangeend
from pg_partitions
where 
	tablename = 'rank'
	and schemaname = 'std11_26';

-- partition by list

create table std11_26.list
(
	id int
	, rank int
	, year int
	, gender char(1)
	, count int
)
distributed by (id)
partition by list(gender)
(
	partition girls values ('f')
	, partition boys values ('m')
	, default partition other
);

select
	partitiontablename
	, partitionrangestart
	, partitionrangeend
from pg_partitions
where 
	tablename = 'list'
	and schemaname = 'std11_26';


-- Транзакции

begin;
	update ....;
	update ...;
commit;































