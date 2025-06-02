/*
"Интеграция с внешними системами"

1. Необходимо создать внешние таблицы в Greenplum c использованием протокола PXF для доступа к данным следующих таблиц базы Postgres: 
gp.plan 
gp.sales 
2. Необходимо создать внешние таблицы в Greenplum c использованием протокола gpfdist для доступа к данным следующих файлов CSV: 
price 
chanel 
product 
region 
*/


--  Создание временной таблицы при помощи таблицы из другой базы

CREATE EXTERNAL TABLE adb.std11_26.plan_ext (
	date date,
	region varchar,
	matdirec varchar,
	quantity int4,
	distr_chan varchar
)
LOCATION (
	'pxf://gp.plan?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

drop external table adb.std11_26.plan_ext;

CREATE EXTERNAL TABLE adb.std11_26.sales_ext (
	"date" date,
	region varchar(20),
	material varchar(20),
	distr_chan varchar(100),
	quantity int4,
	check_nm varchar(100),
	check_pos varchar(100)
)
LOCATION (
	'pxf://gp.sales?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern&partition_by="date":date&range=2021-01-01:2022-01-01&interval=1:month'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' );

-- Создание временной таблицы при помощи чтения внешнего csv файла

drop external table adb.std11_26.chanel_ext;
--  Создание временной таблицы при помощи таблицы из другой базы

CREATE EXTERNAL TABLE adb.std11_26.plan_ext (
	date date,
	region varchar,
	matdirec varchar,
	quantity int4,
	distr_chan varchar
)
LOCATION (
	'pxf://gp.plan?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

drop external table adb.std11_26.plan_ext;

CREATE EXTERNAL TABLE adb.std11_26.sales_ext (
	"date" date,
	region varchar(20),
	material varchar(20),
	distr_chan varchar(100),
	quantity int4,
	check_nm varchar(100),
	check_pos varchar(100)
)
LOCATION (
	'pxf://gp.sales?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern&partition_by="date":date&range=2021-01-01:2022-01-01&interval=1:month'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' );

-- Создание временной таблицы при помощи чтения внешнего csv файла

drop external table adb.std11_26.chanel_ext;

create EXTERNAL TABLE std11_26.chanel_ext (
	distr_chan varchar,
	txtsh text
)
location ( 
	'gpfdist://172.16.128.142:8080/chanel.csv'
) on all
format 'csv' ( header delimiter ';' null '' escape '"' )
encoding 'UTF8'
segment reject limit 10 rows
;

select * from  std11_26.chanel_ext;

drop external table adb.std11_26.region_ext;


create EXTERNAL TABLE std11_26.region_ext (
	region varchar(4),
	txt text
)
location ( 
	'gpfdist://172.16.128.142:8080/region.csv'
) on all
format 'csv' ( delimiter ';' null '' escape '"' quote '"' )
encoding 'UTF8 '
segment reject limit 10 rows
;

select count(*) from  std11_26.region_ext;

-- внешняя таблица путем чтения плоских файлов

create writable EXTERNAL TABLE std11_26.price_write_ext ( like std11_26.price)
location ( 
	'gpfdist://172.16.128.142:8080/price_out.csv'
)
format 'csv' ( delimiter ';' null '' escape '"' quote '"' )
distributed randomly;

create EXTERNAL TABLE std11_26.chanel_ext (
	distr_chan varchar,
	txtsh text
)
location ( 
	'gpfdist://172.16.128.142:8080/chanel.csv'
) on all
format 'csv' ( header delimiter ';' null '' escape '"' )
encoding 'UTF8'
segment reject limit 10 rows
;

select * from  std11_26.chanel_ext;

drop external table adb.std11_26.region_ext;


create EXTERNAL TABLE std11_26.region_ext (
	region varchar(4),
	txt text
)
location ( 
	'gpfdist://172.16.128.142:8080/region.csv'
) on all
format 'csv' ( delimiter ';' null '' escape '"' quote '"' )
encoding 'UTF8 '
segment reject limit 10 rows
;

select count(*) from  std11_26.region_ext;

-- внешняя таблица путем чтения плоских файлов

create writable EXTERNAL TABLE std11_26.price_write_ext ( like std11_26.price)
location ( 
	'gpfdist://172.16.128.142:8080/price_out.csv'
)
format 'csv' ( delimiter ';' null '' escape '"' quote '"' )
distributed randomly;

