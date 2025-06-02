"Дистрибуция. Создание таблиц"

1. Необходимо создать таблицы в Greenplum в схеме std<номер пользователя> со структурами следующих таблиц из базы Postgres: 

gp.plan 
gp.sales 
Для таблиц фактов необходимо определить поля и задать параметры: ориентацию таблиц, сжатие, правила дистрибуции и задать партиционирование, если это необходимо.  

2. Необходимо создать таблицы в Greenplum в схеме std<номер пользователя> со структурами следующих файлов CSV: 
price 
chanel 
product 
region 
Для таблицы справочников данные необходимо будет реплицировать на каждый сегмент.
*/


-- std11_26.plan определение

-- DROP TABLE std11_26.plan;

CREATE TABLE std11_26.plan (
	"date" date NULL,
	region varchar(20) NULL,
	matdirec varchar(20) NULL,
	quantity int4 NULL,
	distr_chan varchar(100) NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE(date) 
          (
          START ('2021-01-01'::date) 
          END ('2022-01-01'::date) 
          EVERY ('1 mon'::interval), 
          DEFAULT PARTITION others
          );

-- std11_26.sales определение

-- DROP TABLE std11_26.sales;

CREATE TABLE std11_26.sales (
	"date" date NULL,
	region varchar(20) NULL,
	material varchar(20) NULL,
	distr_chan varchar(100) NULL,
	quantity int4 NULL,
	check_nm varchar(100) NOT NULL,
	check_pos varchar(100) NOT NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (check_nm)
PARTITION BY RANGE(date) 
          (
          START ('2021-01-01'::date) END ('2022-01-01'::date) EVERY ('1 mon'::interval), 
          DEFAULT PARTITION def
          );

-- std11_26.chanel определение

-- DROP TABLE std11_26.chanel;

CREATE TABLE std11_26.chanel (
	distr_chan varchar(1) NULL,
	txtsh text NULL
)
DISTRIBUTED REPLICATED;

-- std11_26.product определение

-- DROP TABLE std11_26.product;

CREATE TABLE std11_26.product (
	material int4 NULL,
	asgrp int4 NULL,
	brand int4 NULL,
	matcateg varchar(4) NULL,
	matdirec int4 NULL,
	txt text NULL
)
WITH (
	appendonly=true,
	orientation=row,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (material);

-- std11_26.region определение

-- DROP TABLE std11_26.region;

CREATE TABLE std11_26.region (
	region varchar(4) NULL,
	txt text NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (region);

-- std11_26.price определение

-- DROP TABLE std11_26.price;

CREATE TABLE std11_26.price (
	material int4 NOT NULL,
	region varchar(4) NOT NULL,
	distr_chan varchar(1) NOT NULL,
	price int4 NULL
)
WITH (
	appendonly=true,
	orientation=row,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED RANDOMLY
PARTITION BY LIST(region) 
          (
          PARTITION m VALUES('R001'), 
          PARTITION sp VALUES('R002'), 
          PARTITION s VALUES('R003'), 
          PARTITION k VALUES('R004'), 
          DEFAULT PARTITION other
          );











