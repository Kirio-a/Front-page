-- Создание таблицы с интеграционным движком Postgres для получения данных в таблице

create table std11_26.ch_plan_fact  --на текущем хосте
(
	`Регион` String,
	`Товарное направление` String,
	`Канал сбыта` String,
	`Плановое количество` Int32,
	`Фактические количество` Int32,
	`Процент выполнения` Int32,
	`Cамый продаваемый товар в регионе` String
)
engine = PostgreSQL('192.168.214.203:5432', 'adb', 'plan_fact_202102', 'std11_26', 'dA1VfT1gDVlGIs', 'std11_26')       
;

drop table std11_26.ch_price_dict;

create table std11_26.ch_price_dict
(
	`material` Int32,
	`region` String,
	`distr_chan` String,
	`price` Int32
)
engine = PostgreSQL('192.168.214.203:5432', 'adb', 'price', 'std11_26', 'dA1VfT1gDVlGIs', 'std11_26')       
;

drop table std11_26.ch_chanel_dict

create table std11_26.ch_chanel_dict
(
	`distr_chan` Int32,
	`txtsh` String
)
engine = PostgreSQL('192.168.214.203:5432', 'adb', 'chanel', 'std11_26', 'dA1VfT1gDVlGIs', 'std11_26')       
;

drop table std11_26.ch_product_dict

create table std11_26.ch_product_dict
(
	`material` Int32,
	`asgrp` Int32,
	`brand` Int32,
	`matcateg` String,
	`matdirec` Int32,
	`txt` String
)
engine = PostgreSQL('192.168.214.203:5432', 'adb', 'product', 'std11_26', 'dA1VfT1gDVlGIs', 'std11_26')       
;

drop table std11_26.ch_region_dict

create table std11_26.ch_region_dict
(
	`region` String,
	`txt` String
)
engine = PostgreSQL('192.168.214.203:5432', 'adb', 'region', 'std11_26', 'dA1VfT1gDVlGIs', 'std11_26')       
;


-- Создание реплецированной таблицы на всех хостах кластера
-- Скрипт нужно открывать на каждом из хостов отдельно, где нужно создать реплику
drop table std11_26.ch_plan_fact;

create table std11_26.ch_plan_fact  --на текущем хосте
(
	`Регион` String,
	`Товарное направление` String,
	`Канал сбыта` String,
	`Плановое количество` Int32,
	`Фактические количество` Int32,
	`Процент выполнения` Int32,
	`Cамый продаваемый товар в регионе` String
)
engine = ReplicatedMergeTree('/click/std11_26/ch_plan_fact/{shard}', '{replica}')
order by `Регион`;

insert into std11_26.ch_plan_fact
select * from std11_26.ch_plan_fact_ext;

select * from std11_26.ch_plan_fact;

-- Затем выполняем скрипт создания таблицы с другого хоста. 
create table std11_26.ch_plan_fact  --на текущем хосте
(
	`Регион` String,
	`Товарное направление` String,
	`Канал сбыта` String,
	`Плановое количество` Int32,
	`Фактические количество` Int32,
	`Процент выполнения` String,
	`Cамый продаваемый товар в регионе` String
)
engine = ReplicatedMergeTree('/click/std11_26/ch_plan_fact/{shard}', '{replica}')
order by `Регион`;
-- Таблица автоматически заполняется данными из перевоночальной

-- Создаем распределённую таблицу ch_plan_fact_distr, выбрав для неё корректный ключ шардирования.

drop table std11_26.ch_plan_fact_distr;

create table std11_26.ch_plan_fact_distr as std11_26.ch_plan_fact_ext
engine = Distributed('default_cluster', 'std11_26', 'ch_plan_fact_ext', rand())

insert into std11_26.ch_plan_fact_distr
select * from std11_26.ch_plan_fact_ext;
 
select count(*) from std11_26.ch_plan_fact_distr;

select * from std11_26.ch_plan_fact_distr;

select * from std11_26.ch_plan_fact_ext;

select * from system.macros;



















