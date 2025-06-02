/*
"Пользовательские функции"

1. Создайте 2 пользовательские функции для загрузки данных в созданные ранее: 
Загрузка данных в целевые таблицы должна производиться из внешних EXTERNAL таблиц.
Первая функция для загрузки справочников, вторая - для загрузки таблиц фактов.
Для таблиц справочников необходимо реализовать FULL загрузку (полная очистка целевой таблицы и полная вставка всех записей).
Для таблиц фактов можно реализовать загрузку следующими способами:
DELTA_PARTITION - полная подмена партиций.
DELTA_UPSERT - предварительное удаление по ключу и последующая вставка записей из временной таблицы в целевую.
2. Создайте пользовательскую функцию для расчёта витрины, которая будет содержать результат выполнения плана продаж в разрезе: 
Код "Региона".
Код "Товарного направления" (matdirec).
Код "Канала сбыта".
Плановое количество.
Фактические количество.
Процент выполнения плана за месяц.
Код самого продаваемого товара в регионе*.
Требования к функции по расчёту витрины:

Функция должна принимать на вход месяц, по которому будут вестись расчеты. 
Название таблицы должно формироваться по шаблону plan_fact_<YYYYMM>, где <YYYYMM> - месяц расчета. 
Функция должна иметь возможность безошибочного запуска несколько раз по одному и тому же месяцу. 
3. Создайте представление (VIEW) на созданной ранее витрине:
Код "Региона".
Текст "Региона".
Код "Товарного направления" (matdirec).
Код "Канала сбыта".
Текст "Канала сбыта".
Процент выполнения плана за месяц.
Код самого продаваемого товара в регионе.
Код "Бренда" самого продаваемого товара в регионе.
Текст самого продаваемого товара в регионе.
Цена самого продаваемого товара в регионе.
Название представления v_plan_fact. Создание представления можно встроить в функцию по расчёту витрины.
Примечание:
Организация слоёв хранения данных подразумевает под собой создание таблиц в определённых схемах хранилища данных.
*/

DROP TABLE std11_26.sales;

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
DISTRIBUTED by (check_nm)
PARTITION BY RANGE(date) 
          (
          START ('2021-01-01'::date) END ('2022-01-01'::date) EVERY ('1 mon'::interval) WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1'), 
          DEFAULT PARTITION def  WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1')
          );

-- Создание функции логирования

CREATE TABLE std11_26.logs (
	log_id int8 not NULL,
	log_timestamp timestamp not null default now(),
	log_type text not NULL,
	log_msg text not NULL, 
	log_location text not NULL,
	is_error bool NULL,
	log_user text null default "current_user"(),
	constraint pk_log_id primary key (log_id)
)
DISTRIBUTED by (log_id);

-- создание последовательности
create sequence std11_26.log_id_seq
	increment by 1
	minvalue 1
	maxvalue 999999999999999999
	start 1;


create or replace function std11_26.f_load_write_log(p_log_type text,
									  p_log_message text,
									  p_location text)
	returns void
	language plpgsql
	volatile
as $$
declare
 v_log_type text;
 v_log_message text;
 v_sql text;
 v_location text;
 v_res text;

begin

-- проверка типа сообщения
 v_log_type = upper(p_log_type);
 v_location = lower(p_location);

 if v_log_type not in ('ERROR', 'INFO') then 
	raise exception 'Illegal log type! Use one of: ERROR, INFO.';
 end if;

 raise notice '%: %: <%> Location[%]', clock_timestamp(), v_log_type, p_log_message, v_location;

 v_log_message := replace(p_log_message, '''', '''''');

 v_sql := 'insert into std11_26.logs 
			values (' || nextval('std11_26.log_id_seq') ||',
					 current_timestamp, 
					''' || v_log_type || ''',
					' || coalesce('''' || v_log_message || '''', '''empty''') || ',
					' || coalesce('''' || v_location || '''', 'null') || ',
					' || case when v_log_type = 'ERROR' then true else false end || ',
					 current_user);';

 raise notice 'INSERT SQL IS: %', v_sql;
 -- v_res := dblink('adb_server', v_sql);
 execute v_sql; 

end;
$$
execute on any;





-- Создание витрины: подсчет кол-ва проданных товаров и сумму чека за пред 3 мес от указанного в ф-ции

create or replace function std11_26.f_load_mart(p_month varchar)
	returns int4
	language plpgsql
	volatile
as $$
declare
 v_table_name text;
 v_sql text;
 v_return int;
begin

	perform std11_26.f_load_write_log(p_log_type := 'INFO',
									  p_log_message := 'Start f_load_mart',
									  p_location := 'Sales mart calculation');

	drop table if exists std11_26.mart;
	
		create table std11_26.mart
		 WITH (
			appendonly=true,
			orientation=column,
			compresstype=zstd,
			compresslevel=1)
		 as 
		  select region
		 		 , material
		 		 , distr_chan
		 		 , sum(quantity) as qnt
		 		 , count(distinct check_nm) as chk_cnt
		  from std11_26.sales
		 	where "date" between date_trunc('month', to_date(p_month, 'YYYYMM')) - interval '3 month'
		 	  and date_trunc('month', to_date(p_month, 'YYYYMM'))
		  group by 1,2,3
		 distributed by (material);

	select count(*) into v_return from std11_26.mart;
	
	perform std11_26.f_load_write_log(p_log_type := 'INFO',
									  p_log_message := v_return || ' rows inserted',
									  p_location := 'Sales mart calculation');

	perform std11_26.f_load_write_log(p_log_type := 'INFO',
									  p_log_message := 'End f_load_mart',
									  p_location := 'Sales mart calculation');
	
	return v_return;
end;
$$
execute on any; 

select std11_26.f_load_mart('202108');

select * from std11_26.mart;

-- Создание функции загрузки данных в таблицу из файла (при помощи gpfdist)

-- Full load - полная загрузка для справочников и таблиц с небольшим объемом через внешнюю таблицу
-- ip-адрес достаем ч-з ipconfig (terminal)
-- перед загрузкой файла нужно развернуть gpfdist из папки(где находится файл): cd ... -> gpfdist -p 8080


CREATE OR REPLACE FUNCTION std11_26.f_load_full(p_table_name text, p_file_name text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

	 v_ext_table_name text;
	 v_sql text;
	 v_gpfdist text;
	 v_result int;
 
begin
	
	 v_ext_table_name = p_table_name||'_ext';
	 
	 execute 'truncate table '||p_table_name;

	 execute 'drop external table if exists '||v_ext_table_name;
	
	 v_gpfdist = 'gpfdist://172.16.128.110:8080/'||p_file_name||'.csv';  --  нужно указать ip машины, где лежат файлы

	 v_sql =   'create EXTERNAL TABLE '||v_ext_table_name||' (like '||p_table_name||' )
				location ('''||v_gpfdist||'''
				) on all
				format ''csv'' ( header delimiter '';'' null '''' escape ''"'' quote ''"'' )
				encoding ''UTF8''';

	raise notice 'external table is: %', v_sql;

	execute v_sql;	

	execute 'insert into '||p_table_name||' select * from '||v_ext_table_name; 

	execute 'select count(1) from '||p_table_name into v_result;
	  
	return v_result;
end;

$$
EXECUTE ON ANY;

select std11_26.f_load_full('std11_26.region', 'region');

-- Delta partition загрузка - загрузка определенных партиций из других бд

-- DROP FUNCTION std11_26.f_load_simple_partition(text, text, timestamp, timestamp, text, text, text);

CREATE OR REPLACE FUNCTION std11_26.f_load_simple_partition(p_table_name text, p_partition_key text, p_start_date timestamp, p_end_date timestamp, p_pxf_table text, p_user_id text, p_pass text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

declare

	v_ext_table_name text;
	v_temp_table text;
	v_sql text;
	v_pxf text;
	v_result int;
	v_dist_key text;
	v_params text;
	v_where text;
	v_load_interval interval;
	v_start_date date;
	v_end_date date;
	v_table_oid int4;
	v_cnt int8;
	
 
begin
	
	v_ext_table_name = p_table_name||'_ext';
	v_temp_table = p_table_name||'_tmp';
-- достаем ключ распределения целевой таблицы
	select c.oid
	into v_table_oid
	from pg_class as c inner join pg_namespace as n on c.relnamespace = n.oid
	where n.nspname||'.'||c.relname = p_table_name
	limit 1;

	if v_table_oid = 0 or v_table_oid is null then
		v_dist_key = 'distributed randomly';
	else
		v_dist_key = pg_get_table_distributedby(v_table_oid);
	end if;

-- получим параметры целевой таблицы (строка, ч-з зпт)
	select coalesce('where (' || array_to_string(reloptions, ', ') || ')','')
	from pg_class
	into v_params
	where oid = p_table_name::regclass;

	execute 'drop external table if exists '||v_ext_table_name;
	
	v_load_interval = '1 month'::interval;
	v_start_date := date_trunc('month', p_start_date);
	v_end_date  := date_trunc('month', p_end_date);

	v_where = p_partition_key || ' >= '''||v_start_date||'''::date and '|| p_partition_key ||' < '''||v_end_date||'''::date';

	v_pxf = 'pxf://'||p_pxf_table||'?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER='
					||p_user_id||'&PASS='||p_pass;

	raise notice 'pxf connection string: $', v_pxf;

	v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table_name||' (like'||p_table_name||')
			 LOCATION ('''||v_pxf||'''
			 ) ON ALL
			 FORMAT ''CUSTOM'' ( FORMATTER=''pxfwritable_import'' )
			 ENCODING ''UTF8''';

	raise notice 'external table is: $', v_sql;

	execute v_sql;

	v_sql := 'drop table if exists '|| v_temp_table ||';
			  create table '|| v_temp_table ||' (like '|| p_table_name') '|| v_params ||' '|| v_dist_key ||';';

	raise notice 'templ table is: $', v_sql;

	execute v_sql;

	v_sql = 'insert into '|| v_temp_table ||'select * from '|| v_ext_table_name ||'where '||v_where;

	execute v_sql;

	get diagnostics v_cnt = row_count;
	raise notice 'inderted rows: $', v_cnt;
	
	v_sql = 'alter table'|| p_table_name ||'exchange partition ror (date '''||v_start_date||''') 
			 with table '|| v_temp_table ||'with validation';

	raise notice 'exchange partition script: $', v_sql;
	
	execute v_sql;

	execute 'select count(1) from '||p_table_name into v_result;
	  
	return v_result;
end;

$$
EXECUTE ON ANY;


-- Упрощенная функция загрузки данных с другой бд

-- DROP FUNCTION std11_26.f_load_simple(text, text, text, text);

CREATE OR REPLACE FUNCTION std11_26.f_load_simple(p_table_name text, p_pxf_table text, p_user_id text, p_pass text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	

declare

	v_ext_table_name text;
	v_sql text;
	v_pxf text;
	v_result int;
 
begin
	
	v_ext_table_name = p_table_name||'_ext';

	v_pxf = 'pxf://'||p_pxf_table||'?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER='
					||p_user_id||'&PASS='||p_pass;
	
	execute 'drop external table if exists '||v_ext_table_name;
	
	execute 'truncate '||p_table_name;

	v_sql := 'CREATE EXTERNAL TABLE '||v_ext_table_name||'(like '||p_table_name||')
			 LOCATION ('''||v_pxf||'''
			 ) ON ALL
			 FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'')
			 ENCODING ''UTF8''';
	
	execute v_sql;
	
	v_sql = 'insert into '||p_table_name||' select * from adb.'||v_ext_table_name;

	execute v_sql;

	execute 'select count(1) from '||p_table_name into v_result;
	  
	return v_result;
end;


$$
EXECUTE ON ANY;


-- Витрина с результатами плана продаж за определенный месяц (формат: 'YYYYMM')

-- DROP FUNCTION std11_26.f_task_mart(varchar);

CREATE OR REPLACE FUNCTION std11_26.f_task_mart(p_month varchar)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
declare
-- v_table_name text;
 v_sql text;
 v_return int;
 v_mart_name text;
begin
   
	v_mart_name := 'std11_26.plan_fact_'||p_month;

	perform std11_26.f_load_write_log(p_log_type := 'INFO',
									  p_log_message := 'Start f_task_mart',
									  p_location := 'Sales result mart calculation');
	
	v_sql = 'drop table if exists '||v_mart_name;

	drop view if exists v_plan_fact;

	EXECUTE v_sql;
	
	v_sql =	'create table '||v_mart_name||
		 ' WITH (
			appendonly=true,
			orientation=column,
			compresstype=zstd,
			compresslevel=1)
		 as 
		    with plan_by_month as (
			select region
					, matdirec
					, distr_chan
					, sum(quantity) as sum_plan
				from std11_26.plan
				where date_part(''month'', date) = date_part(''month'', to_date('''||p_month||''', ''YYYYMM''))
				group by 1,2,3
			)
			, fact_by_month as (
			select s.region
					, m.matdirec
					, s.distr_chan
					, sum(s.quantity) as sum_fact
					, max(s.quantity) as max_fact
				from std11_26.sales s
				join std11_26.product m
				on s.material::int = m.material
				where date_part(''month'', s.date) = date_part(''month'', to_date('''||p_month||''', ''YYYYMM''))			
				group by 1,2,3
			)
			, bestseller as(			 
			SELECT s.region
					, s.material
					, ROW_NUMBER() OVER (PARTITION BY s.region ORDER BY SUM(s.quantity) DESC) AS row_num
				FROM std11_26.sales s 
				JOIN std11_26.product pd 
				ON s.material::int = pd.material
				where date_part(''month'', s.date) = date_part(''month'', to_date('''||p_month||''', ''YYYYMM''))			
				GROUP BY 1,2 
			)		
			select p.region "Регион"
					, p.matdirec "Товарное направление"
					, p.distr_chan "Канал сбыта"
					, coalesce(p.sum_plan,0) "Плановое количество"
					, coalesce(s.sum_fact,0) "Фактические количество"
					, round(1.*coalesce(s.sum_fact,0)/coalesce(p.sum_plan,0)*100, 1) "Процент выполнения"
					, coalesce(b.material, ''Отсутствует'') as "Cамый продаваемый товар в регионе"
				from fact_by_month s
				full outer join plan_by_month p
				on p.region = s.region and 
				   p.distr_chan = s.distr_chan and 
				   s.matdirec = p.matdirec::int
				join bestseller b
				on p.region = b.region and row_num = 1			
				order by 1,2,3
		 distributed randomly;';

	EXECUTE v_sql;

	EXECUTE 'SELECT count(*) FROM ' ||v_mart_name::text||';' INTO v_return;

	v_sql =	'create or replace view v_plan_fact as
		select m."Регион" "Код региона"
			,r.txt "Текст региона"
			, m."Товарное направление"
			, m."Канал сбыта" "Код канала сбыта"
			, c.txtsh "Текст канала сбыта"
			, m."Процент выполнения"
			, m."Cамый продаваемый товар в регионе" "Код бестселлера"
			, p.brand "Код бренда"
			, p.txt "Текст бестселлера"
			, pr.price "Цена"
		from '||v_mart_name||' m		
		 join std11_26.region r on m."Регион" = r.region
		 join std11_26.chanel c on m."Канал сбыта" = c.distr_chan
		 join std11_26.product p on m."Cамый продаваемый товар в регионе"::int4 = p.material
		 join std11_26.price pr on m."Регион" = pr.region and m."Cамый продаваемый товар в регионе"::int4 = pr.material';

	EXECUTE v_sql;

	RAISE NOTICE 'CREATING VIEW: %', 'DONE';


	perform std11_26.f_load_write_log(p_log_type := 'INFO',
									  p_log_message := v_return || ' rows inserted',
									  p_location := 'Sales result mart calculation');

	perform std11_26.f_load_write_log(p_log_type := 'INFO',
									  p_log_message := 'End f_task_mart',
									  p_location := 'Sales result mart calculation');
	
	return v_return;
end;


$$
EXECUTE ON ANY;





