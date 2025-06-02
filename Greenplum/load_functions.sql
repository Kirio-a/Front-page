-- Full load - полная загрузка для справочников и таблиц с небольшим объемом через внешнюю таблицу из внешнего файла
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
