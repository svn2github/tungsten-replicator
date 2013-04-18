DECLARE
v_user varchar2(30) := 'GRANITT';
v_version varchar2(17);
i_version number;

/* 
Change CDC type as desired :
- SYNC_SOURCE : synchronous capture
- HOTLOG_SOURCE : asynchronous capture (HOTLOG)
*/
v_cdc_type varchar(30) := 'SYNC_SOURCE';
v_sync boolean := (v_cdc_type = 'SYNC_SOURCE');

v_change_set_name varchar2(20) := 'TUNGSTEN_CHANGE_SET';
v_table_name varchar2(40);
v_column_name varchar2(100);
v_column_type varchar(50);
column_type_len varchar(50);
v_column_list varchar(10000);
v_create_ct_statement varchar2(10000);

v_quoted_user varchar2(100);
v_quoted_ct_name varchar2(100);
v_quoted_cs_name varchar2(100);
v_quoted_schema varchar2(100);
v_quoted_table varchar2(100);

CURSOR C1 IS SELECT table_name FROM ALL_TABLES where owner=v_user and table_name not like 'AQ$%'and table_name not like 'CDC$%';
cursor C2 is
select distinct col.column_name, data_type, decode(char_used, 'C', char_length, data_length) from all_tab_columns col where owner=v_user and col.table_name = v_table_name;
BEGIN

SELECT version into v_version from v$instance;
DBMS_OUTPUT.PUT_LINE ('Oracle version : ' || v_version || '/'|| TO_CHAR(INSTR( v_version, '.')));
i_version := TO_NUMBER(SUBSTR(v_version, 1, INSTR(v_version, '.') -1));

v_quoted_schema := '''' || v_user || '''';
v_quoted_cs_name := '''' || v_change_set_name || '''';
v_quoted_user := '''' || v_user  || '_PUB''';

IF v_sync THEN
DBMS_OUTPUT.PUT_LINE ('Setting Up Synchronous Data Capture ' || v_change_set_name  || ' for Oracle ' || TO_CHAR(i_version)); 
DBMS_CDC_PUBLISH.CREATE_CHANGE_SET(change_set_name => v_change_set_name, description => 'Change set used by Tungsten Replicator', change_source_name =>v_cdc_type);
ELSE
DBMS_OUTPUT.PUT_LINE ('Setting Up Asynchronous Data Capture ' || v_change_set_name); 
DBMS_CDC_PUBLISH.CREATE_CHANGE_SET(change_set_name => v_change_set_name, description => 'Change set used by Tungsten Replicator', change_source_name => v_cdc_type, stop_on_ddl => 'y');
END IF;

OPEN C1;
LOOP
FETCH C1 INTO v_table_name;
EXIT WHEN C1%NOTFOUND;
DBMS_OUTPUT.PUT_LINE ('Processing table ' || v_user || '.' || v_table_name);
v_column_list := '';
open C2;
loop
fetch C2 into v_column_name, v_column_type, column_type_len;
EXIT WHEN C2%NOTFOUND;
DBMS_OUTPUT.PUT_LINE ('Found :' || v_column_type || ' / ' || column_type_len);

IF LENGTH(v_column_list) > 0 THEN
IF i_version > 10 OR instr(v_column_type, 'NCLOB') < 1 THEN
/* NCLOB not supported by Oracle 10G */
v_column_list := v_column_list || ', ' || v_column_name || ' ' ||v_column_type;
IF v_column_type != 'DATE' 
	AND instr(v_column_type, 'NCLOB') < 1 
	AND instr(v_column_type, 'TIMESTAMP') < 1 then
	v_column_list := v_column_list || '('||column_type_len||')';
END IF;
END IF;
ELSE
IF i_version > 10 OR instr(v_column_type, 'NCLOB') < 1 THEN
v_column_list := v_column_name || ' ' ||v_column_type;
if v_column_type != 'DATE'
	AND instr(v_column_type, 'NCLOB') < 1 
	AND instr(v_column_type, 'TIMESTAMP') < 1 then
	v_column_list := v_column_list || '('||column_type_len||')';
END IF;
END IF;
END IF;
end loop;
close C2;

/* Create the change table */
IF LENGTH(v_column_list) > 0 THEN
DBMS_OUTPUT.PUT_LINE ('Creating change table for ' || v_user || '.' || v_table_name || '(' || v_column_list || ')');
v_column_list := '''' || v_column_list || '''';

v_quoted_table := '''' || v_table_name  || '''';
v_quoted_ct_name := '''CT_' || SUBSTR(v_table_name, 1, 22)  || '''';

IF v_sync THEN
IF i_version > 10 THEN
v_create_ct_statement :='BEGIN DBMS_CDC_PUBLISH.CREATE_CHANGE_TABLE(owner=>'||v_quoted_user||', change_table_name=> '||v_quoted_ct_name||', change_set_name=>'|| v_quoted_cs_name ||', source_schema=>'|| v_quoted_schema ||', source_table=>'|| v_quoted_table ||', column_type_list => '|| v_column_list||', capture_values => ''both'',  rs_id => ''y'', row_id => ''n'', user_id => ''n'', timestamp => ''n'', object_id => ''n'', source_colmap => ''y'', target_colmap => ''y'', DDL_MARKERS=> ''n'', options_string=>''TABLESPACE ' || v_user ||'_PUB'');END;';
ELSE
v_create_ct_statement :='BEGIN DBMS_CDC_PUBLISH.CREATE_CHANGE_TABLE(owner=>'||v_quoted_user||', change_table_name=> '||v_quoted_ct_name||', change_set_name=>'|| v_quoted_cs_name ||', source_schema=>'|| v_quoted_schema ||', source_table=>'|| v_quoted_table ||', column_type_list => '|| v_column_list||', capture_values => ''both'',  rs_id => ''y'', row_id => ''n'', user_id => ''n'', timestamp => ''n'', object_id => ''n'', source_colmap => ''y'', target_colmap => ''y'', options_string=>''TABLESPACE ' || v_user ||'_PUB'');END;';
END IF;
ELSE
v_create_ct_statement :='BEGIN DBMS_CDC_PUBLISH.CREATE_CHANGE_TABLE(owner=>'||v_quoted_user||', change_table_name=> '||v_quoted_ct_name||', change_set_name=>'|| v_quoted_cs_name ||', source_schema=>'|| v_quoted_schema ||', source_table=>'|| v_quoted_table ||', column_type_list => '|| v_column_list||', capture_values => ''both'',  rs_id => ''y'', row_id => ''n'', user_id => ''n'', timestamp => ''n'', object_id => ''n'', source_colmap => ''n'', target_colmap => ''y'', options_string=>''TABLESPACE ' || v_user ||'_PUB'');END;';
END IF;
DBMS_OUTPUT.PUT_LINE (v_create_ct_statement);
DBMS_OUTPUT.PUT_LINE ('/');
execute immediate v_create_ct_statement;

END IF;
END LOOP;
CLOSE C1;


IF not v_sync THEN
DBMS_OUTPUT.PUT_LINE ('Enabling change set : ' || v_change_set_name);
DBMS_CDC_PUBLISH.ALTER_CHANGE_SET(change_set_name => v_change_set_name,enable_capture => 'y');
/* 
ELSE => in case of Synchronous change set, it is enabled by default (and cannot be disabled)
*/ 
END IF;
END;
/