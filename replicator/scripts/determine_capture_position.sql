set termout off
set feedback off
set echo off
set head off

DECLARE
v_change_set_name varchar2(30) := UPPER('&1');

BEGIN

select 'Capture started at position '||first_scn as msg from all_capture, change_sets where all_capture.CAPTURE_NAME = change_sets.CAPTURE_NAME and change_sets.SET_NAME= v_change_set_name

END;
/
EXIT
