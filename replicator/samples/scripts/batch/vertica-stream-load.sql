# Load script for Vertica.  First command ensures timezone is set to GMT+0:00. 
COPY %%STAGE_TABLE%% FROM STDIN 
  DIRECT NULL 'null' DELIMITER ',' ENCLOSED BY '"'

