# Merge script for MySQL. 
#
# Load CSV to staging table.  This script *must* run on the server.  Tungsten
# uses drizzle JDBC which does not handle LOAD DATA LOCAL INFILE properly. 
LOAD DATA INFILE '%%CSV_FILE%%' INTO TABLE %%STAGE_TABLE_FQN%% 
  CHARACTER SET utf8 FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'

# Delete rows.  This query applies all deletes that match, need it or not. 
# The inner join syntax used avoids an expensive scan of the base table 
# by putting it second in the join order. 
DELETE %%BASE_TABLE%% 
  FROM %%STAGE_TABLE_FQN%% s
  INNER JOIN %%BASE_TABLE%% 
  ON s.%%PKEY%% = %%BASE_TABLE%%.%%PKEY%% AND s.tungsten_opcode = 'D'

# Insert rows.  This query loads each inserted row provided that the 
# insert is (a) the last insert processed and (b) is not followed by a 
# delete.  The subquery could probably be optimized to a join. 
REPLACE INTO %%BASE_TABLE%%(%%BASE_COLUMNS%%) 
  SELECT %%BASE_COLUMNS%% FROM %%STAGE_TABLE_FQN%% AS stage_a
  WHERE tungsten_opcode='I' AND tungsten_row_id IN 
  (SELECT MAX(tungsten_row_id) FROM %%STAGE_TABLE_FQN%% GROUP BY %%PKEY%%)
