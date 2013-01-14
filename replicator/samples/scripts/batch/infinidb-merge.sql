# Merge script for InfiniDB.

# Either call cpimport using the full path, or by putting it into PATH.  
# Depending on your installation you may need to enable sudo for the 
# tungsten account or run as root for cpimport to work.  
!/usr/local/bin/cpimport %%STAGE_SCHEMA%% %%STAGE_TABLE%% %%CSV_FILE%% -s ',' -E '"'

# Delete rows.  This query applies all deletes that match, need it or not.
DELETE FROM %%BASE_TABLE%%
  WHERE %%BASE_TABLE%%.%%PKEY%% 
    IN (SELECT %%PKEY%% FROM %%STAGE_TABLE_FQN%% WHERE tungsten_opcode = 'D')

# Insert rows.  This query loads each inserted row provided that the
# insert is (a) the last insert processed and (b) is not followed by a
# delete.  The subquery could probably be optimized to a join.
INSERT INTO %%BASE_TABLE%%(%%BASE_COLUMNS%%)
  SELECT %%BASE_COLUMNS%% FROM %%STAGE_TABLE_FQN%% AS stage_a
  WHERE tungsten_opcode='I' AND tungsten_row_id IN
  (SELECT MAX(tungsten_row_id) FROM %%STAGE_TABLE_FQN%% GROUP BY %%PKEY%%)
