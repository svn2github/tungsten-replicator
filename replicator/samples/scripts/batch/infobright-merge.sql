# Merge script for MySQL. 
#
# Extract deleted data keys and put in temp CSV file for deletes. 
!egrep '^"D",' %%CSV_FILE%% |cut -d, -f4 > %%CSV_FILE%%.delete

# Load the delete keys. 
LOAD DATA INFILE '%%CSV_FILE%%.delete' INTO TABLE %%STAGE_TABLE_FQN%% 
  CHARACTER SET utf8 FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'

# Delete keys that  match the staging table. 
DELETE %%BASE_TABLE%% 
  FROM %%STAGE_TABLE_FQN%% s
  INNER JOIN %%BASE_TABLE%% 
  ON s.%%PKEY%% = %%BASE_TABLE%%.%%PKEY%%

# Extract inserted data and put into temp CSV file. 
!egrep '^"I",' %%CSV_FILE%% |cut -d, -f4- > %%CSV_FILE%%.insert

# Load the extracted inserts. 
LOAD DATA INFILE '%%CSV_FILE%%.insert' INTO TABLE %%BASE_TABLE%% 
  CHARACTER SET utf8 FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
