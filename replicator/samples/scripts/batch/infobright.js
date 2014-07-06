/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013-2014 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * Load script for Infobright.  This script is designed to ensure that
 * the number of rows deleted and inserted exactly matches the number of 
 * deletes and inserts in the CSV file. 
 */

// Called once when applier goes online. 
function prepare()
{
  logger.info("Preparing load script for Infobright");
}


// Called at start of batch transaction. 
function begin()
{
  // Does nothing. 
}

// Called once for each table that must be loaded. 
function apply(csvinfo)
{
  // Collect useful data. 
  sqlParams = csvinfo.getSqlParameters();
  csv_file = sqlParams.get("%%CSV_FILE%%");

  // Extract deleted data keys and put in temp CSV file for deletes.  Count
  // the rows so loaded. 
  runtime.exec('egrep \'^"D",\' ' + csv_file + '|cut -d, -f4 > ' 
    + csv_file + '.delete');
  expected_delete_rows = runtime.exec('cat ' + csv_file + '.delete|wc -l ');

  // Load delete rows  to staging table.  This script *must* run on the 
  // server.  Tungsten uses drizzle JDBC which does not handle 
  // LOAD DATA LOCAL INFILE properly.
  load_deletes_template = 
    "LOAD DATA INFILE '%%CSV_FILE%%.delete' INTO TABLE %%STAGE_TABLE_FQN%% \
  CHARACTER SET utf8 FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'";
  load_deletes = runtime.parameterize(load_deletes_template, sqlParams);
  logger.info(load_deletes);
  rows = sql.execute(load_deletes);
  if (rows != expected_delete_rows)
  {
    message = "LOAD DATA ROW count does not match: sql=" + load_data 
              + " expected_load_rows=" + expected_load_rows
              + " rows=" + rows;
    logger.error(message);
    throw new com.continuent.tungsten.replicator.ReplicatorException(message);
  }

  // Delete keys that match the staging table. 
  delete_sql_template = "DELETE %%BASE_TABLE%% FROM %%STAGE_TABLE_FQN%% s \
    INNER JOIN %%BASE_TABLE%% ON s.%%PKEY%% = %%BASE_TABLE%%.%%PKEY%%";
  delete_sql = runtime.parameterize(delete_sql_template, sqlParams);
  logger.info(delete_sql);
  rows = sql.execute(delete_sql);
  if (rows != expected_delete_rows)
  {
    message = "DELETE count does not match: sql=" + delete_sql 
              + " expected_delete_rows=" + expected_delete_rows
              + " rows=" + rows;
    logger.error(message);
    throw new com.continuent.tungsten.replicator.ReplicatorException(message);
  }

  // Extract inserted data and put into temp CSV file.
  runtime.exec('egrep \'^"I",\' ' + csv_file + '|cut -d, -f4- > ' 
    + csv_file + '.insert');
  expected_insert_rows = runtime.exec('cat ' + csv_file + '.insert|wc -l ');

  // Load the extracted inserts and check row count.  
  replace_template = "LOAD DATA INFILE '%%CSV_FILE%%.insert' INTO \
    TABLE %%BASE_TABLE%% CHARACTER SET utf8 FIELDS \
    TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'";
  replace = runtime.parameterize(replace_template, sqlParams);
  logger.info(replace);
  rows = sql.execute(replace);
  if (rows != expected_insert_rows)
  {
    message = "REPLACE count does not match: sql=" + replace 
              + " expected_insert_rows=" + expected_insert_rows
              + " rows=" + rows;
    logger.error(message);
    throw new com.continuent.tungsten.replicator.ReplicatorException(message);
  }
}

// Called at commit time for a batch. 
function commit()
{
  // Does nothing. 
}

// Called when the applier goes offline. 
function release()
{
  // Does nothing. 
}
