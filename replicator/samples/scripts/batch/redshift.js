/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013-2014 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * Load script for Redshift through S3. Uses AWS credentials from
 * share/s3-config-{service}.json configuration file.
 *
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 */

// AWS details.
var awsS3Path;
var awsAccessKey;
var awsSecretKey

/** Reads AWS configuration file into a string. */
function readAWSConfigFile()
{
  var serviceName = runtime.getContext().getServiceName();
  var awsConfigFileName = "s3-config-" + serviceName + ".json";
  var awsConfigFile = "../../../../share/" + awsConfigFileName;
  var f = new java.io.File(awsConfigFile);
  if (!f.isFile())
  {
    message = "AWS S3 configuration file (share/" + awsConfigFileName
    ") does not exist, "
        + "create one by using a sample (tungsten/cluster-home/samples/conf/s3-config.json)";
    throw new com.continuent.tungsten.replicator.ReplicatorException(message);
  }

  logger.info("redshift.js using AWS S3 configuration: " + awsConfigFile);
  var file = new java.io.BufferedReader(new java.io.FileReader(f));
  var sb = new java.lang.StringBuffer();
  while ((line = file.readLine()) != null)
  {
    sb.append(line);
    sb.append(java.lang.System.getProperty("line.separator"));
  }

  return sb.toString();
}

/** Called once when applier goes online. */
function prepare()
{
  // Read AWS details from configuration file.
  var json = readAWSConfigFile();
  awsConfig = eval("(" + json + ")");

  awsS3Path = awsConfig["awsS3Path"];
  awsAccessKey = awsConfig["awsAccessKey"];
  awsSecretKey = awsConfig["awsSecretKey"];

  logger.info("AWS S3 CSV staging path: " + awsS3Path);
}

/** Called at start of batch transaction. */
function begin()
{
  // Start the transaction.
  sql.begin();
}

/** Called for each table in the transaction.  Load rows to staging table. */
function apply(csvinfo)
{
  // Fill in variables required to create SQL to merge data for current table.
  csv_file = csvinfo.file.getAbsolutePath();
  csv_filename = csvinfo.file.getName();
  schema = csvinfo.schema;
  table = csvinfo.table;
  seqno = csvinfo.startSeqno;
  key = csvinfo.key;
  stage_table_fqn = csvinfo.getStageTableFQN();
  base_table_fqn = csvinfo.getBaseTableFQN();
  base_columns = csvinfo.getBaseColumnList();
  pkey_columns = csvinfo.getPKColumnList();
  where_clause = csvinfo.getPKColumnJoinList(stage_table_fqn, base_table_fqn);

  // Upload CSV to S3.
  runtime.exec("s3cmd put " + csv_file + " " + awsS3Path + "/");

  // Clear the staging table.
  clear_sql = runtime.sprintf("DELETE FROM %s", stage_table_fqn);
  logger.info("CLEAR: " + clear_sql);
  sql.execute(clear_sql);

  // Create and execute copy command.
  copy_sql = runtime.sprintf(
          "COPY %s FROM '%s/%s' CSV NULL AS 'null' CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'",
          stage_table_fqn, awsS3Path, csv_filename, awsAccessKey, awsSecretKey);
  logger.info("COPY: "
      + copy_sql.substring(0, copy_sql.indexOf("CREDENTIALS") + 12) + "...");
  sql.execute(copy_sql);

  // Check loaded row count.
  expected_copy_rows = runtime.exec("cat " + csv_file + " |wc -l");
  rows = sql.retrieveRowCount(stage_table_fqn);
  if (rows != expected_copy_rows)
  {
    message = "Row count in staging table does not match: sql=" + copy_sql
        + " expected_copy_rows=" + expected_copy_rows + " rows=" + rows;
    logger.error(message);
    throw new com.continuent.tungsten.replicator.ReplicatorException(message);
  }
  else
  {
    if (logger.isDebugEnabled())
    {
      logger.debug("COUNT: " + rows);
    }
  }

  // Remove deleted rows from base table.
  delete_sql = runtime.sprintf(
    "DELETE FROM %s WHERE EXISTS (SELECT * FROM %s WHERE %s AND %s.tungsten_opcode IN ('D', 'UD'))",
    base_table_fqn, 
    stage_table_fqn, 
    where_clause, 
    stage_table_fqn
  );
  logger.info("DELETE: " + delete_sql);
  sql.execute(delete_sql);

  // Insert non-deleted INSERT rows, i.e. rows not followed by another INSERT
  // or a DELETE.
  insert_sql = runtime.sprintf(
    "INSERT INTO %s (%s) SELECT %s FROM %s WHERE tungsten_opcode IN ('I', 'UI') AND tungsten_row_id IN (SELECT MAX(tungsten_row_id) FROM %s GROUP BY %s)", 
    base_table_fqn, 
    base_columns, 
    base_columns, 
    stage_table_fqn, 
    stage_table_fqn, 
    pkey_columns
  );
  logger.info("INSERT: " + insert_sql);
  sql.execute(insert_sql);
}

/** Called at commit time for a batch. */
function commit()
{
  // Commit the transaction.
  sql.commit();
}

/** Called when the applier goes offline. */
function release()
{
  // Does nothing.
}
