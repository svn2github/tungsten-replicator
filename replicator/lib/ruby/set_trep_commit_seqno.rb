
class ReplicatorSetTrepCommitSeqno
  include TungstenScript
  include MySQLServiceScript
  
  def main
    begin
      service_schema = TI.setting("repl_services.#{@options[:service]}.repl_svc_schema")
      
      if @options[:host] != nil
        begin
          TU.info("Load seqno #{@options[:seqno]} from #{@options[:host]}")
          cmd = "#{TI.thl(@options[:service])} list -seqno #{@options[:seqno]} -headers -json"
          thl_record_content = TU.ssh_result(cmd, @options[:host], TI.user())
        rescue CommandError => ce
          TU.debug(ce)
          raise "There was an error running the thl command on #{@options[:host]}"
        end
      
        thl_records = JSON.parse(thl_record_content)
        unless thl_records.instance_of?(Array) && thl_records.size() == 1
          raise "Unable to read the THL record for seqno #{@options[:seqno]} from #{@options[:host]}"
        end
      
        thl_record = thl_records[0]
        columns = ['task_id', 'seqno', 'fragno', 'last_frag', 'epoch_number', 
          'eventid', 'source_id', 'update_timestamp', 'extract_timestamp']
        values = [0, thl_record['seqno'], thl_record['frag'], 
          (thl_record['lastFrag'] == true ? 1 : 0), thl_record['epoch'], 
          "'#{thl_record['eventId']}'", "'#{thl_record['sourceId']}'", 
          'NOW()', 'NOW()']
      else
        columns = ['task_id', 'seqno', 'fragno', 'last_frag', 'epoch_number', 
          'update_timestamp', 'extract_timestamp']
        values = [0, @options[:seqno], 0, 1, @options[:epoch],  
          'NOW()', 'NOW()']
          
        if @options[:event_id] != nil
          columns << "eventid"
          values << "'#{@options[:event_id]}'"
        end
        if @options[:source_id] != nil
          columns << "source_id"
          values << "'#{@options[:source_id]}'"
        end
      end
      sql = "SET SESSION SQL_LOG_BIN=0; DELETE FROM #{service_schema}.trep_commit_seqno; INSERT INTO #{service_schema}.trep_commit_seqno (#{columns.join(",")}) VALUES (#{values.join(",")});"
      
      if @options[:sql] == true
        TU.output(sql)
      else
        cnt = get_mysql_value("SELECT COUNT(*) AS 'cnt' FROM #{service_schema}.trep_commit_seqno", 'cnt')
        if cnt == nil
          TU.warning "Unable to check the '#{service_schema}.trep_commit_seqno' table. This may be an indication of problems with the database."
        end
        if cnt.to_i() > 1
          if @options[:force] == true
            TU.warning("The '#{service_schema}.trep_commit_seqno' table contains more than 1 row. All rows will be replaced.")
          else
            raise "Unable to update '#{service_schema}.trep_commit_seqno' because it currrently has more than one row. Add '--force' to bypass this warning."
          end
        end
        
        tungsten_schema_sql_file = "#{TI.root()}/#{CURRENT_RELEASE_DIRECTORY}/tools/ruby-tpm/configure/sql/tungsten_schema.sql"

        sqlfile = Tempfile.new("set_trep_commit_seqno")
        sqlfile.puts("SET SESSION SQL_LOG_BIN=0; DROP SCHEMA IF EXISTS #{service_schema}; CREATE SCHEMA #{service_schema}; USE #{service_schema}; source #{tungsten_schema_sql_file};")
        sqlfile.puts(sql)
        sqlfile.flush()
        
        TU.notice("Update the #{service_schema}.trep_commit_seqno table")
        TU.cmd_result("cat #{sqlfile.path()}")
        TU.cmd_result("cat #{sqlfile.path()} | #{get_mysql_command()}")
        
        # Emptying the THL and relay logs makes sure that we are starting with 
        # a fresh directory as if `datasource <hostname> restore` was run.
        if @options[:clear_logs] == true
          TU.notice("Empty THL and relay logs for replication service '#{@options[:service]}'")
          dir = TI.setting("repl_services.#{@options[:service]}.repl_thl_directory")
          if File.exists?(dir)
            TU.cmd_result("rm -rf #{dir}/*")
          end
          dir = TI.setting("repl_services.#{@options[:service]}.repl_relay_directory")
          if File.exists?(dir)
            TU.cmd_result("rm -rf #{dir}/*")
          end
        end
      end
    rescue => e
      raise e
    end
  end

  def configure
    super()
    description("Update the trep_commit_seqno table with metadata for the given sequence number.<br>
Examples:<br>
$> set_trep_commit_seqno.sh --host=db1 --seqno=35<br>
$> set_trep_commit_seqno.sh --seqno=35 --epoch=23")

    add_option(:clear_logs, {
      :on => "--clear-logs",
      :default => false,
      :help => "Delete all THL and relay logs for the service"
    })
    
    add_option(:epoch, {
      :on => "--epoch String",
      :help => "The epoch number to use for updating the trep_commit_seqno table"
    })
    
    add_option(:event_id, {
      :on => "--event-id String",
      :help => "The event id to use for updating the trep_commit_seqno table"
    })
    
    add_option(:host, {
      :on => "--host String",
      :help => "Determine metadata for the --seqno statement from this host"
    })
    
    add_option(:seqno, {
      :on => "--seqno String",
      :help => "The sequence number to use for updating the trep_commit_seqno table"
    })
    
    add_option(:source_id, {
      :on => "--source-id String",
      :help => "The source id to use for updating the trep_commit_seqno table"
    })
    
    add_option(:sql, {
      :on => "--sql",
      :default => false,
      :help => "Only output the SQL statements needed to update the schema"
    })
  end
  
  def validate
    super()
    
    unless @options[:sql] == true
      # All replication must be OFFLINE
      if TI.is_replicator?()
        if TI.is_running?("replicator")
          if TI.trepctl_value(@options[:service], "state") =~ /ONLINE/
            TU.error("The replication service '#{@options[:service]}' must be OFFLINE to provision this server")
          end
        end
      else
        TU.error("This server is not configured for replication")
      end
    end
    
    if @options[:seqno] == nil
      TU.error("The --seqno argument is required")
    end
    
    if @options[:clear_logs] == true && @options[:sql] == true
      TU.error("Unable to clear logs when the --sql argument is given")
    end
    
    if @options[:host] == nil && @options[:epoch] == nil
      TU.error("You must provide the --host or --epoch argument")
    end
    
    if @options[:host] != nil && @options[:epoch] != nil
      TU.error("You may not provide the --host or --epoch arguments together")
    end
  end
end