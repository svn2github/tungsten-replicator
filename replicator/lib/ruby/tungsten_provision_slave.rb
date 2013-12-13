# ToDo
# - mysqldump
# - Create a TungstenBackupScript subclass for sending output to remote systems

class TungstenReplicatorProvisionSlave
  include TungstenScript
  include MySQLServiceScript
  include OfflineServicesScript
  
  MASTER_BACKUP_POSITION_SQL = "xtrabackup_tungsten_master_backup_position.sql"
  
  def main
    if @options[:mysqldump] == false
      provision_with_xtrabackup()
    else
      provision_with_mysqldump()
    end
  end
  
  def provision_with_xtrabackup
    begin
      # Does this version of innobackupex-1.5.1 support the faster 
      # --move-back instead of --copy-back
      supports_move_back = TU.cmd_result("#{get_xtrabackup_command()} --help | grep -e\"\-\-move\-back\" | wc -l")
      if supports_move_back == "1"
        supports_move_back = true
      else
        supports_move_back = false
      end

      # Prepare the staging directory for the data
      # If we are restoring directly to the data directory then MySQL
      # must be shutdown and the data directory emptied
      id = build_timestamp_id("provision_xtrabackup_")
      if @options[:direct] == true
        TU.notice("Stop MySQL and empty all data directories")
        empty_mysql_directory()

        staging_dir = @options[:mysqldatadir]
      else
        staging_dir = TI.setting("repl_services.#{@options[:service]}.repl_backup_directory") + "/" + id
      end
      FileUtils.mkdir_p(staging_dir)
      TU.cmd_result("#{sudo_prefix()}chown -R #{TI.user()} #{staging_dir}")

      # SSH to the source server and run the backup. It will place the snapshot
      # into the staging_dir directory
      TU.notice("Create a backup of #{@options[:source]} in #{staging_dir}")
      TU.forward_cmd_results?(true)
      TU.ssh_result("#{TI.root()}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/scripts/xtrabackup_to_slave #{TU.get_tungsten_command_options()} --backup --target=#{TI.hostname()} --storage-directory=#{staging_dir} --service=#{@options[:service]}", 
        @options[:source], TI.user())
      TU.forward_cmd_results?(false)
    
      # This must be done before the MySQL server is started
      TU.notice("Prepare the files for MySQL to run")
      TU.forward_cmd_results?(true)
      TU.cmd_result("#{get_xtrabackup_command()} --apply-log #{staging_dir}")
      TU.forward_cmd_results?(false)

      # If we didn't take the backup directly into the data directory,
      # then we need to stop MySQL, empty the data directory and move the
      # files into the proper location
      unless @options[:direct] == true
        TU.notice("Stop MySQL and empty all data directories")
        # Shutdown MySQL and empty the data directory in preparation for the 
        # restored data
        empty_mysql_directory()

        # Copy the backup files to the mysql data directory
        if supports_move_back == true
          restore_cmd = "--move-back"
        else
          restore_cmd = "--copy-back"
        end
        
        TU.notice("Transfer data files to the MySQL data directory")
        TU.forward_cmd_results?(true)
        TU.cmd_result("#{sudo_prefix()}#{get_xtrabackup_command()} #{restore_cmd} #{staging_dir}")
        TU.forward_cmd_results?(false)
      end

      # Fix the permissions and restart the service
      TU.cmd_result("#{sudo_prefix()}chown -RL #{@options[:mysqluser]}: #{@options[:mysqldatadir]}")

      if @options[:mysqlibdatadir].to_s() != ""
        TU.cmd_result("#{sudo_prefix()}chown -RL #{@options[:mysqluser]}: #{@options[:mysqlibdatadir]}")
      end

      if @options[:mysqliblogdir].to_s() != ""
        TU.cmd_result("chown -RL #{@options[:mysqluser]}: #{@options[:mysqliblogdir]}")
      end

      start_mysql_server()
      
      # This updates the trep_commit_seqno table with the proper location
      # if the backup was taken from a master server
      if TU.cmd("#{sudo_prefix()}test -f #{@options[:mysqldatadir]}/#{MASTER_BACKUP_POSITION_SQL}")
        TU.cmd_result("#{sudo_prefix()}cat #{@options[:mysqldatadir]}/#{MASTER_BACKUP_POSITION_SQL} | #{get_mysql_command()}")
      end
      
      TU.notice("Backup and restore complete")

      if @options[:direct] == false && staging_dir != "" && File.exists?(staging_dir)
        TU.debug("Cleanup #{staging_dir}")
        TU.cmd_result("rm -r #{staging_dir}")
      end
    rescue => e
      if @options[:direct] == false && staging_dir != "" && File.exists?(staging_dir)
        TU.debug("Remove #{staging_dir} due to the error")
        TU.cmd_result("rm -r #{staging_dir}")
      end
      
      # If the backup/restore failed, the MySQL data directory ownership may
      # be left in a broken state
      if @options[:direct] == true
        TU.cmd_result("#{sudo_prefix()}chown -R #{@options[:mysqluser]}: #{staging_dir}")
      end

      raise e
    end
  end
  
  def provision_with_mysqldump
    begin
      # Create a directory to hold the mysqldump output
      id = build_timestamp_id("provision_mysqldump_")
      staging_dir = TI.setting("repl_services.#{@options[:service]}.repl_backup_directory") + "/" + id
      FileUtils.mkdir_p(staging_dir)
      
      # SSH to the source server and run the backup. It will place the output
      # into the staging_dir directory
      TU.notice("Create a mysqldump backup of #{@options[:source]} in #{staging_dir}")
      TU.forward_cmd_results?(true)
      TU.ssh_result("#{TI.root()}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/scripts/mysqldump_to_slave #{TU.get_tungsten_command_options()} --backup --target=#{TI.hostname()} --storage-file=#{staging_dir}/provision.sql.gz --service=#{@options[:service]}", 
        @options[:source], TI.user())
      TU.forward_cmd_results?(false)
      
      TU.notice("Load the mysqldump file")
      TU.cmd_result("gunzip -c #{staging_dir}/provision.sql.gz | #{get_mysql_command()}")
    
      if staging_dir != "" && File.exists?(staging_dir)
        TU.debug("Remove #{staging_dir}")
        TU.cmd_result("rm -r #{staging_dir}")
      end
    rescue => e
      if staging_dir != "" && File.exists?(staging_dir)
        TU.debug("Remove #{staging_dir} due to the error")
        TU.cmd_result("rm -r #{staging_dir}")
      end
      
      raise e
    end
  end
  
  def validate
    # All replication must be OFFLINE
    unless TI.is_replicator?()
      TU.error("This server is not configured for replication")
    end
    
    # This section evaluates :mysqldump and :xtrabackup to determine the best
    # mechanism to use. This will need to be more generic as we support
    # more platforms
    if opt(:mysqldump) == true && opt(:xtrabackup) == true
      TU.warning("You have specified --mysqldump and --xtrabackup, the script will use xtrabackup")
    end
    
    # Make sure to unset the :mysqldump value if :xtrabackup has been given
    if opt(:xtrabackup) == true
      opt(:mysqldump, false)
    end
    
    # Inspect the default value for the replication service to identify the 
    # preferred method
    if opt(:mysqldump) == nil && opt(:xtrabackup) == nil
      if TI.trepctl_property(opt(:service), "replicator.backup.default") == "mysqldump"
        opt(:mysqldump, true)
      else
        opt(:mysqldump, false)
      end
    end
    
    # Run validation for super classes after we have determined the backup
    # type. This makes sure that the needed options are loaded
    super()
    
    if @options[:mysqldump] == false
      if sudo_prefix() != ""
        if TI.setting("root_command_prefix") != "true"
          TU.error("The installation at #{TI.root()} is not allowed to use sudo")
        else
          # Test for specific commands
        end
      else
        if ENV['USER'] != @options[:mysqluser] && ENV['USER'] != "root"
          TU.error("The current user is not the #{@options[:mysqluser]} system user or root. You must run the script as #{@options[:mysqluser]} or enable sudo by running `tpm update --enable-sudo-access=true`.")
        end
      end
    
      # Read data locations from the my.cnf file
      @options[:mysqldatadir] = get_mysql_option("datadir")
      if @options[:mysqldatadir].to_s() == ""
        # The configuration file doesn't have a datadir value
        # See if MySQL will give one and store it in wrapper config file
        @options[:mysqldatadir] = get_mysql_variable("datadir")
        if @options[:mysqldatadir].to_s() != ""
          set_mysql_defaults_value("datadir=#{@options[:mysqldatadir]}")
        end
      end
    
      @options[:mysqlibdatadir] = get_mysql_option("innodb_data_home_dir")
      @options[:mysqliblogdir] = get_mysql_option("innodb_log_group_home_dir")
      @options[:mysqllogdir] = TI.setting("repl_services.#{@options[:service]}.repl_datasource_log_directory")
      @options[:mysqllogpattern] = TI.setting("repl_services.#{@options[:service]}.repl_datasource_log_pattern")

      if @options[:mysqldatadir].to_s() == ""
        TU.error "The configuration file at #{@options[:my_cnf]} does not define a datadir value."
      else
        unless TU.cmd("#{sudo_prefix()}test -w #{@options[:mysqldatadir]}")
          TU.error "The MySQL data dir '#{@options[:mysqldatadir]}' is not writeable"
        end
      end
    
      path = TU.cmd_result("which innobackupex-1.5.1 2>/dev/null", true)
      if path == ""
        TU.error("Unable to find the innobackupex-1.5.1 script")
      elsif sudo_prefix() != ""
        path = TU.cmd_result("#{sudo_prefix()}which innobackupex-1.5.1 2>/dev/null", true)
        if path == ""
          TU.error("Unable to find the innobackupex-1.5.1 script using sudo")
        end
      end
    
      # If the InnoDB files are stored somewhere other than datadir we are
      # not able to put them all in the correct position at this time
      # This only matters if we are restoring directly to the data directory
      if @options[:direct] == true
        if @options[:mysqlibdatadir].to_s() != "" || @options[:mysqliblogdir].to_s() != ""
          TU.error("Unable to restore to #{@options[:mysqldatadir]} because #{@options[:my_cnf]} includes a definition for 'innodb_data_home_dir' or 'innodb_log_group_home_dir'")
        end
      end
    else
      # No extra validation needed for mysqldump
    end
    
    if @options[:source].to_s() == ""
      TU.error("The --source argument is required")
    else
      if TU.test_ssh(@options[:source], TI.user())
        begin
          TU.forward_cmd_results?(true)
          if @options[:mysqldump] == false
            TU.ssh_result("#{TI.root()}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/scripts/xtrabackup_to_slave #{TU.get_tungsten_command_options()} --backup --target=#{TI.hostname()} --service=#{@options[:service]} --validate", 
              @options[:source], TI.user())
          else
            TU.ssh_result("#{TI.root()}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/scripts/mysqldump_to_slave #{TU.get_tungsten_command_options()} --backup --target=#{TI.hostname()} --service=#{@options[:service]} --validate", 
              @options[:source], TI.user())
          end
          TU.forward_cmd_results?(false)
        rescue
        end
      end
    end
  end
  
  def configure
    super()
    
    add_option(:mysqldump, {
      :on => "--mysqldump",
      :help => "Use mysqldump instead of xtrabackup"
    })
    
    add_option(:xtrabackup, {
      :on => "--xtrabackup",
      :help => "Use xtrabackup instead of mysqldump"
    })
    
    add_option(:source, {
      :on => "--source String",
      :help => "Server to use as a source for the backup"
    })
    
    add_option(:direct, {
      :on => "--direct",
      :default => false,
      :help => "Use the MySQL data directory for staging and preparation",
      :aliases => ["--restore-to-datadir"]
    })
    
    # We want the THL and relay logs to be reset with the new data
    set_option_default(:clear_logs, true)
    set_option_default(:offline, true)
    set_option_default(:online, true)
  end
  
  def empty_mysql_directory
    stop_mysql_server()

    TU.cmd_result("#{sudo_prefix()}find #{@options[:mysqldatadir]}/ -mindepth 1 | xargs #{sudo_prefix()} rm -rf")
    TU.cmd_result("#{sudo_prefix()}find #{@options[:mysqllogdir]}/ -name #{@options[:mysqllogpattern]}.*  | xargs #{sudo_prefix()} rm -rf")

    if @options[:mysqlibdatadir].to_s() != ""
      TU.cmd_result("#{sudo_prefix()}find #{@options[:mysqlibdatadir]}/ -mindepth 1 | xargs #{sudo_prefix()} rm -rf")
    end

    if @options[:mysqliblogdir].to_s() != ""
      TU.cmd_result("#{sudo_prefix()}find #{@options[:mysqliblogdir]}/ -mindepth 1 | xargs #{sudo_prefix()} rm -rf")
    end
  end

  def read_property_from_file(property, filename)
    value = nil
    regex = Regexp.new(property)
    File.open(filename, 'r') do |file|
      file.read.each_line do |line|
        line.strip!
        if line =~ regex
          value = line.split("=")[1].strip
        end
      end
    end
    if value == nil
      raise "Unable to find the '#{property}' value in #{filename}"
    end

    return value
  end
  
  def build_timestamp_id(prefix)
    return prefix + Time.now.strftime("%Y-%m-%d_%H-%M") + "_" + rand(100).to_s
  end
  
  def require_local_mysql_service?
    if @options[:mysqldump] == false
      true
    else
      false
    end
  end
end