require "#{File.dirname(__FILE__)}/backup"

class TungstenXtrabackupToSlaveScript < TungstenBackupScript
  include MySQLServiceScript
  
  MASTER_BACKUP_POSITION_SQL = "xtrabackup_tungsten_master_backup_position.sql"
  
  def backup
    begin
      @binlog_file = nil
      @binlog_position = nil
      
      staging_dir = TI.setting("temp_directory") + "/" + build_timestamp_id("backup")
      TU.mkdir_if_absent(staging_dir)
      
      additional_args = []
      additional_args << "--no-timestamp"
      additional_args << "--stream=tar"
      
      if xtrabackup_supports_argument("--no-version-check")
        additional_args << "--no-version-check"
      end

      # Build the command and run it
      # All STDERR output from the command is processed before going to STDERR
      # When the MySQL binlog position is found, it is saved for later use
      TU.notice("Run innobackupex-1.5.1 sending the output to #{@options[:target]}:#{@options[:storage_directory]}")
      TU.forward_cmd_results?(true)
      TU.cmd_stderr("cd #{TI.setting("temp_directory")}; #{sudo_prefix()}#{get_xtrabackup_command()} #{additional_args.join(" ")} #{staging_dir} | ssh #{TU.get_ssh_command_options()} #{@options[:target]} \"mkdir -p #{@options[:storage_directory]}; rm -rf #{@options[:storage_directory]}/*; cd #{@options[:storage_directory]}; tar -xi\"") {
        |line|
        if line =~ /MySQL binlog position/
          m = line.match(/filename \'([a-zA-Z0-9\.\-\_]*)\', position ([0-9]*)/)
          if m
            @binlog_file = m[1]
            @binlog_position = m[2]
          end
        end
      }
      TU.forward_cmd_results?(false)
      
      # There are extra files that some versions of Xtrabackup leave out of 
      # the tar stream. We need to transfer those across
      TU.notice("Transfer extra files to #{@options[:target]}:#{@options[:storage_directory]}")
      TU.cmd_result("rsync -aze \"ssh #{TU.get_ssh_command_options()}\" #{staging_dir}/ #{@options[:target]}:#{@options[:storage_directory]}")
      
      if File.exist?(staging_dir)
        TU.cmd_result("#{sudo_prefix()}rm -rf #{staging_dir}")
      end
    rescue => e
      if staging_dir && File.exist?(staging_dir)
        TU.cmd_result("#{sudo_prefix()}rm -rf #{staging_dir}")
      end
      
      raise e
    end
  end
  
  def validate
    super()
    
    unless TI.is_replicator?()
      TU.error("This server is not configured for replication")
    end
    
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
    
    if @options[:mysqldatadir].to_s() == ""
      TU.error "The configuration file at #{@options[:my_cnf]} does not define a datadir value."
    end
    
    if @options[:target].to_s() == ""
      TU.error("The --target argument is required")
    else
      TU.test_ssh(@options[:target], TI.user())
    end
    
    path = TU.cmd_result("#{sudo_prefix()}which innobackupex-1.5.1 2>/dev/null", true)
    if path == ""
      TU.error("Unable to find the innobackupex-1.5.1 script using sudo")
    end
  end
  
  def configure
    super()
    
    add_option(:target, {
      :on => "--target String",
      :help => "Server to send the backup to"
    })
    
    add_option(:storage_directory, {
      :on => "--storage-directory String",
      :help => "Directory to place the backup in"
    })
  end
  
  def store_master_position_sql(sql, storage_file)
    TU.notice("Write master backup position information to #{@options[:target]}:#{@options[:storage_directory]}")
    sql.each{
      |line|
      TU.ssh_result("echo \"#{line}\" >> #{@options[:storage_directory]}/#{MASTER_BACKUP_POSITION_SQL}", @options[:target], TI.user())
    }
  end
  
  def build_timestamp_id(prefix)
    return prefix + "_xtrabackup_" + Time.now.strftime("%Y-%m-%d_%H-%M") + "_" + rand(100).to_s
  end
  
  def require_local_mysql_service?
    true
  end
end