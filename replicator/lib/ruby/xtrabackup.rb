require "#{File.dirname(__FILE__)}/backup"

class TungstenXtrabackupScript < TungstenBackupScript
  INCREMENTAL_BASEDIR_FILE = "xtrabackup_incremental_basedir"
  
  def configure
    super()
    
    add_option(:tar, {
      :on => "--tar",
      :default => false,
      :help => "Create the backup as a TAR file"
    })
    
    add_option(:restore_to_datadir, {
      :on => "--restore-to-datadir",
      :default => false,
      :help => "Use the MySQL data directory for staging and preparation"
    })
    
    add_option(:incremental, {
      :on => "--incremental",
      :default => false,
      :help => "Create the backup as an incremental snapshot since last backup"
    })
  end
  
  def backup
    if @options[:tar] == "true"
      backup_tar()
    else
      backup_dir()
    end
  end
  
  def backup_tar()
    begin
      cleanup_xtrabackup_storage()

      id = build_timestamp_id("full")
      tar_file = @options[:directory] + "/" + id + ".tar"

      additional_args = []
      additional_args << "--no-timestamp"
      additional_args << "--stream=tar"

      TU.output("Create full backup in #{tar_file}")  
      TU.cmd_stderr("#{get_xtrabackup_command()} #{additional_args.join(" ")} #{@options[:directory]} > #{tar_file}") {
        |line|
        if line =~ /binlog/
          m = line.match(/filename \'([a-zA-Z0-9\.\-\_]*)\', position ([0-9]*)/)
          if m
            @binlog_file = m[1]
            @binlog_position = m[2]
          end
        end
      }

      # Change the directory ownership if run with sudo
      if ENV.has_key?('SUDO_USER')
        TU.cmd_result("chown -R #{ENV['SUDO_USER']}: #{tar_file}")
      end

      # Write the directory name to the final storage file
      TU.cmd_result("echo \"file=#{tar_file}\" > #{@options[:properties]}")
    rescue => e
      TU.error(e.message)

      if tar_file && File.exists?(tar_file)
        TU.output("Remove #{tar_file} due to the error")
        TU.cmd_result("rm #{tar_file}")
      end

      raise e
    end
  end
  
  def backup_dir
    begin
      cleanup_xtrabackup_storage()

      if @options[:incremental] == "false"
        execute_backup()
      else
        begin
          # Find the most recent xtrabackup directory which we will start from
          most_recent_dir = get_last_backup()

          # Check that the lineage for this directory is intact
          # If it cannot find the full backup that the most recent snapshot is
          # based on, we need to do a full backup instead.
          lineage = get_snapshot_lineage(most_recent_dir)

          execute_backup(most_recent_dir)
        rescue BrokenLineageError => ble
          TU.error(ble.message)
          execute_backup()
        end
      end

      # Tungsten Replicator requires a single file as the result of this script.
      # We write the directory name of the backup just created into a file
      # and present that as the backup result.  The restore command will read
      # the backup directory from the file to identify the proper restore point.
      # We are using the basename of the backup directory so it is easier to
      # identify which files are related.
      storage_file = @storage_dir + "/" + File.basename(@storage_dir)
      File.open(storage_file, "w") {
        |tm|
        tm.write(@storage_dir)
      }  

      # Change the directory ownership if run with sudo
      if ENV.has_key?('SUDO_USER')
        TU.cmd_result("chown -R #{ENV['SUDO_USER']}: #{@storage_dir}")
      end

      # Write the directory name to the final storage file
      TU.cmd_result("echo \"file=#{storage_file}\" > #{@options[:properties]}")
    rescue => e
      TU.error(e.message)

      if @storage_dir && File.exists?(@storage_dir)
        TU.output("Remove #{@storage_dir} due to the error")
        TU.cmd_result("rm -r #{@storage_dir}")
      end

      raise e
    end
  end
  
  def execute_backup(incremental_basedir = nil)
    id = build_timestamp_id((incremental_basedir == nil ? "full" : "incr"))
    @storage_dir = @options[:directory] + "/" + id

    additional_args = []
    additional_args << "--no-timestamp"

    if incremental_basedir == nil
      TU.output("Create full backup in #{@storage_dir}")
      TU.cmd_stderr("#{get_xtrabackup_command()} #{additional_args.join(" ")} #{@storage_dir}") {
        |line|
        if line =~ /binlog/
          m = line.match(/filename \'([a-zA-Z0-9\.\-\_]*)\', position ([0-9]*)/)
          if m
            @binlog_file = m[1]
            @binlog_position = m[2]
          end
        end
      }
    else
      additional_args << "--incremental"

      incremental_lsn = read_property_from_file("to_lsn", incremental_basedir.to_s + "/xtrabackup_checkpoints")
      additional_args << "--incremental-lsn=#{incremental_lsn}"

      TU.output("Create an incremental backup from LSN #{incremental_lsn} in #{@storage_dir}")
      # Copy the database files and apply any pending log entries
      TU.cmd_result("#{get_xtrabackup_command()} #{additional_args.join(" ")} #{@storage_dir}")

      File.symlink(incremental_basedir, @storage_dir + "/#{INCREMENTAL_BASEDIR_FILE}")
    end
  end
  
  def restore
    begin
      storage_file = TU.cmd_result(". #{@options[:properties]}; echo $file")

      # If the tungsten_restore_to_datadir file exists, we will restore to the
      # datadir. This can't work if innodb_data_home_dir or innodb_log_group_home_dir 
      # are in the my.cnf file because the files need to go to different directories
      if File.exist?("#{@options[:mysqldatadir]}/tungsten_restore_to_datadir")
        @options[:restore_to_datadir] = "true"
      end

      if @options[:restore_to_datadir] == "true"  
        if @options[:mysqlibdatadir].to_s() != "" || @options[:mysqliblogdir].to_s() != ""
          raise("Unable to restore to #{@options[:mysqldatadir]} because #{@options[:my_cnf]} includes a definition for 'innodb_data_home_dir' or 'innodb_log_group_home_dir'")
        end
      end

      # Does this version of innobackupex-1.5.1 support the faster 
      # --move-back instead of --copy-back
      supports_move_back = TU.cmd_result("#{get_xtrabackup_command()} --help | grep -e\"\-\-move\-back\" | wc -l")
      if supports_move_back == "1"
        supports_move_back = true
      else
        supports_move_back = false
      end

      id = build_timestamp_id("restore")
      if @options[:restore_to_datadir] == "true"
        empty_mysql_directory()

        staging_dir = @options[:mysqldatadir]
      else
        staging_dir = @options[:directory] + "/" + id
      end
      FileUtils.mkdir_p(staging_dir)

      if @options[:tar] == "true"
        TU.output("Unpack '#{storage_file}' to the staging directory '#{staging_dir}'")
        TU.cmd_result("cd #{staging_dir}; tar -xif #{storage_file}")
      else
        restore_directory = TU.cmd_result("cat #{storage_file}")

        TU.debug("Restore from #{restore_directory}")

        lineage = get_snapshot_lineage(restore_directory)
        fullbackup_dir = lineage.shift()
        TU.output("Copy the full base directory '#{fullbackup_dir}' to the staging directory '#{staging_dir}'")
        TU.cmd_result("cp -r #{fullbackup_dir}/* #{staging_dir}")

        TU.output("Apply the redo-log to #{staging_dir}")
        TU.cmd_result("#{get_xtrabackup_command()} --apply-log --redo-only #{staging_dir}")

        lineage.each{
          |incremental_dir|
          TU.output("Apply the incremental updates from #{incremental_dir}")
          TU.cmd_result("#{get_xtrabackup_command()} --apply-log --incremental-dir=#{incremental_dir} #{staging_dir}")
        }
      end

      TU.output("Finish preparing #{staging_dir}")
      TU.cmd_result("#{get_xtrabackup_command()} --apply-log #{staging_dir}")

      unless @options[:restore_to_datadir] == "true"
        empty_mysql_directory()

        # Copy the backup files to the mysql data directory
        if supports_move_back == true
          restore_cmd = "--move-back"
        else
          restore_cmd = "--copy-back"
        end
        TU.cmd_result("#{get_xtrabackup_command()} #{restore_cmd} #{staging_dir}")
      end

      # Fix the permissions and restart the service
      TU.cmd_result("chown -RL #{@options[:mysqluser]}: #{@options[:mysqldatadir]}")

      if @options[:mysqlibdatadir].to_s() != ""
        TU.cmd_result("chown -RL #{@options[:mysqluser]}: #{@options[:mysqlibdatadir]}")
      end

      if @options[:mysqliblogdir].to_s() != ""
        TU.cmd_result("chown -RL #{@options[:mysqluser]}: #{@options[:mysqliblogdir]}")
      end

      TU.cmd_result("#{@options[:mysql_service_command]} start")

      if @options[:restore_to_datadir] == "false" && staging_dir != "" && File.exists?(staging_dir)
        TU.output("Cleanup #{staging_dir}")
        TU.cmd_result("rm -r #{staging_dir}")
      elsif @options[:restore_to_datadir] == "true" && File.exists?("#{staging_dir}/#{INCREMENTAL_BASEDIR_FILE}")
        TU.cmd_result("rm -f #{staging_dir}/#{INCREMENTAL_BASEDIR_FILE}")
      end
    rescue => e
      TU.error(e.message)

      if @options[:restore_to_datadir] == "false" && staging_dir != "" && File.exists?(staging_dir)
        TU.output("Remove #{staging_dir} due to the error")
        TU.cmd_result("rm -r #{staging_dir}")
      elsif @options[:restore_to_datadir] == "true" && File.exists?("#{staging_dir}/#{INCREMENTAL_BASEDIR_FILE}")
        TU.cmd_result("rm -f #{staging_dir}/#{INCREMENTAL_BASEDIR_FILE}")
      end

      raise e
    end
  end
  
  def validate
    super()
    
    # Read data locations from the my.cnf file
    @options[:mysqldatadir] = get_mysql_option("datadir")
    @options[:mysqlibdatadir] = get_mysql_option("innodb_data_home_dir")
    @options[:mysqliblogdir] = get_mysql_option("innodb_log_group_home_dir")
    @options[:mysqluser] = get_mysql_option("user")
    if @options[:mysqluser].to_s() == ""
      @options[:mysqluser] = "mysql"
    end
    
    @storage_dir = nil
    
    # Make sure the xtrabackup storage directory is created properly
    if File.exist?(@options[:directory])
      unless File.directory?(@options[:directory])
        TU.error "The path #{@options[:directory]} is not a directory"
      end
    else
      FileUtils.mkdir_p(@options[:directory])
      # Change the directory ownership if run with sudo
      if ENV.has_key?('SUDO_USER')
        TU.cmd_result("chown -R #{ENV['SUDO_USER']}: #{@options[:directory]}")
      end
    end

    if @options[:my_cnf] == nil
      TU.error "Unable to determine location of MySQL my.cnf file"
    else
      unless File.exist?(@options[:my_cnf])
        TU.error "The file #{@options[:my_cnf]} does not exist"
      end
    end
    
    if @options[:directory] == nil
      TU.error "You must specify a directory for storing Xtrabackup files"
    end

    unless File.writable?(@options[:directory])
      TU.error "The directory '#{@options[:directory]}' is not writeable"
    end

    if @options[:tungsten_backups] == nil
      TU.error "You must specify the Tungsten backups storage directory"
    else
      unless File.writable?(@options[:tungsten_backups])
        TU.error "The directory '#{@options[:tungsten_backups]}' is not writeable"
      end
    end

    unless File.writable?(@options[:mysqllogdir])
      TU.error "The MySQL log dir '#{@options[:mysqllogdir]}' is not writeable"
    end

    unless File.writable?(@options[:mysqldatadir])
      TU.error "The MySQL data dir '#{@options[:mysqldatadir]}' is not writeable"
    end
    
    if @options[:action] == ACTION_RESTORE
      if @options[:mysql_service_command] == nil
        service_command=cmd_result("which service")
        if File.executable?(service_command)
          if File.executable?("/etc/init.d/mysqld")
            @options[:mysql_service_command] = "#{service_command} mysqld"
          elsif File.executable?("/etc/init.d/mysql")
            @options[:mysql_service_command] = "#{service_command} mysql"
          else
            TU.error "Unable to determine the service command to start/stop mysql"
          end
        else
          if File.executable?("/etc/init.d/mysqld")
            @options[:mysql_service_command] = "/etc/init.d/mysqld"
          elsif File.executable?("/etc/init.d/mysql")
            @options[:mysql_service_command] = "/etc/init.d/mysql"
          else
            TU.error "Unable to determine the init.d command to start/stop mysql"
          end
        end
      end
    elsif @options[:action] == ACTION_BACKUP
      if @master_backup == true && @options[:incremental] == "true"
        TU.error("Unable to take an incremental backup of the master. Try running `trepctl -service #{@options[:service]} -backup xtrabackup-full")
      end
    end
  end
  
  def empty_mysql_directory
    begin
      pid_file = get_mysql_variable("pid_file")
      pid = TU.cmd_result("cat #{pid_file}")
    rescue CommandError
      pid = ""
    end
    
    TU.output("Stop the MySQL server")
    TU.cmd_result("#{@options[:mysql_service_command]} stop")

    begin
      # Verify that the stop command worked properly
      # We are expecting an error so we have to catch the exception
      TU.cmd_result("#{get_mysql_command()} -e \"select 1\" > /dev/null 2>&1")
      raise "Unable to properly shutdown the MySQL service"
    rescue CommandError
    end
    
    unless pid.to_s() == ""
      begin
        TU.output("Verify that the MySQL pid has gone away")
        Timeout.timeout(30) {
          pid_missing = false
          
          while pid_missing == false do
            begin
              TU.cmd_result("ps -p #{pid}")
              sleep 5
            rescue CommandError
              pid_missing = true
            end
          end
        }
      rescue Timeout::Error
        raise "Unable to verify that MySQL has fully shutdown"
      end
    end

    TU.cmd_result("rm -rf #{@options[:mysqldatadir]}/*")
    TU.cmd_result("rm -rf #{@options[:mysqllogdir]}/#{@options[:mysqllogpattern]}.*")

    if @options[:mysqlibdatadir].to_s() != ""
      TU.cmd_result("rm -rf #{@options[:mysqlibdatadir]}/*")
    end

    if @options[:mysqliblogdir].to_s() != ""
      TU.cmd_result("rm -rf #{@options[:mysqliblogdir]}/*")
    end
  end

  # Cleanup xtrabackup snapshots that no longer have a matching entry in 
  # the Tungsten backups directory.  If the Xtrabackup directory does not have 
  # a file in the Tungsten backups directory with a matching filename, we will 
  # remove the Xtrabackup snapshot
  def cleanup_xtrabackup_storage
    # Loop over each of the Tungsten backup storage files
    tungsten_storage_files = Pathname.new(@options[:tungsten_backups]).children.collect{
      |child|
      child.to_s
    }

    # Loop over each of the Xtrabackup storage directories
    Pathname.new(@options[:directory]).children.each{
      |xtrabackup_dir|
      basename = xtrabackup_dir.basename.to_s
      unless basename =~ /^full/ || basename =~ /^incr/
        next
      end

      regex = Regexp.new("store-[0-9]+-#{basename}")

      tungsten_storage_matches = tungsten_storage_files.select{
        |tungsten_storage_name|
        (tungsten_storage_name =~ regex)
      }    
      if tungsten_storage_matches.length == 0
        # There aren't any matching files in the Tungsten backups directory
        TU.cmd_result("rm -r #{xtrabackup_dir.to_s}")
      end
    }
  end
  
  def get_last_backup
    last_backup = Pathname.new(@options[:directory]).most_recent_dir()
    if last_backup == nil
      raise BrokenLineageError.new "Unable to find a previous directory for an incremental backup"
    end

    return last_backup
  end

  def get_snapshot_lineage(restore_directory)
    lineage = []

    TU.output("Validate lineage of '#{restore_directory}'")

    basedir_symlink = restore_directory.to_s + "/" + INCREMENTAL_BASEDIR_FILE
    checkpoints_file = restore_directory.to_s + "/xtrabackup_checkpoints"
    backup_type = read_property_from_file("backup_type", checkpoints_file)

    if backup_type == "full-backuped"
      if File.exists?(basedir_symlink)
        raise BrokenLineageError.new "Unexpected #{INCREMENTAL_BASEDIR_FILE} symlink found in full backup directory '#{restore_directory}'"
      end
      lineage << restore_directory
    elsif backup_type == "incremental"
      unless File.exists?(basedir_symlink) && File.symlink?(basedir_symlink)
        raise BrokenLineageError.new "Unable to find #{INCREMENTAL_BASEDIR_FILE} symlink in incremental backup directory '#{restore_directory}'"
      end

      basedir = File.readlink(basedir_symlink)
      lineage = get_snapshot_lineage(basedir)
      lineage << restore_directory
    else
      raise BrokenLineageError.new "Invalid backup_type '#{backup_type}' found in #{checkpoints_file}"
    end

    return lineage
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
    return prefix + "_xtrabackup_" + Time.now.strftime("%Y-%m-%d_%H-%M") + "_" + rand(100).to_s
  end

  def get_mysql_command
    "mysql --defaults-file=#{@options[:my_cnf]} -h#{@options[:host]} --port=#{@options[:port]}"
  end

  def get_xtrabackup_command
    "innobackupex-1.5.1 --defaults-file=#{@options[:my_cnf]} --host=#{@options[:host]} --port=#{@options[:port]} --ibbackup=xtrabackup_51"
  end
  
  def get_mysql_option(opt)
    begin
      val = TU.cmd_result("my_print_defaults --config-file=#{@options[:my_cnf]} mysqld | grep -e'^--#{opt.gsub(/[\-\_]/, "[-_]")}'")
    rescue CommandError => ce
      return nil
    end

    return val.split("\n")[0].split("=")[1]
  end
  
  def get_mysql_variable(var)
    response = TU.cmd_result("#{get_mysql_command()} -e \"SHOW VARIABLES LIKE '#{var}'\\\\G\"")
    
    response.split("\n").each{ | response_line |
      parts = response_line.chomp.split(":")
      if (parts.length != 2)
        next
      end
      parts[0] = parts[0].strip;
      parts[1] = parts[1].strip;
      
      if parts[0] == "Value"
        return parts[1]
      end
    }
    
    return nil
  end
end

class BrokenLineageError < StandardError
end

class Pathname
  def most_recent_dir(matching=/./)
    dirs = self.children.collect { |entry| self+entry }
    dirs.reject! { |entry| ((entry.directory? and entry != nil and entry.to_s =~ matching) ? false : true) }
    dirs.sort! { |entry1,entry2| entry1.mtime <=> entry2.mtime }
    dirs.last
  end
end