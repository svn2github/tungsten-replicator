#!/usr/bin/env ruby

require "cgi"
require "optparse"
require "pp"
require "pathname"

INCREMENTAL_BASEDIR_FILE = "xtrabackup_incremental_basedir"

@op = nil
@properties = nil

@options = {
  :user => "root",
  :password => nil,
  :host => "localhost",
  :port => "3306",
  :directory => nil,
  :tungsten_backups => nil,
  :mysql_service_command => nil,
  :mysqldatadir => "/var/lib/mysql",
  :mysqllogdir => "/var/lib/mysql",
  :mysqllogpattern => "mysql-bin",
  :mysqluser => "mysql",
  :mysqlgroup => "mysql",
  :incremental => "false"
}

def run
  # Convert arguments with a single dash to have two
  args = ARGV.dup
  args.map!{ |arg|
    # The backup agent sends single dashes instead of double dashes
    if arg[0,1] == "-" && arg[0,2] != "--"
      "-" + arg
    else
      arg
    end
  }
  
  opts=OptionParser.new
  opts.on("--backup")  { @op = :backup }
  opts.on("--restore") { @op = :restore }
  opts.on("--properties String") {|val| 
    @properties = val
  }
  opts.on("--options String")  {|val|
    CGI::parse(val).each{
      |key,value|
      sym = key.to_sym
      if value.is_a?(Array)
        @options[sym] = value.at(0).to_s
      else
        @options[sym] = value.to_s
      end
    }
  }
  opts.parse!(args)
  
  if @op == :backup
    backup()
  elsif @op == :restore
    restore()
  else
    raise "You must specify -backup or -restore"
  end
  
  exit 0
end

def backup
  begin
    validate_backup_options()
    cleanup_xtrabackup_storage()

    if @options[:incremental] == "false"
      storage_dir = execute_backup()
    else
      begin
        # Find the most recent xtrabackup directory which we will start from
        most_recent_dir = get_last_backup()
        
        # Check that the lineage for this directory is intact
        # If it cannot find the full backup that the most recent snapshot is
        # based on, we need to do a full backup instead.
        lineage = get_snapshot_lineage(most_recent_dir)
        
        storage_dir = execute_backup(most_recent_dir)
      rescue BrokenLineageError => ble
        log(ble.message)
        storage_dir = execute_backup()
      end
    end
  
    # Tungsten Replicator requires a single file as the result of this script.
    # We write the directory name of the backup just created into a file
    # and present that as the backup result.  The restore command will read
    # the backup directory from the file to identify the proper restore point.
    # We are using the basename of the backup directory so it is easier to
    # identify which files are related.
    storage_file = storage_dir + "/" + File.basename(storage_dir)
    File.open(storage_file, "w") {
      |tm|
      tm.write(storage_dir)
    }  
    cmd_result("echo \"file=#{storage_file}\" > #{@properties}")
  rescue => e
    log("Error: #{e.message}")
    
    if storage_dir && File.exists?(storage_dir)
      log("Remove #{storage_dir} due to the error")
      cmd_result("rm -r #{storage_dir}")
    end
    
    raise e
  end
end

def execute_backup(incremental_basedir = nil)
  id = build_timestamp_id((incremental_basedir == nil ? "full" : "incr"))
  storage_dir = @options[:directory] + "/" + id
  
  if incremental_basedir == nil
    log("Create full backup in #{storage_dir}")
    cmd_result("#{get_xtrabackup_command()} --no-timestamp #{storage_dir}")
  else
    incremental_lsn = read_property_from_file("to_lsn", incremental_basedir.to_s + "/xtrabackup_checkpoints")
    
    log("Create an incremental backup from LSN #{incremental_lsn} in #{storage_dir}")
    # Copy the database files and apply any pending log entries
    cmd_result("#{get_xtrabackup_command()} --no-timestamp #{storage_dir} --incremental --incremental-lsn=#{incremental_lsn}")
    
    File.symlink(incremental_basedir, storage_dir + "/#{INCREMENTAL_BASEDIR_FILE}")
  end
  
  return storage_dir
end

def restore
  begin
    validate_restore_options()
    
    storage_file = cmd_result(". #{@properties}; echo $file")
    restore_directory = cmd_result("cat #{storage_file}")
    
    log("Restore from #{restore_directory}")
    lineage = get_snapshot_lineage(restore_directory)

    id = build_timestamp_id("restore")
    staging_dir = @options[:directory] + "/" + id
    
    fullbackup_dir = lineage.shift()
    log("Copy the full base directory '#{fullbackup_dir}' to the staging directory '#{staging_dir}'")
    cmd_result("cp -r #{fullbackup_dir} #{staging_dir}")
    log("Apply the redo-log to #{staging_dir}")
    cmd_result("#{get_xtrabackup_command()} --apply-log --redo-only  #{staging_dir}")
    
    lineage.each{
      |incremental_dir|
      log("Apply the incremental updates from #{incremental_dir}")
      cmd_result("#{get_xtrabackup_command()} --apply-log --incremental-dir=#{incremental_dir} #{staging_dir}")
    }
    
    cmd_result("#{get_xtrabackup_command()} --apply-log #{staging_dir}")
    
    log("Stop the MySQL server")
    cmd_result("#{@options[:mysql_service_command]} stop")

    begin
      # Verify that the stop command worked properly
      # We are expecting an error so we have to catch the exception
      cmd_result("mysql -u#{@options[:user]} -p#{@options[:password]} -h#{@options[:host]} --port=#{@options[:port]} -e \"select 1\" > /dev/null 2>&1")
      raise "Unable to properly shutdown the MySQL service"
    rescue CommandError
    end
  
    cmd_result("rm -rf #{@options[:mysqldatadir]}/*")
    cmd_result("rm -rf #{@options[:mysqllogdir]}/#{@options[:mysqllogpattern]}.*")
  
    # Copy the backup files to the mysql data directory
    cmd_result("innobackupex-1.5.1 --ibbackup=xtrabackup_51 --copy-back #{staging_dir}")

    # Fix the permissions and restart the service
    cmd_result("chown -RL #{@options[:mysqluser]}:#{@options[:mysqlgroup]} #{@options[:mysqldatadir]}")
    cmd_result("#{@options[:mysql_service_command]} start")
    
    if staging_dir != "" && File.exists?(staging_dir)
      log("Cleanup #{staging_dir}")
      cmd_result("rm -r #{staging_dir}")
    end
  rescue => e
    log("Error: #{e.message}")
    
    if staging_dir != "" && File.exists?(staging_dir)
      log("Remove #{staging_dir} due to the error")
      cmd_result("rm -r #{staging_dir}")
    end
    
    raise e
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
      cmd_result("rm -r #{xtrabackup_dir.to_s}")
    end
  }
end

def validate_options
  if @options[:directory] == nil
    raise "You must specify a directory for storing Xtrabackup files"
  end
  
  unless File.writable?(@options[:directory])
    raise "The directory '#{@options[:directory]}' is not writeable"
  end
  
  if @options[:tungsten_backups] == nil
    raise "You must specify the Tungsten backups storage directory"
  end
  
  unless File.writable?(@options[:tungsten_backups])
    raise "The directory '#{@options[:tungsten_backups]}' is not writeable"
  end
  
  unless File.writable?(@options[:mysqllogdir])
    raise "The MySQL log dir '#{@options[:mysqllogdir]}' is not writeable"
  end
  
  unless File.writable?(@options[:mysqldatadir])
    raise "The MySQL data dir '#{@options[:mysqldatadir]}' is not writeable"
  end
end

def validate_backup_options
  validate_options()
end

def validate_restore_options
  validate_options()
  
  if @options[:mysql_service_command] == nil
    service_command=cmd_result("which service")
    if File.executable?(service_command)
      if File.executable?("/etc/init.d/mysqld")
        @options[:mysql_service_command] = "#{service_command} mysqld"
      elsif File.executable?("/etc/init.d/mysql")
        @options[:mysql_service_command] = "#{service_command} mysql"
      else
        raise "Unable to determine the service command to start/stop mysql"
      end
    else
      if File.executable?("/etc/init.d/mysqld")
        @options[:mysql_service_command] = "/etc/init.d/mysqld"
      elsif File.executable?("/etc/init.d/mysql")
        @options[:mysql_service_command] = "/etc/init.d/mysql"
      else
        raise "Unable to determine the init.d command to start/stop mysql"
      end
    end
  end
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
  
  log("Validate lineage of '#{restore_directory}'")

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

def get_xtrabackup_command
  "innobackupex-1.5.1 --user=#{@options[:user]} --password=#{@options[:password]} --host=#{@options[:host]} --port=#{@options[:port]}"
end

def cmd_result(command)
  log("Execute `#{command}`")
  result = `#{command}`.chomp
  rc = $?
  
  log("Exit Code: #{rc}")
  log("Result: #{result}")
  
  if rc != 0
    raise CommandError.new(command, rc, result)
  end
  
  return result
end

class CommandError < StandardError
  attr_reader :command, :rc, :result
  
  def initialize(command, rc, result)
    @command = command
    @rc = rc
    @result = result
    
    super(build_message())
  end
  
  def build_message
    "Failed: #{command}, RC: #{rc}, Result: #{result}"
  end
end

class BrokenLineageError < StandardError
end

def usage()
  echo "Usage: xtrabackup.rb {-backup|-restore} -properties file [-options opts]"
end

def log(msg)
  $stderr.puts(msg)
end

class Pathname
  def most_recent_dir(matching=/./)
    dirs = self.children.collect { |entry| self+entry }
    dirs.reject! { |entry| ((entry.directory? and entry != nil and entry.to_s =~ matching) ? false : true) }
    dirs.sort! { |entry1,entry2| entry1.mtime <=> entry2.mtime }
    dirs.last
  end
end

run()