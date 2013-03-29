#!/usr/bin/env ruby

require "cgi"
require "optparse"
require "pp"
require "pathname"
require "fileutils"
require "date"

@op = nil
@properties = nil

@options = {
  :host => "localhost",
  :port => "3306",
  :tungsten_backups => nil,
  :gz => "false",
  :my_cnf => nil
}

def run
  log("Begin #{$0} #{ARGV.join(' ')}")
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
  
  log("Operation: #{@op}")
  log("Options:")
  @options.each{
    |k,v|
    log("    #{k} => #{v}")
  }
  
  if @op == :backup
    backup()
  elsif @op == :restore
    restore()
  else
    raise "You must specify -backup or -restore"
  end
  log("Finish #{$0} #{ARGV.join(' ')}")
  
  exit 0
end

def backup
  begin
    validate_backup_options()
    id = build_timestamp_id()
    
    if @options[:gz] == "true"
      mysqldump_file = @options[:tungsten_backups] + "/" + id + ".sql.gz"

      log("Create mysqldump in #{mysqldump_file}")
      cmd_result("echo \"-- Tungsten database dump - should not be logged on restore\n\" | gzip -c > #{mysqldump_file}");
      cmd_result("echo \"SET SESSION SQL_LOG_BIN=0;\n\" | gzip -c >> #{mysqldump_file}");
      cmd_result("#{get_mysqldump_command()} | gzip -c >> #{mysqldump_file}")
    else
      mysqldump_file = @options[:tungsten_backups] + "/" + id + ".sql"
      
      log("Create mysqldump in #{mysqldump_file}")
      cmd_result("echo \"-- Tungsten database dump - should not be logged on restore\n\" > #{mysqldump_file}");
      cmd_result("echo \"SET SESSION SQL_LOG_BIN=0;\n\" >> #{mysqldump_file}");
      cmd_result("#{get_mysqldump_command()} >> #{mysqldump_file}")
    end

    # Change the directory ownership if run with sudo
    if ENV.has_key?('SUDO_USER')
      cmd_result("chown -R #{ENV['SUDO_USER']}: #{mysqldump_file}")
    end

    # Write the directory name to the final storage file
    cmd_result("echo \"file=#{mysqldump_file}\" > #{@properties}")
  rescue => e
    log("Error: #{e.message}")

    if mysqldump_file && File.exists?(mysqldump_file)
      log("Remove #{mysqldump_file} due to the error")
      cmd_result("rm #{mysqldump_file}")
    end

    raise e
  end
end

def restore
  begin
    validate_restore_options()
    
    storage_file = cmd_result(". #{@properties}; echo $file")
    log("Restore from #{storage_file}")
    
    if File.extname(storage_file) == ".gz"
      cmd_result("gunzip -c #{storage_file} | #{get_mysql_command()}")
    else
      cmd_result("cat #{storage_file} | #{get_mysql_command()}")
    end
  rescue => e
    log("Error: #{e.message}")
    raise e
  end
end

def validate_options
  if @options[:tungsten_backups] == nil
    raise "You must specify the Tungsten backups storage directory"
  end
  
  unless File.writable?(@options[:tungsten_backups])
    raise "The directory '#{@options[:tungsten_backups]}' is not writeable"
  end
end

def validate_backup_options
  validate_options()
end

def validate_restore_options
  validate_options()
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

def build_timestamp_id()
  return "mysqldump_" + Time.now.strftime("%Y-%m-%d_%H-%M") + "_" + rand(100).to_s
end

def get_mysql_command
  "mysql --defaults-file=#{@options[:my_cnf]} -h#{@options[:host]} --port=#{@options[:port]}"
end

def get_mysqldump_command
  "mysqldump --defaults-file=#{@options[:my_cnf]} --host=#{@options[:host]} --port=#{@options[:port]} --opt --single-transaction --all-databases --add-drop-database --master-data=2"
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

def get_mysql_option(opt)
  begin
    val = cmd_result("my_print_defaults --config-file=#{@options[:my_cnf]} mysqld | grep -e'^--#{opt.gsub(/[\-\_]/, "[-_]")}'")
  rescue CommandError => ce
    if ce.rc == 256
      return nil
    else
      raise ce
    end
  end
  
  return val.split("=")[1]
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
  echo "Usage: mysqldump.rb {-backup|-restore} -properties file [-options opts]"
end

def log(msg)
  $stderr.puts(DateTime.now.to_s + " " + msg)
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