module TungstenScript
  NAGIOS_OK=0
  NAGIOS_WARNING=1
  NAGIOS_CRITICAL=2
  
  def run
    unless @options[:validate] == true
      begin
        main()
      rescue => e
        TU.exception(e)
      end
    end
    
    if TU.is_valid?()
      cleanup(0)
    else
      cleanup(1)
    end
  end
  
  def initialize
    # Does this script required to run against an installed Tungsten directory
    @require_installed_directory = true
    
    # Definition of each command that this script will support
    @command_definitions = {}
    
    # The command, if any, the script should run
    @command = nil
    
    # Definition of each option that this script is expecting as input
    @option_definitions = {}
    
    # The command-line arguments of all options that have been defined
    # This is used to identify duplicate arguments
    @option_definition_arguments = {}
    
    # The collected option values from the script input
    @options = {}
    
    TU.debug("Begin #{$0} #{ARGV.join(' ')}")
    
    begin
      configure()
    
      if TU.display_help?()
        display_help()
        cleanup(0)
      end
    
      parse_options()
    
      unless TU.is_valid?()
        cleanup(1)
      end
    
      TU.debug("Options:")
      @options.each{
        |k,v|
        TU.debug("    #{k} => #{v}")
      }
    
      validate()
    
      unless TU.is_valid?()
        cleanup(1)
      end
    rescue => e
      TU.exception(e)
      cleanup(1)
    end
  end
  
  def command
    @command
  end
  
  def configure
    add_option(:validate, {
      :on => "--validate",
      :default => false,
      :help => "Only run the script validation"
    })
  end
  
  def opt(option_key, value = nil)
    if value != nil
      @options[option_key] = value
    end
    
    return @options[option_key]
  end
  
  def add_command(command_key, definition)
    begin
      command_key = command_key.to_sym()
      if @command_definitions.has_key?(command_key)
        raise "The #{command_key} command has already been defined"
      end

      if definition[:default] == true
        if @command != nil
          raise "Multiple commands have been specified as the default"
        end
        @command = command_key
      end

      @command_definitions[command_key] = definition
    rescue => e
      TU.exception(e)
    end
  end
  
  def add_option(option_key, definition, &parse)
    begin
      option_key = option_key.to_sym()
      if @option_definitions.has_key?(option_key)
        raise "The #{option_key} option has already been defined"
      end

      unless definition[:on].is_a?(Array)
        definition[:on] = [definition[:on]]
      end
      
      # Check if the arguments for this option overlap with any other options
      definition[:on].each{
        |arg|
        
        arg = arg.split(" ").shift()
        if @option_definition_arguments.has_key?(arg)
          raise "The #{arg} argument is already defined for this script"
        end
        @option_definition_arguments[arg] = true
      }

      if parse != nil
        definition[:parse] = parse
      end

      if definition.has_key?(:default)
        opt(option_key, definition[:default])
      end

      @option_definitions[option_key] = definition
    rescue => e
      TU.exception(e)
    end
  end
  
  def parse_options
    if @command_definitions.size() > 0 && TU.remaining_arguments.size() > 0
      if @command_definitions.has_key?(TU.remaining_arguments[0].to_sym())
        @command = TU.remaining_arguments.shift()
      end
    end
    
    opts = OptionParser.new()
    
    @option_definitions.each{
      |option_key,definition|
      
      args = definition[:on]
      opts.on(*args) {
        |val|
                
        if definition[:parse] != nil
          begin
            val = definition[:parse].call(val)
            
            unless val == nil
              opt(option_key, val)
            end
          rescue MessageError => me
            TU.error(me.message())
          end
        else  
          opt(option_key, val)
        end
      }
    }
    
    TU.run_option_parser(opts)
  end
  
  def parse_integer_option(val)
    val.to_i()
  end
  
  def parse_float_option(val)
    val.to_f()
  end
  
  def parse_boolean_option(val)
    if val == "true"
      true
    elsif val == "false"
      false
    else
      raise MessageError.new("Unable to parse value '#{val}'")
    end
  end
  
  def validate
    if require_installed_directory?()
      if TI == nil
        raise "Unable to run #{$0} without the '--directory' argument pointing to an active Tungsten installation"
      else
        TI.inherit_path()
      end
    end
    
    if @command_definitions.size() > 0 && @command == nil
      TU.error("A command was not given for this script. Valid commands are #{@command_definitions.keys().join(', ')} and must be the first argument.")
    end
  end
  
  def display_help
    unless description() == nil
      TU.output(TU.wrapped_lines(description()))
      TU.output("")
    end
    
    TU.display_help()
    
    if @command_definitions.size() > 0
      TU.write_header("Script Commands", nil)
      
      @command_definitions.each{
        |command_key,definition|
        
        if definition[:default] == true
          default = "default"
        else
          default = ""
        end
        
        TU.output_usage_line(command_key.to_s(), definition[:help], default)
      }
    end
    
    TU.write_header("Script Options", nil)
    
    @option_definitions.each{
      |option_key,definition|
      
      if definition[:help].is_a?(Array)
        help = definition[:help].shift()
        additional_help = definition[:help]
      else
        help = definition[:help]
        additional_help = []
      end
      
      TU.output_usage_line(definition[:on].join(","),
        help, definition[:default], nil, additional_help.join("\n"))
    }
  end
  
  def require_installed_directory?(v = nil)
    if (v != nil)
      @require_installed_directory = v
    end
    
    @require_installed_directory
  end
  
  def description(v = nil)
    if v != nil
      @description = v
    end
    
    @description || nil
  end
  
  def script_log_path
    nil
  end
  
  def cleanup(code = 0)
    if TU.display_help?() != true && script_log_path() != nil
      File.open(script_log_path(), "w") {
        |f|
        TU.log().rewind()
        f.puts(TU.log().read())
      }
    end
    
    TU.debug("Finish #{$0} #{ARGV.join(' ')}")
    TU.debug("RC: #{code}")
    exit(code)
  end
  
  def nagios_ok(msg)
    puts "OK: #{msg}"
    cleanup(NAGIOS_OK)
  end
  
  def nagios_warning(msg)
    puts "WARNING: #{msg}"
    cleanup(NAGIOS_WARNING)
  end
  
  def nagios_critical(msg)
    puts "CRITICAL: #{msg}"
    cleanup(NAGIOS_CRITICAL)
  end
  
  def sudo_prefix
    if ENV['USER'] == "root" || TI.setting("root_command_prefix") != "true"
      return ""
    else
      return "sudo -n "
    end
  end
end

module MySQLServiceScript
  def configure
    super()
    
    if TI
      add_option(:service, {
        :on => "--service String",
        :help => "Replication service to read information from",
        :default => TI.default_dataservice()
      })
    end
  end
  
  def require_mysql_service?
    false
  end
  
  def validate
    super()
    
    if @options[:host] == nil
      @options[:host] = TI.setting("repl_services.#{@options[:service]}.repl_datasource_host")
    end
    if @options[:port] == nil
      @options[:port] = TI.setting("repl_services.#{@options[:service]}.repl_datasource_port")
    end
    
    if @options[:my_cnf] == nil
      @options[:my_cnf] = TI.setting("repl_services.#{@options[:service]}.repl_datasource_mysql_service_conf")
    end
    if @options[:my_cnf] == nil
      TU.error "Unable to determine location of MySQL my.cnf file"
    else
      unless File.exist?(@options[:my_cnf])
        TU.error "The file #{@options[:my_cnf]} does not exist"
      end
    end
    
    if require_mysql_service?()
      if @options[:mysqluser] == nil
        @options[:mysqluser] = get_mysql_option("user")
      end
      if @options[:mysqluser].to_s() == ""
        @options[:mysqluser] = "mysql"
      end
    
      if @options[:mysql_service_command] == nil
        @options[:mysql_service_command] = TI.setting("repl_services.#{@options[:service]}.repl_datasource_boot_script")
      end
      if @options[:mysql_service_command] == nil
        begin
          service_command=TU.cmd_result("which service")
          if TU.cmd("#{sudo_prefix()}test -x #{service_command}")
            if TU.cmd("#{sudo_prefix()}test -x /etc/init.d/mysqld")
              @options[:mysql_service_command] = "#{service_command} mysqld"
            elsif TU.cmd("#{sudo_prefix()}test -x /etc/init.d/mysql")
              @options[:mysql_service_command] = "#{service_command} mysql"
            else
              TU.error "Unable to determine the service command to start/stop mysql"
            end
          else
            if TU.cmd("#{sudo_prefix()}test -x /etc/init.d/mysqld")
              @options[:mysql_service_command] = "/etc/init.d/mysqld"
            elsif TU.cmd("#{sudo_prefix()}test -x /etc/init.d/mysql")
              @options[:mysql_service_command] = "/etc/init.d/mysql"
            else
              TU.error "Unable to determine the service command to start/stop mysql"
            end
          end
        rescue CommandError
          TU.error "Unable to determine the service command to start/stop mysql"
        end
      end
    end
  end
  
  def get_mysql_command
    "mysql --defaults-file=#{@options[:my_cnf]} -h#{@options[:host]} --port=#{@options[:port]}"
  end
  
  def get_mysqldump_command
    "mysqldump --defaults-file=#{@options[:my_cnf]} --host=#{@options[:host]} --port=#{@options[:port]} --opt --single-transaction --all-databases --add-drop-database --master-data=2"
  end
  
  def get_xtrabackup_command
    # Use the configured my.cnf file, or the additional config file 
    # if we created one
    if @options[:extra_mysql_defaults_file] == nil
      defaults_file = @options[:my_cnf]
    else
      defaults_file = @options[:extra_mysql_defaults_file].path()
    end
    
    "innobackupex-1.5.1 --defaults-file=#{defaults_file} --host=#{@options[:host]} --port=#{@options[:port]}"
  end
  
  def get_mysql_result(command, timeout = 30)
    begin      
      Timeout.timeout(timeout.to_i()) {
        return TU.cmd_result("#{get_mysql_command()} -e \"#{command}\"")
      }
    rescue Timeout::Error
    rescue => e
    end
    
    return nil
  end
  
  def get_mysql_value(command, column = nil)
    response = get_mysql_result(command + "\\\\G")
    if response == nil
      return nil
    end
    
    response.split("\n").each{ | response_line |
      parts = response_line.chomp.split(":")
      if (parts.length != 2)
        next
      end
      parts[0] = parts[0].strip;
      parts[1] = parts[1].strip;
      
      if parts[0] == column || column == nil
        return parts[1]
      end
    }
    
    return nil
  end
  
  # Read the configured value for a mysql variable
  def get_mysql_option(opt)
    begin
      val = TU.cmd_result("my_print_defaults --config-file=#{@options[:my_cnf]} mysqld | grep -e'^--#{opt.gsub(/[\-\_]/, "[-_]")}'")
    rescue CommandError => ce
      return nil
    end

    return val.split("\n")[0].split("=")[1]
  end
  
  # Read the current value for a mysql variable
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
  
  # Store additional MySQL configuration values in a temporary file
  def set_mysql_defaults_value(value)
    if @options[:extra_mysql_defaults_file] == nil
      @options[:extra_mysql_defaults_file] = Tempfile.new("xtracfg")
      @options[:extra_mysql_defaults_file].puts("!include #{@options[:my_cnf]}")
      @options[:extra_mysql_defaults_file].puts("")
      @options[:extra_mysql_defaults_file].puts("[mysqld]")
    end
    
    @options[:extra_mysql_defaults_file].puts(value)
    @options[:extra_mysql_defaults_file].flush()
  end
  
  def start_mysql_server
    TU.notice("Start the MySQL service")
    begin
      TU.cmd_result("#{sudo_prefix()}#{@options[:mysql_service_command]} start")
    rescue CommandError
    end
    
    # Wait 30 seconds for the MySQL service to be responsive
    begin
      Timeout.timeout(30) {
        while true
          begin
            if get_mysql_result("SELECT 1") != nil
              break
            end
            
            # Pause for a second before running again
            sleep 1
          rescue
          end
        end
      }
    rescue Timeout::Error
      raise "The MySQL server has taken too long to start"
    end
  end
  
  # Make sure that the mysql server is stopped by stopping it and checking
  # the process has disappeared
  def stop_mysql_server
    TU.notice("Stop the MySQL service")
    begin
      pid_file = get_mysql_variable("pid_file")
      pid = TU.cmd_result("#{sudo_prefix()}cat #{pid_file}")
    rescue CommandError
      pid = ""
    end
    
    begin
      TU.cmd_result("#{sudo_prefix()}#{@options[:mysql_service_command]} stop")
    rescue CommandError
    end
    
    begin
      # Verify that the stop command worked properly
      # We are expecting an error so we have to catch the exception
      TU.cmd_result("#{get_mysql_command()} -e \"select 1\" > /dev/null 2>&1")
      raise "Unable to properly shutdown the MySQL service"
    rescue CommandError
    end
    
    # We saw issues where MySQL would not close completely. This will
    # watch the PID and make sure it does not appear
    unless pid.to_s() == ""
      begin
        TU.debug("Verify that the MySQL pid has gone away")
        Timeout.timeout(30) {
          pid_missing = false
          
          while pid_missing == false do
            begin
              TU.cmd_result("#{sudo_prefix()}ps -p #{pid}")
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
  end
end