class MySQLTerminalCommand
  include ConfigureCommand
  include ClusterCommandModule

  def skip_prompts?
    true
  end
  
  def display_alive_thread?
    false
  end

  def parsed_options?(arguments)
    @service = nil
    arguments = super(arguments)

    if display_help?() && !display_preview?()
      return arguments
    end
  
    # Define extra option to load event.  
    opts=OptionParser.new
    
    opts.on("--service String")            {|s| @service = s}
    
    opts = Configurator.instance.run_option_parser(opts, arguments)

    # Return options. 
    opts
  end

  def output_command_usage
    super()
    output_usage_line("--service", "The service to use for a connection")
  end
  
  def get_bash_completion_arguments
    super() + ["--service"]
  end
 
  def run
    rs_alias = nil
    @config.getPropertyOr(REPL_SERVICES, {}).each_key{
      |rs|
      if @service != nil
        if @config.getProperty([REPL_SERVICES, rs, DEPLOYMENT_SERVICE]) == @service
          rs_alias = rs
          break
        end
      else
        rs_alias = rs
        break
      end
    }
    ds = ConfigureDatabasePlatform.build([REPL_SERVICES, rs_alias], @config)
    unless ds.is_a?(MySQLDatabasePlatform)
      error("Unable to open connection to non MySQL server")
      return false
    end
    
    exec("mysql --defaults-file=#{@config.getProperty([REPL_SERVICES, rs_alias, REPL_MYSQL_SERVICE_CONF])} --host=#{ds.host} --port=#{ds.port}")
  end

  def self.get_command_name
    'mysql'
  end
  
  def self.get_command_description
    "Open a MySQL terminal"
  end
end