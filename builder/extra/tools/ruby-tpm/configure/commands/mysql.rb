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
    if Configurator.instance.display_help? && !Configurator.instance.display_preview?()
      return true
    end
    
    @terminal_args = arguments
    
    []
  end
 
  def run
    unless @config.getProperty(HOST_ENABLE_REPLICATOR) == "true"
      error("Unable to run this command because the current host is not a database server")
      return false
    end
    
    if command_dataservices().size() > 0
      @service = command_dataservices()[0]
    else
      @service = nil
    end
    
    rs_alias = nil
    @config.getPropertyOr(REPL_SERVICES, {}).each_key{
      |rs|
      if rs == DEFAULTS
        next
      end
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
    
    exec("mysql --defaults-file=#{@config.getProperty([REPL_SERVICES, rs_alias, REPL_MYSQL_SERVICE_CONF])} --host=#{ds.host} --port=#{ds.port} #{@terminal_args.join(' ')}")
  end

  def self.get_command_name
    'mysql'
  end
  
  def self.get_command_description
    "Open a MySQL terminal"
  end
end