class ConnectorTerminalCommand
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
    unless @config.getProperty(HOST_ENABLE_CONNECTOR) == "true"
      error("Unable to run this command because the current host is not a connector")
      return false
    end
    
    conncnf = Tempfile.new("conncnf")
    conncnf.puts("[client]")
    conncnf.puts("user=#{@config.getProperty(CONN_CLIENTLOGIN)}")
    conncnf.puts("password=#{@config.getProperty(CONN_CLIENTPASSWORD)}")
    conncnf.close()
    
    exec("mysql --defaults-file=#{conncnf.path()} --host=#{@config.getProperty(HOST)} --port=#{@config.getProperty(CONN_LISTEN_PORT)} #{@terminal_args.join(' ')}")
  end

  def self.get_command_name
    'connector'
  end
  
  def self.get_command_description
    "Open a terminal to the Connector"
  end
end