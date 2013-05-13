class ConnectorTerminalCommand
  include ConfigureCommand
  include ClusterCommandModule

  def skip_prompts?
    true
  end
  
  def display_alive_thread?
    false
  end
  
  def require_dataservice?
    if Configurator.instance.is_locked?()
      false
    else
      true
    end
  end
  
  def parsed_options?(arguments)
    arguments = super(arguments)
    
    if Configurator.instance.display_help? && !Configurator.instance.display_preview?()
      return true
    end
    
    @display_samples = false
    opts = OptionParser.new()
    opts.on("--samples") { @display_samples = true }
    @terminal_args = Configurator.instance.run_option_parser(opts, arguments)
    
    []
  end
  
  def run
    if @display_samples == true
      display_samples()
    else
      get_connection()
    end
    
    return is_valid?()
  end
 
  def get_connection
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
  
  def display_samples
    if Configurator.instance.is_locked?()
      hostname=@config.getProperty(HOST)
      username=@config.getProperty(CONN_CLIENTLOGIN)
      password=@config.getProperty(CONN_CLIENTPASSWORD)
      port=@config.getProperty(CONN_LISTEN_PORT)
    else
      hostname=nil
      command_dataservices().each{
        |ds_alias|
        connectors = @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_CONNECTORS], "").split(",")
        if connectors.size() > 0
          hostname = connectors[0]
          username=@config.getProperty([CONNECTORS, to_identifier(ds_alias), CONN_CLIENTLOGIN])
          password=@config.getProperty([CONNECTORS, to_identifier(ds_alias), CONN_CLIENTPASSWORD])
          port=@config.getProperty([CONNECTORS, to_identifier(ds_alias), CONN_LISTEN_PORT])
          break
        end
      }
      
      if hostname == nil
        error("Unable to find a connector in the given dataservices")
        return
      end
    end
    
    output_usage_line("Bash", "mysql -h#{hostname} -P#{port} -u#{username} -P#{password}")
    output_usage_line("Perl::dbi", "$dbh=DBI->connecti('DBI:mysql:host=#{hostname};port=#{port}', '#{username}', '#{password}')")
    output_usage_line("PHP::mysqli", "$dbh = new mysqli('#{hostname}', '#{username}', '#{password}', 'schema', '#{port}');")
    output_usage_line("PHP::pdo", "$dbh = new PDO('mysql:host=#{hostname};port=#{port}', '#{username}', '#{password}');")
    output_usage_line("Python::mysql.connector", "dbh = mysql.connector.connect(user='#{username}', password='#{password}', host='#{hostname}', port=#{port}, database='schema')")
    output_usage_line("Java::DriverManager", "dbh=DriverManager.getConnection(\"jdbc:mysql://#{hostname}:#{port}/schema\", \"#{username}\", \"#{password}\")")
  end
  
  def output_command_usage
    super()
    
    output_usage_line("--samples", "Display methods to get a connection to the Tungsten Connector")
  end

  def self.get_command_name
    'connector'
  end
  
  def self.get_command_description
    "Open a terminal to the Connector"
  end
end