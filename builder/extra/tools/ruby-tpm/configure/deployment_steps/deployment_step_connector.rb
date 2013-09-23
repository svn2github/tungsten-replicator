module ConfigureDeploymentStepConnector
  def get_methods
    [
      ConfigureDeploymentMethod.new("deploy_connector"),
    ]
  end
  module_function :get_methods
  
  def deploy_connector
    unless is_connector?()
      info("Tungsten Connector is not active; skipping connector configuration")
      return
    end
    
    Configurator.instance.write_header("Performing Tungsten Connector configuration...")
    transformer = Transformer.new(
		  "#{get_deployment_basedir()}/tungsten-connector/samples/conf/connector.properties.tpl",
			"#{get_deployment_basedir()}/tungsten-connector/conf/connector.properties", "#")
	  transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
    transformer.transform_values(method(:transform_values))
    transformer.output
    watch_file(transformer.get_filename())
    
    if @config.getPropertyOr(CONN_RO_LISTEN_PORT) != "" && @config.getProperty(ENABLE_CONNECTOR_RO) != "true"
      ConfigureDeploymentStepConnector.connector_ro_mode?(true)
      transformer = Transformer.new(
  		  "#{get_deployment_basedir()}/tungsten-connector/samples/conf/connector.properties.tpl",
  			"#{get_deployment_basedir()}/tungsten-connector/conf/connector.ro.properties", "#")
  	  transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
      transformer.transform_values(method(:transform_values))
      transformer.output
      watch_file(transformer.get_filename())
      ConfigureDeploymentStepConnector.connector_ro_mode?(false)
    end
    
    write_connector_wrapper_conf()
    write_user_map()
    
    # Configure user name in service script.
    set_run_as_user("#{get_deployment_basedir()}/tungsten-connector/bin/connector")

    add_service("tungsten-connector/bin/connector")
    add_log_file("tungsten-connector/log/connector.log")
    
    FileUtils.cp("#{get_deployment_basedir()}/tungsten-connector/conf/connector.service.properties", 
      "#{get_deployment_basedir()}/cluster-home/conf/cluster/#{@config.getProperty(DATASERVICENAME)}/service/connector.properties")
    
    Configurator.instance.write_header "Writing Bristlecone performance test configurations"
    
    write_bristlecone_evaluator_xml()
    
    write_evaluator("readonly")
    write_evaluator("readwrite")
  end
  
  def write_user_map
    # Fix up the user.map file.  Note that if user does not wish to delete
    # existing file we don't do that.
    user_map = "#{get_deployment_basedir()}/tungsten-connector/conf/user.map"
    if File.exists?(user_map) && @config.getProperty(CONN_DELETE_USER_MAP) == "false"
      info("NOTE: File user.map already exists and delete option is false")
      info("File not regenerated: #{user_map}")
    elsif @config.getProperty(ENABLE_CONNECTOR_BRIDGE_MODE) == "false"
      transformer = Transformer.new(
        "#{get_deployment_basedir()}/tungsten-connector/samples/conf/user.map.tpl", 
        user_map, "# ")
      transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
      transformer.transform_values(method(:transform_values))
      transformer.output
      
      unless File.chmod(0600, user_map) == 1
        error("Unable to set the file permissions for #{user_map}")
      end
    end
  end
    
  def write_bristlecone_evaluator_xml
    Dir["#{get_deployment_basedir()}/bristlecone/samples/config/evaluator/*.xml"].sort().each do |file| 
      debug(file)
      transformer = Transformer.new(file, 
        "#{get_deployment_basedir()}/bristlecone/config/evaluator/#{File.basename(file)}",
        nil)
      transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
      transformer.transform_values(method(:transform_values))
      transformer.output
      watch_file(transformer.get_filename())
    end
  end

  def write_evaluator(rw_mode)
    # Create script to start bristlecone test with give 'rw_mode' connection.
    script = "#{get_deployment_basedir()}/cluster-home/bin/evaluator_#{rw_mode}"
    out = File.open(script, "w")
    out.puts "#!/bin/bash"
    out.puts "# Run bristlecone evaluator with connector #{rw_mode} connection"
    out.puts "BRI_HOME=`dirname $0`/../../bristlecone"
    if @config.getProperty(USERID) != "postgres" then
      out.puts "echo Cleaning-up evaluator database"
      out.puts "mysql -h #{@config.getProperty(HOST)} -u #{@config.getProperty(CONN_CLIENTLOGIN)} -p#{@config.getProperty(CONN_CLIENTPASSWORD)} -P #{@config.getProperty(CONN_LISTEN_PORT)} -e 'DROP DATABASE IF EXISTS evaluator'"
      out.puts "mysql -h #{@config.getProperty(HOST)} -u #{@config.getProperty(CONN_CLIENTLOGIN)} -p#{@config.getProperty(CONN_CLIENTPASSWORD)} -P #{@config.getProperty(CONN_LISTEN_PORT)} -e 'CREATE DATABASE evaluator'"
      out.puts "$BRI_HOME/bin/evaluator_tungsten.sh  $BRI_HOME/config/evaluator/sample.create_tables.xml 2>&1 > /dev/null"
      out.puts "echo Running actual test..."
      out.puts "$BRI_HOME/bin/evaluator_tungsten.sh  $BRI_HOME/config/evaluator/sample.#{rw_mode}.xml $*"
    else
      out.puts "echo Cleaning-up evaluator database"
      out.puts "PGPASSWORD=#{@config.getProperty(CONN_CLIENTPASSWORD)} psql -h #{@config.getProperty(HOST)} -p #{@config.getProperty(CONN_LISTEN_PORT)} -U #{@config.getProperty(CONN_CLIENTLOGIN)} #{@config.props[CONN_CLIENTDEFAULTDB]} -c 'DROP DATABASE IF EXISTS evaluator'"
      out.puts "PGPASSWORD=#{@config.getProperty(CONN_CLIENTPASSWORD)} psql -h #{@config.getProperty(HOST)} -p #{@config.getProperty(CONN_LISTEN_PORT)} -U #{@config.getProperty(CONN_CLIENTLOGIN)} #{@config.props[CONN_CLIENTDEFAULTDB]} -c 'CREATE DATABASE evaluator'"
      out.puts "$BRI_HOME/bin/evaluator_tungsten.sh  $BRI_HOME/config/evaluator/sample.create_tables.xml 2>&1 > /dev/null"
      out.puts "echo Running actual test..."
      out.puts "$BRI_HOME/bin/evaluator_tungsten.sh  $BRI_HOME/config/evaluator/sample.#{rw_mode}.xml $*"
    end
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    info "GENERATED FILE: " + script
  end
  
  def write_connector_wrapper_conf
    transformer = Transformer.new(
      "#{get_deployment_basedir()}/tungsten-connector/samples/conf/wrapper.conf",
      "#{get_deployment_basedir()}/tungsten-connector/conf/wrapper.conf", nil)
    transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
    transformer.transform_values(method(:transform_values))

    transformer.output
    watch_file(transformer.get_filename())
  end
	
	# Force the connector.ro.properties file to be written using
	# read-only option settings
	def self.connector_ro_mode?(v = nil)
	  if v != nil
	    @connector_ro_mode = v
	  end
	
	  @connector_ro_mode || nil
	end
end