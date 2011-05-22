module ConfigureDeploymentStepReplicator
  def get_deployment_methods
    unless Configurator.instance.package.is_a?(ConfigureServicePackage)
      [
        ConfigureDeploymentMethod.new("deploy_replicator"),
        ConfigureDeploymentMethod.new("deploy_replication_dataservices", 50)
#        ConfigureDeploymentMethod.new("postgresql_configuration", ConfigureDeployment::FINAL_STEP_WEIGHT-1)
      ]
    else
      []
    end
  end
  module_function :get_deployment_methods
  
  def deploy_replicator
    unless is_replicator?()
      info("Tungsten Replicator is not active; skipping configuration")
      return
    end
    
    Configurator.instance.write_header("Perform Tungsten Replicator configuration")    
    
    mkdir_if_absent(@config.getProperty(REPL_BACKUP_DUMP_DIR))
    mkdir_if_absent(@config.getProperty(REPL_BACKUP_STORAGE_DIR))
    mkdir_if_absent(@config.getProperty(REPL_RELAY_LOG_DIR))
    mkdir_if_absent(@config.getProperty(REPL_LOG_DIR))

    write_replication_service_properties()
    write_replicator_thl()
    write_wrapper_conf()
    deploy_mysql_connectorj_package()
    add_service("tungsten-replicator/bin/replicator")
    set_run_as_user("#{get_deployment_basedir()}/tungsten-replicator/bin/replicator")
  end
  
  def write_replication_service_properties
    transformer = Transformer.new(
		  "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/sample.services.properties",
			"#{get_deployment_basedir()}/tungsten-replicator/conf/services.properties", "#")

		transformer.transform { |line|
		  if line =~ /replicator.rmi_port=/ then
        "replicator.rmi_port=" + @config.getProperty(REPL_RMI_PORT)
  		else
  		  line
  		end
		}
  end
  
  def write_replicator_thl
		# Fix up the THL utility class name.
		transformer = Transformer.new(
									  "#{get_deployment_basedir()}/tungsten-replicator/bin/thl",
									  "#{get_deployment_basedir()}/tungsten-replicator/bin/thl", nil)
	
		transformer.transform { |line|
			if line =~ /THL_CTRL=/
			    "THL_CTRL=com.continuent.tungsten.enterprise.replicator.thl.EnterpriseTHLManagerCtrl"
			else
				line
			end
		}
	end
	
	def write_wrapper_conf
    transformer = Transformer.new(
      "#{get_deployment_basedir()}/tungsten-replicator/conf/wrapper.conf",
      "#{get_deployment_basedir()}/tungsten-replicator/conf/wrapper.conf", nil)

    transformer.transform { |line|
      if line =~ /wrapper.java.maxmemory=/
        "wrapper.java.maxmemory=" + @config.getProperty(REPL_JAVA_MEM_SIZE)
      else
        line
      end
    }
  end
  
  def deploy_mysql_connectorj_package
		connector = @config.getProperty(REPL_MYSQL_CONNECTOR_PATH)
		if connector != nil and connector != "" and File.exist?(connector)
		  # Deploy user's specified MySQL Connector/J (TENT-222).
  		info "Deploying MySQL Connector/J..."
			FileUtils.cp(connector, "#{get_deployment_basedir()}/tungsten-replicator/lib/")
		end
	end
  
  def get_trepctl_cmd
    "#{get_deployment_basedir()}/tungsten-replicator/bin/trepctl -port #{@config.getProperty(REPL_RMI_PORT)}"
  end
  
  def get_dynamic_properties_file()
    "#{get_deployment_basedir()}/tungsten-replicator/conf/dynamic.properties"
  end
  
  def is_replicator?
    true
  end
  
  def is_master?
    @config.getPropertyOr(REPL_SERVICES, {}).each{
      |service_alias,service_properties|
      
      if service_properties[REPL_ROLE] == REPL_ROLE_M
        return true
      end
    }
    
    false
  end
  
  def get_replication_dataservice_template(service_config)
    dbms_type = service_config.getProperty([DATASERVERS, service_config.getProperty(REPL_DATASERVER), DBMS_TYPE])
    
    case dbms_type
    when "mysql"
      if service_config.getProperty(REPL_USE_DRIZZLE) == "true"
  			"#{get_deployment_basedir()}/tungsten-replicator/samples/conf/replicator.properties.mysql-with-drizzle-driver"
  		else
  		  "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/replicator.properties.mysql"
  		end
  	when "postgresql"
  	  "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/sample.static.properties.postgresql"
  	else
  	  error("Unable to determine the replicator properties template for #{dbms_type}")
  	end
	end
end