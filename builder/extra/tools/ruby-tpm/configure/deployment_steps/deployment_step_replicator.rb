module ConfigureDeploymentStepReplicator
  def get_methods
    [
      ConfigureDeploymentMethod.new("deploy_replicator")
    ]
  end
  module_function :get_methods
  
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
    
    Configurator.instance.command.build_topologies(@config)
    @config.getPropertyOr(REPL_SERVICES, {}).each_key{
      |hs_alias|
      if hs_alias == DEFAULTS
        next
      end
      
      ds_alias = @config.getProperty([REPL_SERVICES, hs_alias, DEPLOYMENT_DATASERVICE])
      
      @config.setProperty(DEPLOYMENT_SERVICE, hs_alias)
      @config.setProperty(DEPLOYMENT_DATASERVICE, ds_alias)
      
      info("Configure the #{ds_alias} replication service")
      
      if @config.getProperty([REPL_SERVICES, hs_alias, REPL_SVC_CLUSTER_ENABLED]) == "true"
        transformer = Transformer.new(
    		  "#{get_deployment_basedir()}/tungsten-manager/samples/conf/mysql_checker_query.sql.tpl",
    			"#{get_deployment_basedir()}/tungsten-manager/conf/mysql_checker_query.sql", nil)

        transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
    	  transformer.transform_values(method(:transform_replication_dataservice_values))

        transformer.output
        watch_file(transformer.get_filename())
        
        write_replication_monitor_extension()
      end
      
      trigger_event(:before_deploy_replication_service)
      deploy_replication_dataservice()
      trigger_event(:after_deploy_replication_service)
      
      @config.setProperty(DEPLOYMENT_SERVICE, nil)
      @config.setProperty(DEPLOYMENT_DATASERVICE, nil)
    }
    
    write_replication_service_properties()
    write_wrapper_conf()
    add_service("tungsten-replicator/bin/replicator")
    add_log_file("tungsten-replicator/log/trepsvc.log")
    set_run_as_user("#{get_deployment_basedir()}/tungsten-replicator/bin/replicator")
  end
  
  def write_replication_service_properties
    transformer = Transformer.new(
		  "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/sample.services.properties",
			"#{get_deployment_basedir()}/tungsten-replicator/conf/services.properties", "#")

    transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
	  transformer.transform_values(method(:transform_replication_dataservice_values))

    transformer.output
    watch_file(transformer.get_filename())
  end
  
  def write_replication_monitor_extension
    svc_properties = "#{get_deployment_basedir()}/cluster-home/conf/cluster/#{@config.getProperty(DATASERVICENAME)}/extension/event.properties"    
    echo_event_sh = get_svc_command("${manager.home}/scripts/echoEvent.sh")
    
    # Create service properties file.
    out = File.open(svc_properties, "w")
    out.puts("# event.properties")
    out.puts("name=event")
    out.puts("command.onResourceStateTransition=#{echo_event_sh}")
    out.puts("command.onDataSourceStateTransition=#{echo_event_sh}")
    out.puts("command.onFailover=#{echo_event_sh}")
    out.puts("command.onPolicyAction=#{echo_event_sh}")
    out.puts("command.onRecovery=#{echo_event_sh}")
    out.puts("command.onDataSourceCreate=#{echo_event_sh}")
    out.puts("command.onResourceNotification=#{echo_event_sh}")
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    
    info "GENERATED FILE: " + svc_properties
    watch_file(svc_properties)
  end
	
	def write_wrapper_conf
    transformer = Transformer.new(
      "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/wrapper.conf",
      "#{get_deployment_basedir()}/tungsten-replicator/conf/wrapper.conf", nil)
    transformer.set_fixed_properties(@config.getProperty(get_host_key(FIXED_PROPERTY_STRINGS)))
    transformer.transform_values(method(:transform_values))

    transformer.output
    watch_file(transformer.get_filename())
  end
  
  def get_dynamic_properties_file()
    @config.getProperty(REPL_SVC_DYNAMIC_CONFIG)
  end
end
