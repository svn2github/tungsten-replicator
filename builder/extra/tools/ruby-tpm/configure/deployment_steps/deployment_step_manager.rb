module ConfigureDeploymentStepManager
  def get_methods
    [
      ConfigureDeploymentMethod.new("deploy_manager"),
    ]
  end
  module_function :get_methods
  
  def deploy_manager
    unless is_manager?()
      info("Tungsten Replicator is not active; skipping manager configuration")
      return
    end
    
    Configurator.instance.write_header("Perform Tungsten Manager configuration")   

    transformer = Transformer.new(
		  "#{get_deployment_basedir()}/tungsten-manager/samples/conf/manager.properties.tpl",
			"#{get_deployment_basedir()}/tungsten-manager/conf/manager.properties", "#")
	  transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
    transformer.transform_values(method(:transform_values))
    transformer.output
    watch_file(transformer.get_filename())
    
    transformer = Transformer.new(
		  "#{get_deployment_basedir()}/tungsten-manager/samples/conf/mysql.service.properties.tpl",
			"#{get_deployment_basedir()}/tungsten-manager/conf/mysql.service.properties", "#")
	  transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
    transformer.transform_values(method(:transform_values))
    transformer.output
    watch_file(transformer.get_filename())
    
    group_communication_config = @config.getProperty(MGR_GROUP_COMMUNICATION_CONFIG)
    transformer = Transformer.new(
		  "#{get_deployment_basedir()}/tungsten-manager/samples/conf/#{group_communication_config}.tpl",
			"#{get_deployment_basedir()}/tungsten-manager/conf/#{group_communication_config}", nil)
	  transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
    transformer.transform_values(method(:transform_values))
  
    transformer.output
    watch_file(transformer.get_filename())
    
    transformer = Transformer.new(
		  "#{get_deployment_basedir()}/tungsten-manager/samples/conf/hedera.properties.tpl",
			"#{get_deployment_basedir()}/tungsten-manager/conf/hedera.properties", "#")
	  transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
    transformer.transform_values(method(:transform_values))
  
    transformer.output
    watch_file(transformer.get_filename())
    
    transformer = Transformer.new(
		  "#{get_deployment_basedir()}/tungsten-manager/samples/conf/monitor.properties.tpl",
			"#{get_deployment_basedir()}/tungsten-manager/conf/monitor.properties", "#")
    transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
    transformer.transform_values(method(:transform_values))
    transformer.output
    watch_file(transformer.get_filename())
    
    unless @config.getProperty(MGR_IS_WITNESS) == "true"
      transformer = Transformer.new(
  		  "#{get_deployment_basedir()}/tungsten-manager/samples/conf/checker.tungstenreplicator.properties.tpl",
  			"#{get_deployment_basedir()}/tungsten-manager/conf/checker.tungstenreplicator.properties", "#")
      transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
  	  transformer.transform_values(method(:transform_values))
  	  transformer.output
      watch_file(transformer.get_filename())
	  
  	  transformer = Transformer.new(
  		  "#{get_deployment_basedir()}/tungsten-manager/samples/conf/checker.instrumentation.properties.tpl",
  			"#{get_deployment_basedir()}/tungsten-manager/conf/checker.instrumentation.properties", "#")
      transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
  	  transformer.transform_values(method(:transform_values))
      transformer.output
      watch_file(transformer.get_filename())
    end
    
    if @config.getProperty(MANAGER_ENABLE_INSTRUMENTATION) == "true"
      FileUtils.cp("#{get_deployment_basedir()}/tungsten-manager/rules-ext/Instrumentation.drl", "#{get_deployment_basedir()}/tungsten-manager/rules/")
    end
    
    add_service("tungsten-manager/bin/manager")
    add_log_file("tungsten-manager/log/tmsvc.log")
    set_run_as_user("#{get_deployment_basedir()}/tungsten-manager/bin/manager")
    FileUtils.cp("#{get_deployment_basedir()}/tungsten-manager/conf/manager.service.properties", 
      "#{get_deployment_basedir()}/cluster-home/conf/cluster/#{@config.getProperty(DATASERVICENAME)}/service/manager.properties")
      
    write_manager_wrapper_conf()
  end
  
  def write_manager_wrapper_conf
    transformer = Transformer.new(
      "#{get_deployment_basedir()}/tungsten-manager/samples/conf/wrapper.conf",
      "#{get_deployment_basedir()}/tungsten-manager/conf/wrapper.conf", nil)
    transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
    transformer.transform_values(method(:transform_values))

    transformer.output
    watch_file(transformer.get_filename())
  end
end