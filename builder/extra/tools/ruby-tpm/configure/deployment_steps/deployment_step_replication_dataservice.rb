module ConfigureDeploymentStepReplicationDataservice
  def deploy_replication_dataservice()
    mkdir_if_absent(@config.getProperty(get_service_key(REPL_LOG_DIR)))
    
    if @config.getProperty(get_service_key(REPL_RELAY_LOG_DIR))
      mkdir_if_absent(@config.getProperty(get_service_key(REPL_RELAY_LOG_DIR)))
    end
    
    if @config.getProperty(get_service_key(REPL_BACKUP_STORAGE_DIR))
      mkdir_if_absent(@config.getProperty(get_service_key(REPL_BACKUP_STORAGE_DIR)))
    end
    
    # Configure replicator.properties.service.template
    transformer = Transformer.new(
		  get_replication_dataservice_template(),
			@config.getProperty(REPL_SVC_CONFIG_FILE), "#")
		
		transformer.set_fixed_properties(@config.getTemplateValue(get_service_key(FIXED_PROPERTY_STRINGS)))
		transformer.transform_values(method(:transform_replication_dataservice_values))
    transformer.output
    watch_file(transformer.get_filename())
  end
	
	def transform_replication_dataservice_values(matches)
	  case matches.at(0)
    when "APPLIER"
      v = @config.getTemplateValue(get_service_key(Kernel.const_get(matches[1])), method(:transform_replication_dataservice_values))
    when "EXTRACTOR"
      v = @config.getTemplateValue(get_service_key(Kernel.const_get("EXTRACTOR_" + matches[1])), method(:transform_replication_dataservice_values))
    when "SERVICE"
      v = @config.getTemplateValue(get_service_key(Kernel.const_get(matches[1])), method(:transform_replication_dataservice_values))
    when "HOST"
      v = @config.getTemplateValue(get_host_key(Kernel.const_get(matches[1])), method(:transform_replication_dataservice_values))
    else
      v = @config.getTemplateValue(matches.map{
        |match|
        Kernel.const_get(match)
      }, method(:transform_replication_dataservice_values))
    end
    
    return v
	end
	
	def get_replication_dataservice_template()
    if @config.getProperty(REPL_ROLE) == REPL_ROLE_DI
      "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/replicator.properties.direct"
	  else
	    begin
	      extractor_template = get_extractor_datasource().get_extractor_template()
	    rescue
	      if @config.getProperty(REPL_ROLE) == REPL_ROLE_S
	        return "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/replicator.properties.slave"
	      else
	        raise "Unable to extract from #{get_extractor_datasource.get_connection_summary}"
	      end
	    end
	  
	    "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/replicator.properties.masterslave"
	  end
	end
end
