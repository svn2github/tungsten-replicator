class RegularConfigureDeployment < ConfigureDeployment
  def get_name
    DISTRIBUTED_DEPLOYMENT_NAME
  end
  
  def get_deployment_configurations()
    config_objs = []
    hosts = []
    
    if @config.getProperty(DEPLOY_PACKAGE_URI)
      uri = URI::parse(@config.getProperty(DEPLOY_PACKAGE_URI))
      package_basename = File.basename(uri.path)
    else
      uri = nil
    end
    
    @config.getProperty(HOSTS).each{
      |host_alias, host_props|

      config_obj = @config.dup()
      config_obj.setProperty(DEPLOYMENT_HOST, host_alias)
      
      if uri && uri.scheme == "file" && (uri.host == nil || uri.host == "localhost") && !(Configurator.instance.is_localhost?(@config.getProperty([HOSTS, host_alias, HOST])))
        config_obj.setProperty(GLOBAL_DEPLOY_PACKAGE_URI, @config.getProperty(DEPLOY_PACKAGE_URI))
        config_obj.setProperty(DEPLOY_PACKAGE_URI, "file://localhost#{config_obj.getProperty([HOSTS, host_alias, TEMP_DIRECTORY])}/#{package_basename}")
      end
      
      if !(Configurator.instance.is_localhost?(@config.getProperty([HOSTS, host_alias, HOST]))) && @config.getProperty(REPL_MYSQL_CONNECTOR_PATH)
        config_obj.setProperty(GLOBAL_REPL_MYSQL_CONNECTOR_PATH, config_obj.getProperty(REPL_MYSQL_CONNECTOR_PATH))
        config_obj.setProperty(REPL_MYSQL_CONNECTOR_PATH, "#{@config.getProperty([HOSTS, host_alias, TEMP_DIRECTORY])}/#{Configurator::TEMP_DEPLOY_DIRECTORY}/#{Configurator.instance.get_basename()}/#{File.basename(config_obj.getProperty(REPL_MYSQL_CONNECTOR_PATH))}")
      end
    
      config_objs.push(config_obj)
    }
    
    config_objs
  end
  
  def get_deployment_object_modules(config)
    modules = [
      ConfigureDeploymentStepDeployment
      ]
    
    modules << ConfigureDeploymentStepReplicator
    modules << ConfigureDeploymentStepReplicationDataservice
    
    DatabaseTypeDeploymentStep.submodules().each{
      |klass|
      
      modules << klass
    }
    
    modules << ConfigureDeploymentStepServices

    modules
  end
  
  def include_deployment_for_package?(package)
    if package.is_a?(ConfigurePackageCluster)
      true
    elsif package.is_a?(ReplicatorInstallPackage)
      true
    else
      false
    end
  end
  
  def require_package_uri
    true
  end
  
  def self.inherited(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end

  def self.subclasses
    @subclasses
  end
end