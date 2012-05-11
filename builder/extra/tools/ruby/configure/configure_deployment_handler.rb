class ConfigureDeploymentHandler
  include ConfigureMessages
  
  def initialize(deployment_method)
    super()
    @deployment_method = deployment_method
    @config = Properties.new()
  end
  
  def prepare(configs)
    reset_errors()
    configs.each{
      |config|
      
      prepare_config(config)
    }
    
    is_valid?()
  end
  
  def prepare_config(config)
    @config.props = config.props
    
    unless Configurator.instance.is_localhost?(@config.getProperty(HOST)) && @config.getProperty(USERID) == Configurator.instance.whoami()
      validation_temp_directory = "#{@config.getProperty(TEMP_DIRECTORY)}/#{Configurator.instance.get_unique_basename()}/"
      
      # Transfer validation code
      debug("Transfer configuration code to #{@config.getProperty(HOST)}")
      user = @config.getProperty(USERID)
      ssh_user = Configurator.instance.get_ssh_user(user)
      cmd_result("rsync -aze 'ssh -p#{Configurator.instance.get_ssh_port()}' --delete #{Configurator.instance.get_base_path()}/ #{ssh_user}@#{@config.getProperty(HOST)}:#{validation_temp_directory}")
      if user != ssh_user
        ssh_result("chown -R #{user} #{validation_temp_directory}/#{Configurator.instance.get_basename}", @config.getProperty(HOST), ssh_user)
      end

      debug("Transfer host configuration file to #{@config.getProperty(HOST)}")
      config_tempfile = Tempfile.new("tcfg")
      config_tempfile.close()
      config.store(config_tempfile.path())
      scp_result(config_tempfile.path(), "#{validation_temp_directory}/#{Configurator::TEMP_DEPLOY_HOST_CONFIG}", @config.getProperty(HOST), @config.getProperty(USERID))
      File.unlink(config_tempfile.path())
    end
  end
  
  def deploy(configs)
    reset_errors()
    configs.each{
      |config|
      
      deploy_config(config)
    }
    
    is_valid?()
  end
  
  def deploy_config(config)
    @config.props = config.props
    
    if Configurator.instance.is_localhost?(@config.getProperty(HOST))&& 
        (Configurator.instance.whoami == @config.getProperty(USERID))
      Configurator.instance.write ""
      Configurator.instance.write_header "Local deploy #{@config.getProperty(HOME_DIRECTORY)}"
      
      result = @deployment_method.deploy_config(config)
    else
      extra_options = ["--package #{Configurator.instance.package.class.name}"]
      if Configurator.instance.enable_log_level?(Logger::DEBUG)
        extra_options << "-v"
      end
      unless Configurator.instance.enable_log_level?(Logger::INFO)
        extra_options << "-q"
      end
      
      Configurator.instance.write ""
      Configurator.instance.write_header "Remote deploy #{@config.getProperty(HOST)}:#{@config.getProperty(HOME_DIRECTORY)}"
      
      deployment_temp_directory = "#{@config.getProperty(TEMP_DIRECTORY)}/#{Configurator.instance.get_unique_basename()}"
      command = "cd #{deployment_temp_directory}; ruby -I#{Configurator.instance.get_ruby_prefix()} -I#{Configurator.instance.get_ruby_prefix()}/lib #{Configurator.instance.get_ruby_prefix()}/deploy.rb -b -c #{Configurator::TEMP_DEPLOY_HOST_CONFIG} --net-ssh-option=port=#{Configurator.instance.get_ssh_port()} #{extra_options.join(' ')}"
      result_dump = ssh_result(command, @config.getProperty(HOST), @config.getProperty(USERID), true)
      
      begin
        result = Marshal.load(result_dump)
        
        @errors = @errors + result.errors
        result.output()
      rescue TypeError => te
        raise "Cannot read the deployment result: #{result_dump}"
      rescue ArgumentError => ae
        raise "Unable to load the deployment result: #{result_dump}"
      end
    end
  end
  
  def cleanup(configs)
    reset_errors()
    configs.each{
      |config|
      
      @config.props = config.props
      begin
        unless Configurator.instance.is_localhost?(@config.getProperty(HOST))
          ssh_result("rm -rf #{@config.getProperty(TEMP_DIRECTORY)}/#{Configurator.instance.get_unique_basename()}", @config.getProperty(HOST), @config.getProperty(USERID))
        end
      rescue RemoteError
      end
    }
    
    is_valid?()
  end
  
  def get_message_hostname
    @config.getProperty(HOST)
  end
end
