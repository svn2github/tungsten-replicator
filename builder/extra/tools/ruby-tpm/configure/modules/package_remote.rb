module RemoteCommand
  def initialize(config)
    super(config)
    @load_remote_config = false
  end
  
  def require_remote_config?
    false
  end
  
  def loaded_remote_config?
    @load_remote_config
  end
  
  def parsed_options?(arguments)
    arguments = super(arguments)
    
    if display_help?() && !display_preview?()
      return arguments
    end
    
    deployment_host = @config.getNestedProperty([DEPLOYMENT_HOST])
    if deployment_host.to_s == ""
      deployment_host = DEFAULTS
    end
    
    if has_command_hosts?()
      target_hosts = command_hosts()
    elsif deployment_host == DEFAULTS
      target_hosts = [Configurator.instance.hostname]
    else
      target_hosts = [@config.getProperty([HOSTS, deployment_host, HOST])]
    end
    target_user = @config.getProperty([HOSTS, deployment_host, USERID])
    target_home_directory = @config.getProperty([HOSTS, deployment_host, CURRENT_RELEASE_DIRECTORY])
    default_host = nil
    @override_differences = false
    
    opts=OptionParser.new
    opts.on("--default-host String")            { |val|
      if target_hosts.include?(val)
        default_host = val
        @override_differences = true
      else
        error("Unable to find the default host (#{val}) in the list of hosts to be included")
      end
    }
    opts.on("--user String")                    { |val| 
      target_user = val }
    opts.on("--release-directory String", "--directory String")       { |val| 
      @load_remote_config = true
      target_home_directory = val }
    
    arguments = Configurator.instance.run_option_parser(opts, arguments)
    
    unless is_valid?()
      return arguments
    end

    if @load_remote_config == true
      load_remote_config(target_hosts, target_user, target_home_directory, default_host)
    elsif require_remote_config?()
      error "You must provide --user, --hosts and --directory to run the #{self.class.get_command_name()} command"
    end
    
    arguments
  end
  
  def load_remote_config(target_hosts, target_user, target_home_directory, default_host=nil)
    host_configs = {}
    autodetect_hosts = false
    
    target_hosts.each{
      |target_host|
      if default_host == nil
        default_host = target_host
      end
      
      if target_host == "autodetect"
        if default_host == nil
          error("You must specify a hostname before autodetect in the --hosts argument")
        else
          unless Configurator.instance.is_real_hostname?(target_host)
            autodetect_hosts = true
            next
          end
        end
      end

      _add_host_config(target_host, target_user, target_home_directory.dup(), host_configs)
    }
    
    if autodetect_hosts == true
      target_hosts.delete_if{|h| h == "autodetect"}
      
      additional_hosts = []
      host_configs.each_value{
        |config_obj|
        
        if config_obj.has_key?(DATASERVICES)
          config_obj[DATASERVICES].each_key{
            |ds_alias|
            additional_hosts = additional_hosts + config_obj[DATASERVICES][ds_alias][DATASERVICE_MEMBERS].to_s().split(',')
            additional_hosts = additional_hosts + config_obj[DATASERVICES][ds_alias][DATASERVICE_CONNECTORS].to_s().split(',')
          }
        end
      }
      additional_hosts.uniq!()
      additional_hosts.delete_if{
        |host|
        (host_configs.has_key?(host) == true)
      }
      additional_hosts.each{
        |host|
        target_hosts << host
        _add_host_config(host, target_user, target_home_directory.dup(), host_configs)
      }
    end
    
    unless is_valid?()
      return false
    end
    
    sections_to_merge = [
      DATASERVICES,
      HOSTS,
      MANAGERS,
      CONNECTORS,
      REPL_SERVICES,
      DATASERVICE_HOST_OPTIONS,
      DATASERVICE_MANAGER_OPTIONS,
      DATASERVICE_CONNECTOR_OPTIONS,
      DATASERVICE_REPLICATION_OPTIONS
    ]
    
    final_props = {}
    
    # Initialize the properties to import based on the first host in the list
    sections_to_merge.each{
      |key|
      
      if host_configs[default_host].has_key?(key)
        final_props[key] = host_configs[default_host][key]
        
        # Make sure we have a DEFAULTS value to catch differences with 
        # any possible defaults on other hosts
        unless final_props[key].has_key?(DEFAULTS)
          final_props[key][DEFAULTS] = {}
        end
      end
    }
    
    has_differences = false
    host_configs.each{
      |target_host, host_config_props|
      if target_host == default_host
        next
      end
      
      sections_to_merge.each{
        |key|

        if host_config_props.has_key?(key)
          host_config_props[key].each{
            |g_alias,g_props|
            if final_props[key].has_key?(g_alias)
              # If it already exists in final_props, we need to make sure 
              # there is a match in the values
              unless final_props[key][g_alias] == g_props
                unless @override_differences == true
                  error "The values for #{key}.#{g_alias} do not match between #{target_host} and the other hosts.  Make sure these values match in the #{Configurator::HOST_CONFIG} file on each host."
                  has_differences = true
                end
              end
            else # Add it to final_props because it doesn't exist there yet
              final_props[key][g_alias] = g_props
            end
          }
        end
      }
    }
    
    if has_differences
      error "Try adding --default-host=<host> to resolve any configuration file differences with the settings from that host"
    end

    # Remove the data services we are about to import from the stored config
    final_props[DATASERVICES].each_key{
      |ds_alias|
      @config.setProperty([DATASERVICES, ds_alias], nil)
    }
    clean_cluster_configuration()

    # Import the configuration information
    sections_to_merge.each{
      |key|
      unless final_props.has_key?(key)
        next
      end
      final_props[key].each{
        |g_alias,g_props|
        
        if g_alias == DEFAULTS
          # Remove any default values that do not match defaults in the 
          # current configuration
          g_props = g_props.delete_if{
            |d_key,d_value|
            (@config.getNestedProperty([key, DEFAULTS, d_key]) == d_value)
          }
          if g_props.size() > 0
            # Store the remaining default values by including them in the 
            # values for these specific data services
            case key
            when HOSTS
              override_key = DATASERVICE_HOST_OPTIONS
            when MANAGERS
              override_key = DATASERVICE_MANAGER_OPTIONS
            when CONNECTORS
              override_key = DATASERVICE_CONNECTOR_OPTIONS
            when REPL_SERVICES
              override_key = DATASERVICE_REPLICATION_OPTIONS
            else # No defaults for DATASERVICES, or the DATASERVICE_ groups
              override_key = nil
            end
            
            if override_key != nil
              final_props[DATASERVICES].each_key{
                |ds_alias|
                
                if ds_alias == DEFAULTS
                  next
                end
                
                if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
                  next
                end
                
                @config.include([override_key, ds_alias], g_props)
              }
            end
          end
        else
          @config.override([key, g_alias], g_props)
        end
      }
    }
    clean_cluster_configuration()
    
    if is_valid?()
      @load_remote_config = true
      command_hosts(target_hosts.join(','))
      notice("Configuration loaded from #{target_hosts.join(',')}")
    end
  end
  
  def _add_host_config(target_host, target_user, target_home_directory, host_configs)
    begin
      host_configs[target_host] = false
      target_home_directory = validate_home_directory(target_home_directory, target_host, target_user)

      info "Load the current config from #{target_user}@#{target_host}:#{target_home_directory}"
      
      command = "#{target_home_directory}/tools/tpm query config"    
      config_output = ssh_result(command, target_host, target_user)
      parsed_contents = JSON.parse(config_output)
      unless parsed_contents.instance_of?(Hash)
        raise "Unable to read the configuration file from #{target_user}@#{target_host}:#{target_home_directory}"
      end
      
      host_configs[target_host] = parsed_contents.dup
    rescue JSON::ParserError
      error "Unable to parse the configuration file from #{target_user}@#{target_host}:#{target_home_directory}"
    rescue => e
      exception(e)
      error "Unable to load the current config from #{target_user}@#{target_host}:#{target_home_directory}"
    end
  end
  
  def output_command_usage
    super()
    
    output_usage_line("--user", "The system user that Tungsten runs as")
    output_usage_line("--directory", "The directory to look in for the Tungsten installation")
    output_usage_line("--default-host", "Use the information from this host to resolve any configuration differences")
  end
  
  def get_bash_completion_arguments
    super() + ["--user", "--release-directory", "--directory", "--default-host", "--hosts"]
  end
  
  def validate_home_directory(target_home_directory, target_host, target_user)
    if target_home_directory == "autodetect"
      matching_wrappers = ssh_result("ps aux | grep 'Tungsten [Replicator|Manager|Connector]' | grep -v grep | awk '{print $11}'", target_host, target_user)
      matching_wrappers.each_line{
        |wrapper_path|
        target_home_directory = wrapper_path[0, wrapper_path.rindex("releases")] + "tungsten/"
        break
      }
    end
    
    if ssh_result("if [ -d #{target_home_directory} ]; then if [ -f #{target_home_directory}/#{DIRECTORY_LOCK_FILENAME} ]; then echo 0; else echo 1; fi else echo 1; fi", target_host, target_user) == "0"
      return target_home_directory
    else
      unless target_home_directory =~ /tungsten\/tungsten[\/]?$/
        target_home_directory = target_home_directory + "/tungsten"
        if ssh_result("if [ -d #{target_home_directory} ]; then if [ -f #{target_home_directory}/#{DIRECTORY_LOCK_FILENAME} ]; then echo 0; else echo 1; fi else echo 1; fi", target_host, target_user) == "0"
          return target_home_directory
        end
      end
    end
    
    unless ssh_result("if [ -d #{target_home_directory} ]; then echo 0; else echo 1; fi", target_host, target_user) == "0"
      raise "Unable to find a Tungsten directory at #{target_home_directory}.  Try running with --directory=autodetect."
    else
      raise "Unable to find #{target_home_directory}/#{DIRECTORY_LOCK_FILENAME}.  Try running with --directory=autodetect."
    end
  end
end