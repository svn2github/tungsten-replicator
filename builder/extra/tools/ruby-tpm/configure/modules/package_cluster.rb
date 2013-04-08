module ClusterCommandModule
  COMMAND = "command"
  
  def initialize(config)
    super(config)
    
    @general_options = Properties.new()
    @dataservice_options = Properties.new()
    @host_options = Properties.new()
    @manager_options = Properties.new()
    @connector_options = Properties.new()
    @replication_options = Properties.new()
  end
  
  def get_prompts
    [
      ConfigTargetBasenamePrompt.new(),
      DeploymentCommandPrompt.new(),
      RemotePackagePath.new(),
      DeployCurrentPackagePrompt.new(),
      DeployPackageURIPrompt.new(),
      DeploymentHost.new(),
      StagingHost.new(),
      StagingUser.new(),
      StagingDirectory.new(),
      DeploymentServicePrompt.new(),
      ClusterHosts.new(),
      Clusters.new(),
      Managers.new(),
      Connectors.new(),
      ReplicationServices.new(),
      DataserviceHostOptions.new(),
      DataserviceManagerOptions.new(),
      DataserviceReplicationOptions.new(),
      DataserviceConnectorOptions.new()
    ]
  end
  
  def get_validation_checks
    checks = []
    
    ClusterHostCheck.subclasses.each{
      |klass|
      checks << klass.new()
    }
    
    checks << GlobalHostAddressesCheck.new()
    
    return checks
  end
  
  def get_deployment_object_modules(config)    
    modules = []
    
    modules << ConfigureDeploymentStepDeployment
    modules << ConfigureDeploymentStepManager
    modules << ConfigureDeploymentStepReplicator
    modules << ConfigureDeploymentStepReplicationDataservice
    modules << ConfigureDeploymentStepConnector
    modules << ConfigureDeploymentStepServices
    modules << ConfigureDeploymentStepDataService
  
    DatabaseTypeDeploymentStep.submodules().each{
      |klass|
    
      modules << klass
    }

    modules
  end
  
  def allow_command_line_cluster_options?
    true
  end
  
  def parsed_options?(arguments)
    arguments = super(arguments)
    
    if display_help?() && !display_preview?()
      return arguments
    end
    
    unless is_valid?()
      return arguments
    end
    
    unless allow_command_line_cluster_options?()
      return arguments
    end
    
    opts = OptionParser.new
    
    each_prompt(nil){
      |prompt|
      
      add_prompt(opts, prompt, @general_options)
    }
    
    each_prompt(Clusters){
      |prompt|
      
      add_prompt(opts, prompt, @dataservice_options, [DATASERVICES])
    }
    
    each_prompt(ClusterHosts){
      |prompt|
      
      add_prompt(opts, prompt, @host_options, [HOSTS])
    }
    
    each_prompt(Managers){
      |prompt|
      
      add_prompt(opts, prompt, @manager_options, [MANAGERS])
    }
    
    each_prompt(Connectors){
      |prompt|
      
      add_prompt(opts, prompt, @connector_options, [CONNECTORS])
    }
    
    each_prompt(ReplicationServices){
      |prompt|
      if prompt.is_a?(MySQLServerID)
        next
      end
      if prompt.is_a?(DatasourceDBHost)
        next
      end
      
      add_prompt(opts, prompt, @replication_options, [REPL_SERVICES])
    }

    return Configurator.instance.run_option_parser(opts, arguments)
  end
  
  def load_prompts
    load_cluster_options()
    
    super()
  end
  
  def defaults_only?
    false
  end
  
  def add_prompt(opts, prompt, config_obj, prefix = [])
    arguments = ["--#{prompt.get_command_line_argument()} [String]"]
    prompt.get_command_line_aliases().each{
      |a|
      arguments << "--#{a} [String]"
    }
    opts.on(*arguments) {
      |val|
      
      if defaults_only?() && prompt.is_a?(GroupConfigurePromptMember)
        unless prompt.allow_group_default()
          error("The \"--#{prompt.get_command_line_argument()}\" argument is not supported in the `tpm configure defaults` command.")
          next
        end
      end
      
      if (av = prompt.get_command_line_argument_value()) != nil
        if val == nil
          val = av
        end
      end
      
      begin
        validated = prompt.accept?(val)
      rescue => e
        error("Unable to parse \"--#{prompt.get_command_line_argument()}\": #{e.message}")
        next
      end
      
      if Configurator.instance.is_locked?() && prompt.allow_inplace_upgrade?() == false
        error("Unable to accept \"--#{prompt.get_command_line_argument()}\" in an installed directory.  Try running 'tpm update' from a staging directory.")
        next
      end
      
      if prompt.is_a?(GroupConfigurePromptMember) && prompt.allow_group_default()
        config_obj.setProperty(prefix + [DEFAULTS, prompt.name], validated)
      else
        config_obj.setProperty(prefix + [COMMAND, prompt.name], validated)
      end
    }
  end
  
  def load_cluster_defaults
    @config.props = @config.props.merge(@general_options.getPropertyOr([COMMAND], {}))

    @config.override([DATASERVICES, DEFAULTS], @dataservice_options.getProperty([DATASERVICES, DEFAULTS]))
    @config.override([HOSTS, DEFAULTS], @host_options.getProperty([HOSTS, DEFAULTS]))
    @config.override([MANAGERS, DEFAULTS], @manager_options.getProperty([MANAGERS, DEFAULTS]))
    @config.override([CONNECTORS, DEFAULTS], @connector_options.getProperty([CONNECTORS, DEFAULTS]))
    @config.override([REPL_SERVICES, DEFAULTS], @replication_options.getProperty([REPL_SERVICES, DEFAULTS]))
    
    _load_fixed_properties([HOSTS, DEFAULTS, FIXED_PROPERTY_STRINGS])
    _load_skipped_validation_classes([HOSTS, DEFAULTS, SKIPPED_VALIDATION_CLASSES])
    _load_skipped_validation_warnings([HOSTS, DEFAULTS, SKIPPED_VALIDATION_WARNINGS])
  
    clean_cluster_configuration()
    
    if is_valid?()
      notice("Configuration defaults updated in #{Configurator.instance.get_config_filename()}")
      save_config_file()
    end
  end
  
  def load_cluster_options
    @config.props = @config.props.merge(@general_options.getPropertyOr([COMMAND], {}))
    
    all_empty = true
    [
      @dataservice_options,
      @host_options,
      @manager_options,
      @connector_options,
      @replication_options,
      fixed_properties(),
      removed_properties(),
      ConfigureValidationHandler.get_skipped_validation_classes(),
      ConfigureValidationHandler.get_enabled_validation_classes(),
      ConfigureValidationHandler.get_skipped_validation_warnings(),
      ConfigureValidationHandler.get_enabled_validation_warnings()
    ].each{
      |opts|
      
      unless opts.empty?()
        all_empty = false
      end
    }
    
    if all_empty
      debug("No options given so skipping load_cluster_options")
      return
    end
    
    dataservices = command_dataservices()
    if dataservices.empty?()
      dataservices = @config.getPropertyOr([DATASERVICES], {}).keys().delete_if{|v| (v == DEFAULTS)}
      if dataservices.size() == 0
        raise "You must specify a dataservice name after the command or by the --dataservice-name argument"
      end
    end
    
    dataservices.each{
      |dataservice_alias|

      @config.setProperty([DATASERVICES, dataservice_alias, DATASERVICENAME], dataservice_alias)
      @config.override([DATASERVICES, dataservice_alias], @dataservice_options.getPropertyOr([DATASERVICES, DEFAULTS]))
      @config.override([DATASERVICES, dataservice_alias], @dataservice_options.getPropertyOr([DATASERVICES, COMMAND]))
      
      topology = Topology.build(dataservice_alias, @config)
      if @config.getProperty([DATASERVICES, dataservice_alias, DATASERVICE_IS_COMPOSITE]) == "true"
        dataservice_hosts = []
        connector_hosts = []
        
        @config.getProperty([DATASERVICES, dataservice_alias, DATASERVICE_COMPOSITE_DATASOURCES]).to_s().split(",").each{
          |composite_ds_member|
          
          dataservice_hosts = dataservice_hosts + @config.getPropertyOr([DATASERVICES, composite_ds_member, DATASERVICE_MEMBERS], "").split(",")
          connector_hosts = connector_hosts + @config.getPropertyOr([DATASERVICES, composite_ds_member, DATASERVICE_CONNECTORS], "").split(",")
        }
      else
        dataservice_hosts = @config.getPropertyOr([DATASERVICES, dataservice_alias, DATASERVICE_MEMBERS], "").split(",")
        connector_hosts = @config.getPropertyOr([DATASERVICES, dataservice_alias, DATASERVICE_CONNECTORS], "").split(",")
      end
    
      (dataservice_hosts+connector_hosts).uniq().each{
        |host|
        h_alias = to_identifier(host)
        if h_alias == ""
          next
        end

        # Check if we are supposed to define this host
        unless include_host?(h_alias)
          next
        end
      
        @config.setProperty([HOSTS, h_alias, HOST], host)
      }
      
      if include_all_hosts?()
        if @config.getProperty([DATASERVICES, dataservice_alias, DATASERVICE_IS_COMPOSITE]) == "true"
          @config.getProperty([DATASERVICES, dataservice_alias, DATASERVICE_COMPOSITE_DATASOURCES]).to_s().split(",").each{
            |composite_ds_member|
            
            _override_dataservice_component_options(composite_ds_member)
          }
        else
          _override_dataservice_component_options(dataservice_alias)
        end
      else
        (dataservice_hosts+connector_hosts).uniq().each{
          |host|
          h_alias = to_identifier(host)
          if h_alias == ""
            next
          end

          # Check if we are supposed to define this host
          unless include_host?(h_alias)
            next
          end

          @config.override([HOSTS, h_alias], @host_options.getProperty([HOSTS, DEFAULTS]))
          @config.override([HOSTS, h_alias], @host_options.getProperty([HOSTS, COMMAND]))

          _load_fixed_properties([HOSTS, h_alias, FIXED_PROPERTY_STRINGS])
          _load_skipped_validation_classes([HOSTS, h_alias, SKIPPED_VALIDATION_CLASSES])
          _load_skipped_validation_warnings([HOSTS, h_alias, SKIPPED_VALIDATION_WARNINGS])
        }
        
        dataservice_hosts.each{
          |host|
          h_alias = to_identifier(host)
          hs_alias = dataservice_alias + "_" + h_alias
          if h_alias == ""
            next
          end

          # Check if we are supposed to define this host
          unless include_host?(h_alias)
            next
          end
        
          if topology.use_management?()
            @config.override([MANAGERS, hs_alias], @manager_options.getProperty([MANAGERS, DEFAULTS]))
            @config.override([MANAGERS, hs_alias], @manager_options.getProperty([MANAGERS, COMMAND]))
          end
          if topology.use_replicator?()
            @config.override([REPL_SERVICES, hs_alias], @replication_options.getProperty([REPL_SERVICES, DEFAULTS]))
            @config.override([REPL_SERVICES, hs_alias], @replication_options.getProperty([REPL_SERVICES, COMMAND]))
          end
        }
        
        connector_hosts.each{
          |host|
          h_alias = to_identifier(host)
          if h_alias == ""
            next
          end

          # Check if we are supposed to define this host
          unless include_host?(h_alias)
            next
          end
        
          if topology.use_connector?()
            @config.override([CONNECTORS, h_alias], @connector_options.getProperty([CONNECTORS, DEFAULTS]))
            @config.override([CONNECTORS, h_alias], @connector_options.getProperty([CONNECTORS, COMMAND]))
          end
        }
      end
    }
    
    unless include_all_hosts?()
      command_hosts().each{
        |host|
        
        if @config.getPropertyOr([HOSTS, to_identifier(host)], nil) == nil
          warning("Unable to find an entry for #{host}")
        end
      }
    end
    
    clean_cluster_configuration()
    
    if is_valid?()
      dataservices = command_dataservices()
      if dataservices.empty?()
        dataservices = @config.getPropertyOr([DATASERVICES], {}).keys().delete_if{|v| (v == DEFAULTS)}
      end
      
      notice("Data service(s) #{dataservices.join(',')} updated in #{Configurator.instance.get_config_filename()}")
      save_config_file()
      
      if Configurator.instance.is_locked?()
        warning("Updating individual hosts may cause an inconsistent configuration file on your staging server.  You should refresh the configuration by running `tools/tpm fetch #{dataservices.join(',')}`.")
      end
    end
  end
  
  def _load_fixed_properties(target_key)
    @config.append(target_key, fixed_properties())
    
    if removed_properties().size() > 0
      props = @config.getNestedProperty(target_key)
      if props == nil
        return
      end
      
      removed_properties().each{
        |remove_key|
        
        props.delete_if{
          |prop_val|
          if prop_val =~ /^#{remove_key}[+~]?=/
            true
          else
            false
          end
        }
      }  
      @config.setProperty(target_key, props)
    end
  end
  
  def _load_skipped_validation_classes(target_key)
    @config.append(target_key, ConfigureValidationHandler.get_skipped_validation_classes())
    
    if ConfigureValidationHandler.get_enabled_validation_classes().size() > 0
      klasses = @config.getNestedProperty(target_key)
      if klasses == nil
        return
      end
      
      ConfigureValidationHandler.get_enabled_validation_classes().each{
        |enable_class|
        
        klasses.delete_if{
          |skip_class|
          if skip_class == enable_class
            true
          else
            false
          end
        }
      }  
      @config.setProperty(target_key, klasses)
    end
  end
  
  def _load_skipped_validation_warnings(target_key)
    @config.append(target_key, ConfigureValidationHandler.get_skipped_validation_warnings())
    
    if ConfigureValidationHandler.get_enabled_validation_warnings().size() > 0
      klasses = @config.getNestedProperty(target_key)
      if klasses == nil
        return
      end
      
      ConfigureValidationHandler.get_enabled_validation_warnings().each{
        |enable_class|
        
        klasses.delete_if{
          |skip_class|
          if skip_class == enable_class
            true
          else
            false
          end
        }
      }  
      @config.setProperty(target_key, klasses)
    end
  end
  
  def _override_dataservice_component_options(dataservice_alias)
    topology = Topology.build(dataservice_alias, @config)
    @config.override([DATASERVICE_HOST_OPTIONS, dataservice_alias], @host_options.getProperty([HOSTS, DEFAULTS]))
    @config.override([DATASERVICE_HOST_OPTIONS, dataservice_alias], @host_options.getProperty([HOSTS, COMMAND]))
    
    _load_fixed_properties([DATASERVICE_HOST_OPTIONS, dataservice_alias, FIXED_PROPERTY_STRINGS])
    _load_skipped_validation_classes([DATASERVICE_HOST_OPTIONS, dataservice_alias, SKIPPED_VALIDATION_CLASSES])
    _load_skipped_validation_warnings([DATASERVICE_HOST_OPTIONS, dataservice_alias, SKIPPED_VALIDATION_WARNINGS])
  
    if topology.use_management?()
      @config.override([DATASERVICE_MANAGER_OPTIONS, dataservice_alias], @manager_options.getProperty([MANAGERS, DEFAULTS]))
      @config.override([DATASERVICE_MANAGER_OPTIONS, dataservice_alias], @manager_options.getProperty([MANAGERS, COMMAND]))
    end
    if topology.use_connector?()
      @config.override([DATASERVICE_CONNECTOR_OPTIONS, dataservice_alias], @connector_options.getProperty([CONNECTORS, DEFAULTS]))
      @config.override([DATASERVICE_CONNECTOR_OPTIONS, dataservice_alias], @connector_options.getProperty([CONNECTORS, COMMAND]))
    end
    if topology.use_replicator?()
      @config.override([DATASERVICE_REPLICATION_OPTIONS, dataservice_alias], @replication_options.getProperty([REPL_SERVICES, DEFAULTS]))
      @config.override([DATASERVICE_REPLICATION_OPTIONS, dataservice_alias], @replication_options.getProperty([REPL_SERVICES, COMMAND]))
    end
  end
  
  def clean_cluster_configuration
    # Reduce the component options to remove values that are the same as their defaults
    [DATASERVICES, HOSTS, MANAGERS, CONNECTORS, REPL_SERVICES].each{
      |group_name|
      
      @config.getPropertyOr([group_name], {}).keys().each{
        |m_alias|
        if m_alias == DEFAULTS
          next
        end

        @config.getPropertyOr([group_name, m_alias], {}).each{
          |key, value|
          if @config.getNestedProperty([group_name, DEFAULTS, key]) == value
            @config.setProperty([group_name, m_alias, key], nil)
          end
        }
      }
    }
    
    # Reduce the data service options to remove values that are the same as their defaults
    @config.getPropertyOr([DATASERVICES], {}).keys().each{
      |ds_alias|
      if ds_alias == DEFAULTS
        next
      end
      topology = Topology.build(ds_alias, @config)
      
      option_groups = {
        DATASERVICE_REPLICATION_OPTIONS => REPL_SERVICES,
        DATASERVICE_CONNECTOR_OPTIONS => CONNECTORS,
        DATASERVICE_MANAGER_OPTIONS => MANAGERS,
        DATASERVICE_HOST_OPTIONS => HOSTS,
      }
      unless topology.use_management?()
        option_groups.delete(DATASERVICE_MANAGER_OPTIONS)
      end
      unless topology.use_connector?()
        option_groups.delete(DATASERVICE_CONNECTOR_OPTIONS)
      end
      unless topology.use_replicator?()
        option_groups.delete(DATASERVICE_REPLICATION_OPTIONS)
      end
      option_groups.each{
        |dso_key, group_name|
        
        dso = @config.getPropertyOr([dso_key, ds_alias], {})
        dso.each{
          |key, value|
          if @config.getNestedProperty([group_name, DEFAULTS, key]) == value
            dso.delete(key)
          end
        }
        @config.setProperty([dso_key, ds_alias], dso)
      }
      
      @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_MEMBERS], "").split(",").each{
        |host|
        h_alias = to_identifier(host)
        hs_alias = ds_alias + "_" + h_alias
        if h_alias == ""
          next
        end
        
        # Check if we are supposed to define this host
        unless include_host?(h_alias)
          next
        end
        
        if topology.use_management?()
          @config.setProperty([MANAGERS, hs_alias, DEPLOYMENT_HOST], h_alias)
          @config.setProperty([MANAGERS, hs_alias, DEPLOYMENT_DATASERVICE], ds_alias)
        end
        if topology.use_replicator?()
          @config.setProperty([REPL_SERVICES, hs_alias, DEPLOYMENT_HOST], h_alias)
          @config.setProperty([REPL_SERVICES, hs_alias, DEPLOYMENT_DATASERVICE], ds_alias)
        end
      }
      
      @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_CONNECTORS], "").split(",").each{
        |host|
        h_alias = to_identifier(host)
        if h_alias == ""
          next
        end
        
        # Check if we are supposed to define this host
        unless include_host?(h_alias)
          next
        end
        
        if topology.use_connector?()
          @config.setProperty([CONNECTORS, h_alias, DEPLOYMENT_HOST], h_alias)
          @config.append([CONNECTORS, h_alias, DEPLOYMENT_DATASERVICE], ds_alias)
        end
      }
    }
    
    # Remove MANAGERS and REPL_SERVICES entries that do not appear in a data service
    [MANAGERS, REPL_SERVICES].each{
      |group_name|
      
      @config.getPropertyOr(group_name, {}).keys().each{
        |m_alias|
        if m_alias == DEFAULTS
          next
        end

        h_alias = @config.getProperty([group_name, m_alias, DEPLOYMENT_HOST])
        hostname = @config.getProperty([HOSTS, h_alias, HOST])
        ds_alias = @config.getProperty([group_name, m_alias, DEPLOYMENT_DATASERVICE])
        
        unless @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_MEMBERS], "").split(",").include?(hostname)
          @config.setProperty([group_name, m_alias], nil)
        end
      }
    }
    
    # Remove CONNECTORS entries that do not appear in a data service
    @config.getPropertyOr(CONNECTORS, {}).keys().each{
      |m_alias|
      if m_alias == DEFAULTS
        next
      end
      
      h_alias = @config.getProperty([CONNECTORS, m_alias, DEPLOYMENT_HOST])
      hostname = @config.getProperty([HOSTS, h_alias, HOST])
      
      ds_list = @config.getPropertyOr([CONNECTORS, m_alias, DEPLOYMENT_DATASERVICE], []).delete_if{
        |ds_alias|
        (@config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_CONNECTORS], "").split(",").include?(hostname) != true)
      }
      
      if ds_list.size > 0
        @config.setProperty([CONNECTORS, m_alias, DEPLOYMENT_DATASERVICE], ds_list)
      else
        @config.setProperty([CONNECTORS, m_alias], nil)
      end
    }
    
    # Remove HOSTS entries that do not appear as a manager, replicator or connector
    @config.getPropertyOr(HOSTS, {}).keys().each{
      |h_alias|
      if h_alias == DEFAULTS
        next
      end
      
      is_found = false
      
      [MANAGERS, REPL_SERVICES, CONNECTORS].each{
        |group_name|
        @config.getPropertyOr(group_name, {}).each_key{
          |m_alias|
          if @config.getProperty([group_name, m_alias, DEPLOYMENT_HOST]) == h_alias
            is_found = true
          end
        }
      }
      
      if is_found == false
        @config.setProperty([HOSTS, h_alias], nil)
      end
    }
    
    # Remove data services from these options containers
    [
      DATASERVICE_REPLICATION_OPTIONS,
      DATASERVICE_CONNECTOR_OPTIONS,
      DATASERVICE_MANAGER_OPTIONS,
      DATASERVICE_HOST_OPTIONS,
    ].each{
      |dso_key|
      
      @config.getPropertyOr(dso_key, {}).keys().each{
        |ds_alias|
        if ds_alias == DEFAULTS
          next
        end
        
        if @config.getNestedProperty([DATASERVICES, ds_alias]) == nil
          @config.setProperty([dso_key, ds_alias], nil)
        end
      }
    }
  end
  
  def include_host_by_dataservice?(h_alias)
    v = false
    
    [
      [MANAGERS],
      [CONNECTORS],
      [REPL_SERVICES]
    ].each{
      |path|
      
      @config.getPropertyOr(path, {}).each_key{
        |p_alias|
        if p_alias == DEFAULTS
          next
        end
        
        if @config.getProperty(path + [p_alias, DEPLOYMENT_HOST]) != h_alias
          next
        end
        
        if include_dataservice?(@config.getProperty(path + [p_alias, DEPLOYMENT_DATASERVICE]))
          v = true
        end
      }
    }
    
    return v
  end
  
  def display_cluster_options
    Configurator.instance.write_divider(Logger::ERROR)
    Configurator.instance.output("Data Service options:")
    
    if display_preview?
      hosts = @config.getProperty(HOSTS)
      unless hosts
        host = nil
      else
        host = hosts.keys.at(0)
      end
    end
    
    each_prompt(nil){
      |prompt|

      prompt.output_usage()
    }
    
    each_prompt(Clusters){
      |prompt|
      if display_preview? && host
        prompt.set_member(host)
      end

      prompt.output_usage()
    }
    
    each_prompt(ClusterHosts){
      |prompt|
      if display_preview? && host
        prompt.set_member(host)
      end

      prompt.output_usage()
    }
    
    each_prompt(Managers){
      |prompt|
      if display_preview? && host
        prompt.set_member(host)
      end

      prompt.output_usage()
    }
    
    each_prompt(Connectors){
      |prompt|
      if display_preview? && host
        prompt.set_member(host)
      end

      prompt.output_usage()
    }
    
    each_prompt(ReplicationServices){
      |prompt|
      if display_preview? && host
        prompt.set_member(host)
      end

      prompt.output_usage()
    }
  end

  def get_cluster_bash_completion_arguments
    args = []
    
    each_prompt(nil){
      |prompt|
      prompt.get_bash_completion_arguments().each{
        |arg|
        args << arg
      }
    }
    
    each_prompt(Clusters){
      |prompt|
      prompt.get_bash_completion_arguments().each{
        |arg|
        args << arg
      }
    }
    
    each_prompt(ClusterHosts){
      |prompt|
      prompt.get_bash_completion_arguments().each{
        |arg|
        args << arg
      }
    }
    
    each_prompt(Managers){
      |prompt|
      prompt.get_bash_completion_arguments().each{
        |arg|
        args << arg
      }
    }
    
    each_prompt(Connectors){
      |prompt|
      prompt.get_bash_completion_arguments().each{
        |arg|
        args << arg
      }
    }
    
    each_prompt(ReplicationServices){
      |prompt|
      prompt.get_bash_completion_arguments().each{
        |arg|
        args << arg
      }
    }
    
    args
  end
  
  def use_prompt?(prompt)
    prompt.enabled_for_command_line?()
  end
  
  def each_prompt(klass = nil, &block)
    if klass == nil
      get_prompts().each{
        |prompt|
        
        prompt.set_config(@config)
        exec_prompt(prompt, block)
      }
    else
      ch = klass.new()
      ch.set_config(@config)
      ch.each_prompt{
        |prompt|
        
        exec_prompt(prompt, block)
      }
    end
  end
  
  def exec_prompt(prompt, block)
    if use_prompt?(prompt)
      begin
        block.call(prompt)
      rescue => e
        error(e.message)
      end
    end
  end
  
  # This should return false if you do not want to use any remote package
  # Added to support 'tpm update --replace-release'
  def get_default_remote_package_path
    nil
  end
  
  def get_deployment_configuration(host_alias)
    unless include_host?(host_alias)
      return nil
    end
    
    if @config.getProperty(DEPLOY_PACKAGE_URI)
      uri = URI::parse(@config.getProperty(DEPLOY_PACKAGE_URI))
      package_basename = File.basename(uri.path)
    else
      uri = nil
    end
    
    config_obj = @config.dup()
    config_obj.setProperty(DEPLOYMENT_HOST, host_alias)
    
    unless Configurator.instance.is_locked?()
      remote_package_path = get_default_remote_package_path()
      # A value of false means that we shouldn't use any remote package. We
      # should skip this check and move on
      unless remote_package_path == false
        [config_obj.getProperty(CURRENT_RELEASE_DIRECTORY), Configurator.instance.get_base_path()].each{|path|      
          begin
            Timeout.timeout(5){        
              val = ssh_result("if [ -f #{path}/tools/tpm ]; then #{path}/tools/tpm query version; else echo \"\"; fi", config_obj.getProperty(HOST), config_obj.getProperty(USERID))
              if val == Configurator.instance.get_release_version()
                remote_package_path = path
              end
            }
          rescue Timeout::Error
          rescue RemoteCommandError
          rescue CommandError
          rescue MessageError
          end
        
          if remote_package_path != nil
            break
          end
        }
      
        if remote_package_path
          config_obj.setDefault(REMOTE_PACKAGE_PATH, remote_package_path)
        end
      end
      
      if config_obj.getProperty(REMOTE_PACKAGE_PATH) == config_obj.getProperty(CURRENT_RELEASE_DIRECTORY)
        current_full_path=ssh_result("readlink #{config_obj.getProperty(REMOTE_PACKAGE_PATH)}", config_obj.getProperty(HOST), config_obj.getProperty(USERID))
        config_obj.setProperty([SYSTEM, CONFIG_TARGET_BASENAME], File.basename(current_full_path))
        config_obj.setProperty([CONFIG_TARGET_BASENAME], File.basename(current_full_path))
      else
        config_obj.setProperty([SYSTEM, CONFIG_TARGET_BASENAME], @config.getProperty(CONFIG_TARGET_BASENAME))
        config_obj.setProperty([CONFIG_TARGET_BASENAME], @config.getProperty(CONFIG_TARGET_BASENAME))
        
        config_obj.setProperty(STAGING_HOST, Configurator.instance.hostname())
        config_obj.setProperty(STAGING_USER, Configurator.instance.whoami())
        config_obj.setProperty(STAGING_DIRECTORY, Configurator.instance.get_base_path())
      end
    end

    [
      [HOSTS]
    ].each{
      |path|
      
      config_obj.getPropertyOr(path, {}).delete_if{
        |h_alias, h_props|

        (h_alias != DEFAULTS && h_alias != config_obj.getProperty([DEPLOYMENT_HOST]))
      }
    }
    
    ds_list = []
    [
      [MANAGERS],
      [CONNECTORS],
      [REPL_SERVICES]
    ].each{
      |path|
      
      config_obj.getPropertyOr(path, {}).delete_if{
        |g_alias, g_props|

        (g_alias != DEFAULTS && g_props[DEPLOYMENT_HOST] != config_obj.getNestedProperty([DEPLOYMENT_HOST]))
      }
      
      config_obj.getPropertyOr(path, {}).keys().each{
        |p_alias|
        if p_alias == DEFAULTS
          next
        end
        
        ds_list = ds_list + Array(config_obj.getProperty(path + [p_alias, DEPLOYMENT_DATASERVICE]))
      }
    }
    
    # Are any of the data services for this host to be deployed?
    found_included_dataservice = false
    ds_list.each{
      |ds_alias|
      if include_dataservice?(ds_alias)
        found_included_dataservice = true
      end
    }
    if found_included_dataservice == false
      return nil
    end
    
    config_obj.getPropertyOr(DATASERVICES, {}).keys().each{
      |ds_alias|
      
      if ds_alias == DEFAULTS
        next
      end
      
      if config_obj.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
        comp_ds_list = config_obj.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_COMPOSITE_DATASOURCES], "").split(",")
        
        ds_list.each{
          |ds|
          
          if comp_ds_list.include?(ds)
            ds_list << ds_alias
            ds_list = ds_list + comp_ds_list
          end
        }
      end
    }
    
    ds_list.each{
      |ds_alias|
      config_obj.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_RELAY_SOURCE], "").split(",").each{
        |rds_alias|
        ds_list << rds_alias
      }
    }
    
    config_obj.getPropertyOr(DATASERVICES, {}).keys().each{
      |ds_alias|
      
      if ds_alias == DEFAULTS
        next
      end
      
      if ds_list.include?(ds_alias)
        next
      end
        
      config_obj.setProperty([DATASERVICES, ds_alias], nil)
    }
    
    # Remove data services from these options containers
    [
      DATASERVICE_REPLICATION_OPTIONS,
      DATASERVICE_CONNECTOR_OPTIONS,
      DATASERVICE_MANAGER_OPTIONS,
      DATASERVICE_HOST_OPTIONS,
    ].each{
      |dso_key|
      
      config_obj.getPropertyOr(dso_key, {}).keys().each{
        |ds_alias|
        if ds_alias == DEFAULTS
          next
        end
        
        if config_obj.getNestedProperty([DATASERVICES, ds_alias]) == nil
          config_obj.setProperty([dso_key, ds_alias], nil)
        end
      }
    }
    
    [
      [HOSTS],
      [MANAGERS],
      [CONNECTORS],
      [REPL_SERVICES],
      [DATASERVICES]
    ].each{
      |path|
      
      config_obj.getPropertyOr([SYSTEM] + path, {}).delete_if{
        |g_alias, g_props|

        (config_obj.getNestedProperty(path).has_key?(g_alias) != true)
      }
    }
    
    if uri && uri.scheme == "file" && (uri.host == nil || uri.host == "localhost") && !(Configurator.instance.is_localhost?(@config.getProperty([HOSTS, host_alias, HOST])))
      config_obj.setProperty(GLOBAL_DEPLOY_PACKAGE_URI, @config.getProperty(DEPLOY_PACKAGE_URI))
      config_obj.setProperty(DEPLOY_PACKAGE_URI, "file://localhost#{config_obj.getProperty([HOSTS, host_alias, TEMP_DIRECTORY])}/#{package_basename}")
    end
    
    if !(Configurator.instance.is_localhost?(config_obj.getProperty([HOSTS, host_alias, HOST]))) && config_obj.getProperty([HOSTS, host_alias, REPL_MYSQL_CONNECTOR_PATH])
      config_obj.setProperty([HOSTS, host_alias, GLOBAL_REPL_MYSQL_CONNECTOR_PATH], config_obj.getProperty([HOSTS, host_alias, REPL_MYSQL_CONNECTOR_PATH]))
      config_obj.setProperty([HOSTS, host_alias, REPL_MYSQL_CONNECTOR_PATH], "#{config_obj.getProperty([HOSTS, host_alias, TEMP_DIRECTORY])}/#{config_obj.getProperty(CONFIG_TARGET_BASENAME)}/#{File.basename(config_obj.getProperty([HOSTS, host_alias, REPL_MYSQL_CONNECTOR_PATH]))}")
    end
    
    return config_obj
  end
  
  def post_build_deployment_configurations(config_objs)
    config_objs.each{
      |p_cfg|
      p_alias = p_cfg.getProperty([DEPLOYMENT_HOST])
      
      config_objs.each{
        |c_cfg|
        c_alias = c_cfg.getProperty([DEPLOYMENT_HOST])

        if c_cfg == p_cfg
          next
        end
        
        c_cfg.setProperty([REMOTE, HOSTS, p_alias, HOST], p_cfg.getProperty([HOSTS, p_alias, HOST]))
        c_cfg.setProperty([REMOTE, HOSTS, p_alias, PORTS_FOR_USERS], p_cfg.getProperty([HOSTS, p_alias, PORTS_FOR_USERS]))
        c_cfg.setProperty([REMOTE, HOSTS, p_alias, PORTS_FOR_CONNECTORS], p_cfg.getProperty([HOSTS, p_alias, PORTS_FOR_CONNECTORS]))
        c_cfg.setProperty([REMOTE, HOSTS, p_alias, PORTS_FOR_MANAGERS], p_cfg.getProperty([HOSTS, p_alias, PORTS_FOR_MANAGERS]))
        c_cfg.setProperty([REMOTE, HOSTS, p_alias, PORTS_FOR_REPLICATORS], p_cfg.getProperty([HOSTS, p_alias, PORTS_FOR_REPLICATORS]))
      }
    }
    
    super(config_objs)
  end
  
  def output_cluster_completion_text
    if skip_deployment?
      return
    end
    
    output = get_cluster_completion_text()
    if output == ""
      return
    end
    
    write("", Logger::NOTICE)
    write_header("Next Steps", Logger::NOTICE)
    write output, Logger::NOTICE 
  end
  
  def get_cluster_completion_text
    if include_all_hosts?()
      hosts_arg = ""
    else
      hosts_arg = "--hosts=#{command_hosts().join(',')}"
    end
    
    output = ""
    
    display_promote_connectors = false
    get_deployment_configurations().each{
      |cfg|
      
      h_alias = cfg.getProperty(DEPLOYMENT_HOST)

      if @promotion_settings.getProperty([h_alias, RESTART_CONNECTORS]) == false
        display_promote_connectors = true
      end
      
      if @promotion_settings.getProperty([h_alias, ACTIVE_DIRECTORY_PATH]) && @promotion_settings.getProperty([h_alias, CONNECTOR_ENABLED]) == "true"
        unless @promotion_settings.getProperty([h_alias, CONNECTOR_IS_RUNNING]) == "true"
          display_promote_connectors = true
        end
      elsif cfg.getProperty(SVC_START) != "true"
        display_promote_connectors = true
      end
    }
    if display_promote_connectors == true
      output = <<OUT
The connectors are not running the latest version.  In order to complete 
the process you must promote the connectors.

  tools/tpm promote-connector #{command_dataservices().join(',')}

OUT
    end
    
    output = <<OUT
#{output}Unless automatically started, you must start the Tungsten services before the 
cluster will be available.  Use the tpm command to start the services:

  tools/tpm start #{command_dataservices().join(',')} #{hosts_arg}

Wait a minute for the services to start up and configure themselves.  After 
that you may proceed.

OUT
    
    display_profile_info = true
    get_deployment_configurations().each{
      |cfg|
      if display_profile_info == true &&
          cfg.getProperty(PROFILE_SCRIPT) != "" &&
          Configurator.instance.is_localhost?(cfg.getProperty(HOST)) && 
          Configurator.instance.whoami == cfg.getProperty(USERID)
          
        output = <<OUT
#{output}We have added Tungsten environment variables to #{cfg.getProperty(PROFILE_SCRIPT)}.
Run `source #{cfg.getProperty(PROFILE_SCRIPT)}` to rebuild your environment.

OUT
        
        display_profile_info = false
      end
    }
     
    if Configurator.instance.is_enterprise?()
      output = <<OUT
#{output}Once your services start successfully you may begin to use the cluster.
To look at services and perform administration, run the following command
from any host that is a cluster member.

  $CONTINUENT_ROOT/tungsten/tungsten-manager/bin/cctrl

Configuration is now complete.  For further information, please consult
Tungsten documentation, which is available at docs.continuent.com.
OUT
  else
    output = <<OUT
#{output}Once your services start successfully replication will begin.
To look at services and perform administration, run the following command
from any host that is a replication member.

$CONTINUENT_ROOT/tungsten/tungsten-replicator/bin/trepctl services

Configuration is now complete.  For further information, please consult
Tungsten documentation, which is available at docs.continuent.com.
OUT
  end

    return output
  end
end

module ClusterConfigurationsModule
  def build_deployment_configurations
    config_objs = super()

    # If this is a configured directory we will only have a single host 
    # configuration.  Traverse the dataservices to get the other hostnames.
    # Then use that to load the configuration for each host.  Only hosts that 
    # are accessible
    if Configurator.instance.is_locked?()
      pids = {}
      results = {}
      additional_hosts = []
    
      config_objs.each{
        |config_obj|
        config_obj.getPropertyOr([DATASERVICES], {}).each_key{
          |ds_alias|
          additional_hosts = additional_hosts + config_obj.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_MEMBERS]).split(',')
          additional_hosts = additional_hosts + config_obj.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_CONNECTORS]).split(',')
        }
      }

      begin
        additional_hosts.each{
          |host|
          if host == @config.getProperty(HOST)
            next
          end

          results[host] = Tempfile.new('tpm')
          pids[host] = fork {
            begin
              Timeout.timeout(15) {
                results[host].write(ssh_result("if [ -f #{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tools/tpm ]; then #{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tools/tpm query config; else echo \"\"; fi", host, @config.getProperty(USERID)))
              }
            rescue CommandError
            rescue RemoteCommandError
            rescue MessageError
            rescue Timeout::Error
            end
          }
        }
        pids.each_value{|pid| Process.waitpid(pid) }

        results.each_value{
          |file|
          file.rewind()

          begin
            contents = file.read()
            unless contents == ""
              result = result = JSON.parse(contents)
              if result.instance_of?(Hash)
                config_obj = Properties.new()
                config_obj.import(result)

                config_objs << config_obj
              end
            end
          rescue TypeError => te
            raise "Cannot read the parallel result: #{result_dump}"
          rescue ArgumentError => ae
            error("Unable to load the parallel result.  This can happen due to a bug in Ruby.  Try updating to a newer version and retry the installation.")
          end
        }
      ensure
        results.each_value{
          |file|
          file.close()
          file.unlink()
        }
      end
    end
  
    return config_objs
  end
end