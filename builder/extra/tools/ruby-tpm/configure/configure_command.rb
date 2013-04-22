module ConfigureCommand
  include ConfigureMessages
  include ConfigureDeployment
  
  def initialize(config)
    super()
    set_config(config)
    @interactive = false
    @force = false
    @batch = false
    @distribute_log = false
    @advanced = true
    @display_help = false
    @display_preview = false
    @subcommand = false
    
    @skip_prompts = false
    @skip_validation = false
    @skip_deployment = false
    
    @command_hosts = []
    @command_aliases = []
    @command_dataservices = []
    @fixed_properties = []
    @removed_properties = []
  end
  
  def distribute_log?(v = nil)
    if v != nil
      @distribute_log = v
    end
    
    return @distribute_log
  end
  
  def skip_prompts?(v = nil)
    if v != nil
      @skip_prompts = v
    end
    
    return @skip_prompts
  end
  
  def skip_validation?(v = nil)
    if v != nil
      @skip_validation = v
    end
    
    return @skip_validation
  end
  
  def skip_deployment?(v = nil)
    if v != nil
      @skip_deployment = v
    end
    
    return @skip_deployment
  end
  
  def forced?(v = nil)
    if v != nil
      @force = v
    end
    
    return @force
  end
  
  def display_help?(v = nil)
    if v != nil
      @display_help = v
    end
    
    return @display_help
  end
  
  def display_preview?(v = nil)
    if v != nil
      @display_preview = v
    end
    
    return @display_preview
  end
  
  def advanced?(v = nil)
    if v != nil
      @advanced = v
    end
    
    return @advanced
  end
  
  def interactive?(v = nil)
    if v != nil
      if v == true && allow_interactive?() != true
        raise "Unable to run #{self.class.get_command_name} in interactive mode"
      end
      
      @interactive = v
    end
    
    return @interactive
  end
  
  def allow_interactive?
    false
  end
  
  def use_remote_package?
    false
  end
  
  def use_remote_tools_only?
    if skip_deployment?()
      true
    else
      false
    end
  end
  
  def command_hosts(hosts = nil)
    if hosts != nil
      if allow_command_hosts?() == false
        raise "Unable to specify --host for the #{self.class.get_command_name} command"
      end
      if hosts == ""
        raise "You have specified an empty value for the --hosts argument"
      end
      
      @command_hosts = []
      @command_aliases = []
      hosts.split(",").each{
        |v|
        if v == ""
          next
        end
        
        @command_hosts << v
        @command_aliases << to_identifier(v)
      }
    end
    
    @command_hosts
  end
  
  def has_command_hosts?
    (include_all_hosts?() == false)
  end
  
  def include_all_hosts?
    (@command_aliases == nil || @command_aliases.empty?())
  end
  
  def include_host?(host)
    if @command_aliases == nil || @command_aliases.empty?()
      return true
    else
      return (@command_aliases.include?(to_identifier(host)))
    end
  end
  
  def allow_command_hosts?
    true
  end
  
  def subcommand(v = nil)
    if v != nil
      unless subcommand_allowed?(v)
        if allow_command_dataservices?()
          command_dataservices(v)
        else
          raise "'#{v}' is not valid for the #{self.class.get_command_name} command"
        end
      end
      
      @subcommand = v
    end
    
    @subcommand
  end
  
  def subcommand_allowed?(v)
    allowed_subcommands().include?(v)
  end
  
  def allowed_subcommands
    []
  end
  
  def require_all_command_dataservices?
    true
  end
  
  def command_dataservices(ds_list = nil)
    if ds_list != nil
      missing_dataservices = []
      
      ds_list.split(",").each{
        |v|
        if v == ""
          next
        end

        if (allow_undefined_dataservice?() || @config.getPropertyOr(DATASERVICES, {}).has_key?(to_identifier(v)))
          @command_dataservices << to_identifier(v)
        else
          missing_dataservices << v
        end
      }
      
      if require_all_command_dataservices?() && missing_dataservices.empty?() != true
        raise "These data services are not defined in the configuration file: #{missing_dataservices.join(',')}"
      end
    end
    
    @command_dataservices
  end
  
  def include_dataservice?(ds, check_composite_dataservices = true)
    if @command_dataservices == nil || @command_dataservices.empty?()
      true
    else
      if ds.is_a?(Array)
        v = false
        
        ds.each{
          |ds_alias|
          
          if include_dataservice?(ds_alias)
            v = true
          end
        }
        
        return v
      end
      
      if (@command_dataservices.include?(to_identifier(ds)))
        return true
      end
      
      if check_composite_dataservices == true
        @config.getPropertyOr([DATASERVICES], {}).keys().each{
          |ds_alias|
        
          if @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_COMPOSITE_DATASOURCES], "").split(",").include?(to_identifier(ds))
            if include_dataservice?(ds_alias, false)
              return true
            end
          end
        }
      end
      
      return false
    end
  end
  
  def includes_physical_dataservices?
    ds_list = command_dataservices()
    if ds_list.empty?()
      ds_list = @config.getPropertyOr(DATASERVICES, {}).keys()
    end
    
    ds_list.each{
      |ds_alias|
      
      if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_IS_COMPOSITE]) == "false"
        return true
      end
    }
    
    return false
  end
  
  def includes_composite_dataservices?
    ds_list = command_dataservices()
    if ds_list.empty?()
      ds_list = @config.getPropertyOr(DATASERVICES, {}).keys()
    end
    
    ds_list.each{
      |ds_alias|
      
      if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
        return true
      end
    }
    
    return false
  end
  
  def allow_command_dataservices?
    true
  end
  
  def allow_undefined_dataservice?
    false
  end
  
  def require_dataservice?
    false
  end
  
  def allow_multiple_tpm_commands?
    false
  end
  
  def fixed_properties
    @fixed_properties
  end
  
  def add_fixed_property(v)
    @fixed_properties << v
  end
  
  def removed_properties
    @removed_properties
  end
  
  def add_removed_property(v)
    @removed_properties << v
  end
  
  def get_prompts
    []
  end
  
  def get_validation_checks
    []
  end
  
  def allow_check_current_version?
    false
  end
  
  def check_current_version
    unless allow_check_current_version?()
      return
    end
    
    begin
      www_version  = open('http://www.continuent.com/latest-ga') {|f| f.read }
      www_version=www_version.chomp
      www_version_a=www_version.split(/:/)
      
      #Need to check we  got the right string back in-case a proxy was hit
      if www_version_a[0] == 'version'
        if www_version_a[1] != Configurator.instance.version()
          Configurator.instance.warning("You are running Release #{Configurator.instance.version()} the latest GA version is (#{www_version_a[1]})")
        end
      end
    rescue
      Configurator.instance.debug('Unable to get latest release details from website')
    end
  end
  
  def run
    write_header("#{Configurator.instance.product_name()} Configuration Procedure")
    write_from_file(File.dirname(__FILE__) + "/interface_text/configure_run")
    check_current_version()
    
    begin
      unless skip_prompts?()
        unless load_prompts()
          return false
        end
      end
      
      if skip_validation?() && skip_deployment?()
        return true
      end
      
      if get_deployment_configurations().size() == 0 && Configurator.instance.is_locked?() != true
        error("Unable to find any host configurations for the data service specified.  Check the list of configured data services by running 'tools/tpm query dataservices'.")
        return false
      end
      
      unless skip_prompts?()
        get_deployment_configurations().each{
          |cfg|
          Configurator.instance.log(cfg.to_s())
        }
      end
      
      # Make sure that basic connectivity to the hosts works
      debug("Prevalidate the configuration on each host")
      unless prevalidate()
        write_header("Validation failed", Logger::ERROR)
        output_errors()

        unless forced?()
          return false
        end
      end
      
      # Copy over configuration script code and the host configuration file
      debug("Prepare the configuration on each host")
      unless prepare()
        write_header("There are errors with the values provided in the configuration file", Logger::ERROR)
        output_errors()

        unless forced?()
          return false
        end
      end

      unless skip_validation?()
        debug("Validate the configuration on each host")
        unless validate()
          write_header("Validation failed", Logger::ERROR)
          output_errors()

          unless forced?()
            raise IgnoreError.new()
          end
        end
        debug(@validation_return_properties.to_s())

        info("")
        info("Validation finished")
      end

      unless skip_deployment?()
        # Execute the deployment of each configuration object for the deployment
        debug("Deploy the configuration on each host")
        unless deploy()
          write_header("Deployment failed", Logger::ERROR)
          output_errors()
          
          output_deployment_failed_text()

          unless forced?()
            raise IgnoreError.new()
          end
        end

        debug("Validate that the deployment is ready to be committed")
        unless validate_commit()
          write_header("Commit validation failed", Logger::ERROR)
          output_errors()

          unless forced?()
            raise IgnoreError.new()
          end
        end

        # Execute the commitment of each configuration object for the deployment
        debug("Commit the deployment on each host")
        unless commit()
          write_header("Commitment failed", Logger::ERROR)
          output_errors()
          
          output_commitment_failed_text()

          unless forced?()
            raise IgnoreError.new()
          end
        end
      end

      cleanup()
    rescue IgnoreError
      cleanup()
      return false
    rescue => e
      exception(e)
      return false
    end

    output_completion_text()
    return true
  end
  
  def load_prompts
    is_valid?()
  end
  
  def enable_log?()
    if Configurator.instance.is_locked?()
      return true
    else
      return false
    end
  end
  
  def distribute_log(log_filename)
    unless File.dirname(log_filename) == Configurator.instance.get_base_path()
      get_deployment_configurations().each{
        |config|
      
        begin
          cmd = "if [ -d #{config.getProperty(TARGET_DIRECTORY)} ]; then if [ -f #{config.getProperty(TARGET_DIRECTORY)}/tungsten-configure.log ]; then echo ''; else echo #{config.getProperty(TARGET_DIRECTORY)}; fi elif [ -d #{config.getProperty(PREPARE_DIRECTORY)} ]; then if [ -f #{config.getProperty(PREPARE_DIRECTORY)}/tungsten-configure.log ]; then echo ''; else echo #{config.getProperty(PREPARE_DIRECTORY)}; fi fi"
          deploy_dir = ssh_result(cmd, config.getProperty(HOST), config.getProperty(USERID))
          if deploy_dir != ""
            scp_result(log_filename, "#{deploy_dir}/tungsten-configure.log", config.getProperty(HOST), config.getProperty(USERID))
          end
        rescue RemoteCommandError
        rescue CommandError
        rescue
        end
      }
    end
  end
  
  def parsed_options?(arguments)
    if Configurator.instance.is_locked?()
      @command_hosts << @config.getProperty([HOSTS, @config.getProperty(DEPLOYMENT_HOST), HOST])
      @command_aliases << @config.getProperty(DEPLOYMENT_HOST)
    end
    
    opts=OptionParser.new
    opts.on("--dataservice-name String")        { |val| command_dataservices(val) }
    opts.on("--host String", "--hosts String")  { |val| command_hosts(val) }
    opts.on("-f", "--force")          { forced?(true) }
    opts.on("--no-prompts")         { skip_prompts?(true) }
    opts.on("--no-validation")      { skip_validation?(true) }
    opts.on("--no-deployment")      { skip_deployment?(true) }
    opts.on("--property String")      {|val|
                                        if val.index('=') == nil
                                          error "Invalid value #{val} given for '--property'. There should be a key/value pair joined by a single =."
                                        end
                                        add_fixed_property(val)
                                      }
    opts.on("--remove-property String") {|val|
                                        add_removed_property(val)
                                      }
    opts.on("-p", "--preview")        {
                                        display_help?(true)
                                        display_preview?(true)
                                      }

    # These are hidden from the user for backward compatibility
    opts.on("--validate-only")        { skip_deployment?(true) }
    opts.on("--config-only")          { skip_validation?(true)
                                        skip_deployment?(true) }
    remainder = Configurator.instance.run_option_parser(opts, arguments)
    
    unless remainder.empty?() || remainder[0][0,1] == "-"
      begin
        subcommand(remainder[0])
        remainder.shift()
      rescue => e
        exception(e)
      end
    end
    
    return remainder
  end
  
  def arguments_valid?
    if require_dataservice?() && command_dataservices().empty?()
      error("You must provide one or more data services for this command to run on")
    end
    
    return is_valid?()
  end
  
  def get_remote_tpm_options
    extra_args = []
    
    unless command_hosts().empty?()
      extra_args << "--hosts=#{command_hosts().join(',')}"
    end
    
    unless command_dataservices().empty?()
      extra_args << "--dataservice-name=#{command_dataservices().join(',')}"
    end
    
    extra_args
  end
  
  def output_usage()
    command_args = get_command_arguments().to_s
    if command_args.length > 0
      command_args = "[#{command_args}] "
    end
    
    subcommands = allowed_subcommands()
    if allow_command_dataservices?()
      subcommands.unshift("<data service name>")
    end
    
    if subcommands.size() > 0
      subcommands = "[#{subcommands.join(",")}] "
    end
    
    puts "Usage: #{TPM_COMMAND_NAME} #{self.class.get_command_name()} #{subcommands}[general-options] #{command_args}[command-options]"
    output_general_usage()
    
    if allowed_subcommands().size() > 0
      Configurator.instance.write_divider(Logger::ERROR)
      Configurator.instance.output("Subcommands:")
      output_subcommand_usage()
    end
    
    Configurator.instance.write_divider(Logger::ERROR)
    Configurator.instance.output("Command options:")
    output_command_usage()
  end
  
  def output_general_usage
    Configurator.instance.write_divider(Logger::ERROR)
    Configurator.instance.output("General options:")
    output_usage_line("-f, --force", "Do not display confirmation prompts or stop the configure process for errors")
    output_usage_line("-h, --help", "Displays help message")
    output_usage_line("--profile file", "Sets name of config file (default: tungsten.cfg)")
    output_usage_line("-p, --preview", "Displays the help message and preview the effect of the command line options")
    output_usage_line("-q, --quiet", "Only display warning and error messages")
    output_usage_line("-n, --notice", "Display notice, warning and error messages")
    output_usage_line("-i, --info", "Display info, notice, warning and error messages")
    output_usage_line("-v, --verbose", "Display debug, info, notice, warning and error messages")
    output_usage_line("--net-ssh-option=key=value", "Set the Net::SSH option for remote system calls", nil, nil, "Valid options can be found at http://net-ssh.github.com/ssh/v2/api/classes/Net/SSH.html#M000002")
    
    if Configurator.instance.advanced_mode?()
      output_usage_line("--config-file-help", "Display help information for content of the config file")
      output_usage_line("--template-file-help", "Display the keys that may be used in configuration template files")
      output_usage_line("--log", "Write all messages, visible and hidden, to this file.  You may specify a filename, 'pid' or 'timestamp'.")
      output_usage_line("--no-validation", "Skip validation checks that run on each host")
      output_usage_line("--no-deployment", "Skip deployment steps that create the install directory")
      output_usage_line("--skip-validation-check String", "Do not run the specified validation check.  Validation checks are identified by the string included in the error they output.")
      output_usage_line("--enable-validation-check String", "Remove a corresponding --skip-validation-check argument")
      output_usage_line("--skip-validation-warnings String", "Do not display warnings for the specified validation check.  Validation checks are identified by the string included in the warning they output.")
      output_usage_line("--enable-validation-warnings String", "Remove a corresponding --skip-validation-warnings argument")
      output_usage_line("--property=key=value")
      output_usage_line("--property=key+=value")
      output_usage_line("--property=key~=/match/replace/", "Modify the value for key in any file that the configure script touches", "", nil, 
        "key=value\t\t\tSet key to value without evaluating template values or other rules<br>
        key+=value\t\tEvaluate template values and then append value to the end of the line<br>
        key~=/match/replace/\tEvaluate template values then excecute the specified Ruby regex with sub<br>
        <br>
        --property=replicator.key~=/(.*)/somevalue,\\1/ will prepend 'somevalue' before the template value for 'replicator.key'<br><br>
        Reference the String::sub! function for details on how to build the match or replace parameters.<br>http://ruby-doc.org/core/classes/String.html#M001185")
      output_usage_line("--remove-property=key", "Remove a corresponding --property argument.")
    end
  end
  
  def get_command_arguments()
  end
  
  def get_bash_completion_arguments()
    []
  end
  
  def output_subcommand_usage
    allowed_subcommands().each{
      |s|
      output_usage_line(s)
    }
  end
  
  def output_command_usage()
    if allow_command_hosts?()
      output_usage_line("--hosts", "Limit the command to the hosts listed", nil, nil, "You must use the hostname as it appears in the configuration.")
    end
    
    if allow_command_dataservices?()
      output_usage_line("--dataservice-name", "Limit the command to the hosts in this dataservice", nil, nil, "Multiple data services may be specified by providing a comma separated list")
    end
  end
  
  def output_completion_text
    notice("")
    notice("Command successfully completed")
  end
  
  def output_deployment_failed_text
    notice("")
    notice("Check the status of all hosts before taking action")
  end
  
  def output_commitment_failed_text
    notice("")
    notice("Check the status of all hosts before taking action")
  end
  
  def get_default_config_file
    profiles_dir = get_profiles_dir()
    
    if Configurator.instance.is_locked?()
      val = "#{Configurator.instance.get_base_path()}/#{Configurator::HOST_CONFIG}"
      if File.exist?(val)
        return val
      end
    else
      if profiles_dir.to_s() != ""
        val = "#{profiles_dir}/#{Configurator::DATASERVICE_CONFIG}"
        if File.exist?(val)
          return val
        end
      end
      
      val = "#{Configurator.instance.get_base_path()}/#{Configurator::DATASERVICE_CONFIG}"
      if File.exist?(val)
        return val
      end
    end
    
    if Configurator.instance.is_locked?()
      return "#{Configurator.instance.get_continuent_root()}/#{CONFIG_DIRECTORY_NAME}/#{Configurator::HOST_CONFIG}"
    else
      if profiles_dir.to_s() != ""
        return "#{profiles_dir}/#{Configurator::DATASERVICE_CONFIG}"
      end
      
      return "#{Configurator.instance.get_base_path()}/#{Configurator::DATASERVICE_CONFIG}"
    end
  end
  
  def save_config_file
    saved_config = @config.dup()
    ph = ConfigurePromptHandler.new(saved_config)
    ph.prepare_saved_config()
    
    dirname = File.dirname(Configurator.instance.get_config_filename())
    filename = File.basename(Configurator.instance.get_config_filename())
    
    unless File.exists?(dirname)
      FileUtils.mkdir_p(dirname)
    end
    if File.exists?(Configurator.instance.get_config_filename())
      i = get_number_of_saved_configs()-1
      while i > 0
        if File.exists?("#{dirname}/#{filename}.#{i}")
          FileUtils.cp("#{dirname}/#{filename}.#{i}", "#{dirname}/#{filename}.#{i+1}")
        end
        
        i -= 1
      end

      if get_number_of_saved_configs() > 0
        FileUtils.cp(Configurator.instance.get_config_filename(), "#{dirname}/#{filename}.1")
      end
    end
    
    saved_config.store(Configurator.instance.get_config_filename())
    info("Configuration saved to #{Configurator.instance.get_config_filename()}")
  end
  
  def get_profiles_dir
    profiles_dir = nil
    unless Configurator.instance.is_enterprise?()
      profiles_dir = ENV[Configurator::REPLICATOR_PROFILES]
      
      if profiles_dir.to_s() == ""
        profiles_dir = ENV[Configurator::PROFILES_VARIABLE]
      end
    else
      profiles_dir = ENV[Configurator::PROFILES_VARIABLE]
    end
    
    profiles_dir
  end
  
  def get_number_of_saved_configs
    profiles_dir = get_profiles_dir()
    
    if profiles_dir.to_s() == ""
      return 0
    else
      return 5
    end
  end
  
  def display_alive_thread?
    true
  end
  
  def self.included(subclass)
    @@subclasses ||= []
    
    @@subclasses << subclass
  end

  def self.subclasses
    @@subclasses || []
  end
  
  def self.get_command_class(command)
    @@subclasses.each{
      |klass|
      begin
        if klass.get_command_name() == command
          return klass
        end
      rescue NoMethodError
      end
    }
    
    return nil
  end
end

module RequireDataserviceArgumentModule
  def require_dataservice?
    true
  end
end