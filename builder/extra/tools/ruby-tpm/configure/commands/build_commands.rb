class ReverseEngineerCommand
  include ConfigureCommand
  include RemoteCommand
  include ClusterCommandModule
  
  def skip_prompts?
    true
  end
  
  def display_alive_thread?
    false
  end
  
  def run
    commands = build_commands(@config)
    output commands.join("\n")
  end
  
  def build_commands(cfg)
    ph = ConfigurePromptHandler.new(cfg)
    
    default_arguments = []
    dataservice_arguments = {}
    composite_dataservice_arguments = {}
    host_arguments = {}
    host_service_arguments = {}
    
    cfg.getPropertyOr([DATASERVICES], {}).each{
      |ds_alias, props|
      if ds_alias == DEFAULTS
        props.each{
          |k,v|

          prompt = ph.find_prompt_by_name(k)
          if prompt
            begin
              add_to(prompt.build_command_line_argument(v), default_arguments)
            rescue IgnoreError
            end
          else
            error("Unable to determine the prompt class for #{k}")
          end
        }
        
        next
      end
      
      unless include_dataservice?(ds_alias)
        next
      end
      
      if cfg.getProperty([DATASERVICES, ds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
        is_composite = true
      else
        is_composite = false
      end
      
      unless is_composite
        dataservice_arguments[ds_alias] = []
      else
        composite_dataservice_arguments[ds_alias] = []
      end
      
      props.each{
        |k,v|
        
        prompt = ph.find_prompt_by_name(k)
        if prompt
          begin
            unless is_composite
              add_to(prompt.build_command_line_argument(v), dataservice_arguments[ds_alias])
            else
              add_to(prompt.build_command_line_argument(v), composite_dataservice_arguments[ds_alias])
            end
          rescue IgnoreError
          end
        else
          error("Unable to determine the prompt class for #{k}")
        end
      }
      
      [
        DATASERVICE_REPLICATION_OPTIONS,
        DATASERVICE_CONNECTOR_OPTIONS,
        DATASERVICE_MANAGER_OPTIONS,
        DATASERVICE_HOST_OPTIONS,
      ].each{
        |dso_key|
        
        cfg.getPropertyOr([dso_key, ds_alias], {}).each{
          |k,v|
          
          prompt = ph.find_prompt_by_name(k)
          if prompt
            begin
              unless is_composite
                add_to(prompt.build_command_line_argument(v), dataservice_arguments[ds_alias])
              else
                add_to(prompt.build_command_line_argument(v), composite_dataservice_arguments[ds_alias])
              end
            rescue IgnoreError
            end
          else
            error("Unable to determine the prompt class for #{k}")
          end
        }
      }
    }
    
    [
      [MANAGERS, DEFAULTS],
      [CONNECTORS, DEFAULTS],
      [REPL_SERVICES, DEFAULTS]
    ].each{
      |path|
      
      props = cfg.getPropertyOr(path, {})
      props.each{
        |k,v|

        prompt = ph.find_prompt_by_name(k)
        if prompt
          begin
            add_to(prompt.build_command_line_argument(v), default_arguments)
          rescue IgnoreError
          end
        else
          error("Unable to determine the prompt class for #{k}")
        end
      }
    }
    
    cfg.getPropertyOr([HOSTS], []).each{
      |h_alias, props|
      if h_alias == DEFAULTS
        props.each{
          |k,v|

          prompt = ph.find_prompt_by_name(k)
          if prompt
            begin
              add_to(prompt.build_command_line_argument(v), default_arguments)
            rescue IgnoreError
            end
          else
            error("Unable to determine the prompt class for #{k}")
          end
        }
        
        next
      end
      
      unless include_host?(h_alias)
        next
      end
      
      unless include_host_by_dataservice?(h_alias)
        next
      end
      
      host_arguments[h_alias] = []
      host_service_arguments[h_alias] = {}
      
      props.each{
        |k,v|
        
        prompt = ph.find_prompt_by_name(k)
        if prompt
          begin
            add_to(prompt.build_command_line_argument(v), host_arguments[h_alias])
          rescue IgnoreError
          end
        else
          error("Unable to determine the prompt class for #{k}")
        end
      }
      
      [
        [MANAGERS],
        [CONNECTORS],
        [REPL_SERVICES]
      ].each{
        |path|
        
        cfg.getPropertyOr(path, {}).each{
          |p_alias, props|
          if p_alias == DEFAULTS
            next
          end
            
          if cfg.getProperty(path + [p_alias, DEPLOYMENT_HOST]) != h_alias
            next
          end

          ds_alias = cfg.getProperty(path + [p_alias, DEPLOYMENT_DATASERVICE])
          unless include_dataservice?(ds_alias)
            next
          end

          props.each{
            |k,v|

            prompt = ph.find_prompt_by_name(k)
            if prompt
              begin
                arg = prompt.build_command_line_argument(v)
                ds_alias.each{
                  |d|
                  unless host_service_arguments[h_alias].has_key?(d)
                    host_service_arguments[h_alias][d] = []
                  end
                  add_to(arg, host_service_arguments[h_alias][d])
                }
              rescue IgnoreError
              end
            else
              error("Unable to determine the prompt class for #{k}")
            end
          }
        }
      }
    }
    
    commands = []
    if cfg.getProperty(STAGING_HOST)
      commands << "# Installed from #{cfg.getProperty(STAGING_USER)}@#{cfg.getProperty(STAGING_HOST)}:#{cfg.getProperty(STAGING_DIRECTORY)}"
    end
    
    if default_arguments.length > 0
      commands << "# Defaults for all data services and hosts"
      commands << "tools/tpm configure defaults \\"
      commands << default_arguments.sort().map{|s| Escape.shell_single_word(s)}.join(" \\\n")
    end
    
    dataservice_arguments.each{
      |ds_alias,args|
      if args.length > 0
        commands << "# Options for the #{ds_alias} data service"
        commands << "tools/tpm configure #{ds_alias} \\"
        commands << args.sort().map{|s| Escape.shell_single_word(s)}.join(" \\\n")
      end
    }
    
    composite_dataservice_arguments.each{
      |ds_alias,args|
      if args.length > 0
        commands << "# Options for the #{ds_alias} data service"
        commands << "tools/tpm configure #{ds_alias} \\"
        commands << args.sort().map{|s| Escape.shell_single_word(s)}.join(" \\\n")
      end
    }
    
    host_arguments.each{
      |h_alias,args|
      if args.length > 0
        commands << "# Options for #{cfg.getProperty([HOSTS, h_alias, HOST])}"
        commands << "tools/tpm configure --hosts=#{cfg.getProperty([HOSTS, h_alias, HOST])} \\"
        commands << args.sort().map{|s| Escape.shell_single_word(s)}.join(" \\\n")
      end
    }
    
    host_service_arguments.each{
      |h_alias,ds|
      ds.each{
        |ds_alias,args|
        if args.length > 0
          commands << "# Options for #{cfg.getProperty([HOSTS, h_alias, HOST])}"
          commands << "tools/tpm configure #{cfg.getProperty([DATASERVICES, ds_alias, DATASERVICENAME])} \\\n--hosts=#{cfg.getProperty([HOSTS, h_alias, HOST])} \\"
          commands << args.sort().map{|s| Escape.shell_single_word(s)}.join(" \\\n")
        end
      }
    }
    
    commands
  end
  
  def add_to(args, container)
    if args.is_a?(Array)
      args.each{
        |a|
        container << a
      }
    elsif args != nil
      container << args.to_s()
    end
  end
  
  def self.display_command
    true
  end
  
  def self.get_command_name
    'dump'
  end
  
  def self.get_command_aliases
    ['reverse']
  end
  
  def self.get_command_description
    "Display the 'tpm configure' commands required to recreate the configuration"
  end
end