ACTIVE_DIRECTORY_PATH = "active_directory_path"
ACTIVE_VERSION = "active_version"
MANAGER_ENABLED = "manager_enabled"
REPLICATOR_ENABLED = "replicator_enabled"
CONNECTOR_ENABLED = "connector_enabled"
MANAGER_IS_RUNNING = "manager_is_running"
REPLICATOR_IS_RUNNING = "replicator_is_running"
CONNECTOR_IS_RUNNING = "connector_is_running"
MANAGER_POLICY = "manager_policy"
MANAGER_COORDINATOR = "manager_coordinator"

module ConfigureDeployment
  FIRST_GROUP_ID = -100
  FIRST_STEP_WEIGHT = -100
  FINAL_GROUP_ID = 100
  FINAL_STEP_WEIGHT = 100
  
  def initialize
    @validation_handler = nil
    @deployment_handler = nil
    @promotion_settings = nil
    @validation_return_properties = nil
  end
  
  def set_config(config)
    @config = config
    @deployment_configs = nil
  end
  
  def get_deployment_configurations
    if @deployment_configs == nil
      @deployment_configs = build_deployment_configurations()
    end
    
    return @deployment_configs
  end
  
  def build_deployment_configurations()
    config_objs = []
    mtx = Mutex.new
    threads = []

    reset_errors()
    
    if use_external_configuration?()
      pre_configs = load_external_configurations()
    else
      pre_configs = [@config]
    end

    begin
      pre_configs.each{
        |cfg|
        cfg.getPropertyOr([HOSTS], {}).each_key{
          |h_alias|
          if h_alias == DEFAULTS
            next
          end
          if use_external_configuration?()
            if cfg.getProperty([HOSTS, h_alias, HOST]) != Configurator.instance.hostname()
              next
            end
          end

          threads << Thread.new(h_alias, cfg.dup()) {
            |h_alias, cfg|
            config_obj = get_deployment_configuration(h_alias, cfg)
            if config_obj == nil
              Thread.current.kill
            end
            
            # This is a local server and we need to make sure the 
            # PREFERRED_PATH is added
            path = config_obj.getProperty(PREFERRED_PATH)
            unless path.to_s() == ""
              debug("Adding #{path} to $PATH")
              ENV['PATH'] = path + ":" + ENV['PATH']
            end

            tracking_key = "#{config_obj.getProperty(HOST)}:#{config_obj.getProperty(HOME_DIRECTORY)}"
            config_obj.setProperty(DEPLOYMENT_CONFIGURATION_KEY, tracking_key)
            config_obj.setProperty(DEPLOYMENT_COMMAND, self.class.name)
            mtx.synchronize do
              config_objs << config_obj
            end
          }
        }
      }
      threads.each{|t| t.join() }
    end
    
    config_objs
  end
  
  def get_deployment_configuration(host_alias, config_obj)
    raise "Undefined function: get_deployment_configuration"
  end
  
  def get_deployment_objects
    if @deployment_objects == nil
      @deployment_objects = build_deployment_objects()
    end
    
    return @deployment_objects
  end
  
  def build_deployment_objects
    deployment_objects = {}
    get_deployment_configurations().each{
      |c|
      deployment_objects[c.getProperty(DEPLOYMENT_HOST)] = get_deployment_object(c)
    }
    
    return deployment_objects
  end
  
  def get_deployment_objects_group_ids(class_name = nil)
    group_ids = []
    
    get_deployment_objects().each{
      |host_alias,obj|
      
      debug("Deployment methods for #{@config.getProperty([HOSTS, host_alias, HOST])}")
      obj.log_group_methods(class_name)
      
      obj.get_group_ids(class_name).each{
        |group_id|
        group_ids << group_id
      }
    }
    
    return group_ids.uniq().sort()
  end
  
  def get_deployment_objects_methods(class_name)
    methods = {}
    
    get_deployment_objects().each{
      |host_alias,obj|
      
      obj.get_object_methods(class_name).each{
        |group_id, group_methods|
        
        unless methods.has_key?(group_id)
          methods[group_id] = []
        end
        
        group_methods.each{
          |method|
          
          methods[group_id] << method
        }
      }
    }
    
    return methods
  end
  
  def get_deployment_basedir(config)
    config.getProperty(CURRENT_RELEASE_DIRECTORY)
  end
  
  def prevalidate
    parallel_handle(get_validation_handler_class(), 'prevalidate')
  end
  
  def validate
    parallel_handle(get_validation_handler_class(), 'validate')
    
    get_validation_handler().add_remote_result(get_remote_result())
    get_validation_handler().post_validate()
    reset_errors()
    add_remote_result(get_validation_handler().get_remote_result())
    
    @validation_return_properties = output_properties.dup()
    is_valid?()
  end
  
  def validate_config(deployment_config)
    get_validation_handler().validate_config(deployment_config)
  end
  
  def validate_commit
    parallel_handle(get_validation_handler_class(), 'validate_commit')
    @promotion_settings = output_properties.dup()
    is_valid?()
  end
  
  def include_promotion_setting(name, value)
    get_deployment_configurations().each{
      |cfg|
      @promotion_settings.include([cfg.getProperty([DEPLOYMENT_CONFIGURATION_KEY])], {
        name => value
      })
    }
  end
  
  def override_promotion_setting(name, value)
    get_deployment_configurations().each{
      |cfg|
      @promotion_settings.override([cfg.getProperty([DEPLOYMENT_CONFIGURATION_KEY])], {
        name => value
      })
    }
  end
  
  def validate_commit_config(deployment_config)
    get_validation_handler().validate_commit_config(deployment_config)
  end
  
  def parallel_handle(klass, method)
    mtx = Mutex.new
    threads = []
    
    reset_errors()
    
    begin
      config_objs = get_deployment_configurations()
      config_objs.each_index {
        |idx|
      
        threads << Thread.new(idx, config_objs[idx]) {
          |idx, cfg|
          h = klass.new()
          h.send(method.to_sym(), [cfg])
          
          mtx.synchronize do
            add_remote_result(h.get_remote_result())
          end
        }
      }
      threads.each{|t| t.join() }
    end
    
    is_valid?()
  end
  
  def handle(klass, method)
    reset_errors()
    
    begin
      config_objs = get_deployment_configurations()
      config_objs.each {
        |cfg|
        h = klass.new()
        h.send(method.to_sym(), [cfg])
        add_remote_result(h.get_remote_result())
      }
    end
    
    is_valid?()
  end
  
  def prepare
    parallel_handle(get_deployment_handler_class(), 'prepare')
    
    get_deployment_configurations().each{
      |cfg|
      
      key = cfg.getProperty([DEPLOYMENT_CONFIGURATION_KEY])
      h_props = @output_properties.getNestedProperty([key, "props"])
      if h_props != nil
        cfg.props = h_props.dup
      end
    }
    
    is_valid?()
  end
  
  def parallel_deploy(type, additional_properties = nil)
    @deployment_handlers = []
    
    mtx = Mutex.new
    config_objs = get_deployment_configurations()
    deployment_methods = get_deployment_objects_methods(type)
    
    reset_errors()
    
    threads = []
    config_objs.each_index {
      |idx|

      unless @deployment_handlers[idx]
        @deployment_handlers[idx] = get_deployment_handler_class().new()
        @deployment_handlers[idx].set_additional_properties(config_objs[idx], additional_properties)
        
        if type == ConfigureDeploymentMethod.name
          threads << Thread.new(idx) {
            |idx|
            h = @deployment_handlers[idx]
            h.prepare_deploy_config(config_objs[idx])
          }
        end
      end
    }
    threads.each{|t| t.join() }
    
    if additional_properties != nil
      Configurator.instance.debug("Additional properties for #{type.to_s} deployment methods")
      Configurator.instance.debug(additional_properties.to_s)
      
      threads = []
      config_objs.each_index {
        |idx|
        threads << Thread.new(idx) {
          |idx|
          h = @deployment_handlers[idx]
          h.set_additional_properties(config_objs[idx], additional_properties)
        }
      }
      threads.each{|t| t.join() }
    end
    
    begin
      get_deployment_objects_group_ids(type).each {
        |group_id|
        
        allow_parallel=true
        if deployment_methods.has_key?(group_id)
          deployment_methods[group_id].each{
            |method|
            unless method.allow_parallel
              allow_parallel=false
            end
          }
        end
        
        if allow_parallel == true
          threads = []
          
          config_objs.each_index {
            |idx|
            threads << Thread.new(idx) {
              |idx|
              h = @deployment_handlers[idx]
              
              h.deploy_config(config_objs[idx], type, group_id)
              mtx.synchronize do
                add_remote_result(h.get_remote_result())
              end
            }
          }
          threads.each{|t| t.join() }
        else
          config_objs.each_index {
            |idx|

            h = @deployment_handlers[idx]
            h.deploy_config(config_objs[idx], type, group_id)
            add_remote_result(h.get_remote_result())
          }
        end
        
        unless is_valid?()
          return false
        end
      }
    end
    
    is_valid?()
  end
  
  def deploy
    parallel_deploy(ConfigureDeploymentMethod.name)
  end
  
  def deploy_config(deployment_config, deployment_method_class_name, run_group_id = nil, additional_properties = nil)
    get_deployment_object(deployment_config).run(deployment_method_class_name, run_group_id, additional_properties)
  end
  
  def commit
    parallel_deploy(ConfigureCommitmentMethod.name, @promotion_settings)
  end
  
  def cleanup
    parallel_handle(get_deployment_handler_class(), 'cleanup')
  end
  
  def get_validation_handler_class
    ConfigureValidationHandler
  end
  
  def get_validation_handler
    unless @validation_handler
      @validation_handler = get_validation_handler_class().new()
    end
    
    @validation_handler
  end
  
  def get_deployment_handler_class
    ConfigureDeploymentHandler
  end
  
  def get_deployment_handler
    unless @deployment_handler
      @deployment_handler = get_deployment_handler_class().new()
    end
    
    @deployment_handler
  end
  
  def get_deployment_object_modules(config)
    []
  end
  
  def get_deployment_object(config)
    # Load each of the files in the deployement_steps directory
    Dir[File.dirname(__FILE__) + '/deployment_steps/*.rb'].each do |file| 
      system_require File.dirname(file) + '/' + File.basename(file, File.extname(file))
    end
    
    # Get an object that represents the deployment steps required by the config
    obj = Class.new{
      include ConfigureDeploymentCore
    }.new(config)

    # Execute each of the deployment steps
    obj.prepare(get_deployment_object_modules(config))
    return obj
  end
  
  def get_weight
    0
  end
  
  def self.inherited(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end

  def self.subclasses
    @subclasses
  end
end

class ConfigureDeploymentStepMethod
  attr_reader :method_name, :weight, :group_id, :allow_parallel
  def initialize(method_name, group_id = 0, weight = 0, allow_parallel = true)
    @method_name=method_name
    @group_id=group_id
    @weight=weight
    @allow_parallel=allow_parallel
  end
end

class ConfigureDeploymentMethod < ConfigureDeploymentStepMethod
end

class ConfigureCommitmentMethod < ConfigureDeploymentStepMethod
end

module DatabaseTypeDeploymentStep
  def self.included(subclass)
    @submodules ||= []
    @submodules << subclass
  end

  def self.submodules
    @submodules || []
  end
end