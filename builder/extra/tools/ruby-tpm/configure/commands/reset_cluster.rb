class ResetClusterCommand
  include ConfigureCommand
  include RemoteCommand
  include ClusterCommandModule
  
  DELETE_LOGS = "delete_logs"
  ARCHIVE_LOGS_SUFFIX = "archive_logs_suffix"
  
  def initialize(config)
    super(config)
    @skip_prompts = true
  end
  
  def allow_command_line_cluster_options?
    false
  end
  
  def validate_commit
    super()
    
    include_promotion_setting(DELETE_LOGS, delete_logs?().to_s())
    include_promotion_setting(ARCHIVE_LOGS_SUFFIX, "pid#{Process.pid}")
    
    is_valid?()
  end
  
  def delete_logs?(val = nil)
    if val != nil
      @delete_logs = val
    end
    
    if @delete_logs == nil
      true
    else
      @delete_logs
    end
  end
  
  def parsed_options?(arguments)
    arguments = super(arguments)
    
    if display_help?() && !display_preview?()
      return arguments
    end
    
    opts=OptionParser.new
    opts.on("--archive-logs") { delete_logs?(false) }
    
    Configurator.instance.run_option_parser(opts, arguments)
  end
  
  def output_command_usage
    super()
  
    output_usage_line("--archive-logs", "Archive the log (THL, Relay, Service) files instead of deleting them")
  end
  
  def get_bash_completion_arguments
    super() + ["--archive-logs"]
  end
  
  def get_validation_checks
    [
      ActiveDirectoryIsRunningCheck.new(),
      CurrentTopologyCheck.new()
    ]
  end
  
  def get_deployment_object_modules(config)
    [
      ResetClusterDeploymentStep
    ]
  end
  
  def self.get_command_name
    'reset'
  end
  
  def self.get_command_description
    "Reset the cluster on each host"
  end
end

module ResetClusterDeploymentStep
  def get_methods
    [
      ConfigureCommitmentMethod.new("set_maintenance_policy", ConfigureDeployment::FIRST_GROUP_ID, 0),
      ConfigureCommitmentMethod.new("stop_replication_services", -1, 0),
      ConfigureCommitmentMethod.new("clear_dynamic_properties", 0, 0),
      ConfigureCommitmentMethod.new("rotate_logs", 0, 0),
      ConfigureCommitmentMethod.new("start_replication_services_offline", 0, 1),
      ConfigureCommitmentMethod.new("reset_replication_services", 0, 2),
      ConfigureCommitmentMethod.new("wait_for_manager", 2, -1),
      ConfigureCommitmentMethod.new("set_original_policy", 4, 2),
      ConfigureCommitmentMethod.new("report_services", ConfigureDeployment::FINAL_GROUP_ID, ConfigureDeployment::FINAL_STEP_WEIGHT, false)
    ]
  end
  module_function :get_methods
  
  def rotate_logs
    if is_replicator?()
      Configurator.instance.command.build_topologies(@config)
      @config.getPropertyOr([REPL_SERVICES], {}).each_key{
        |rs_alias|
        if rs_alias == DEFAULTS
          next
        end
        
        thl_dir = @config.getProperty([REPL_SERVICES, rs_alias, REPL_LOG_DIR])
        if File.exist?(thl_dir)
          if get_additional_property(ResetClusterCommand::DELETE_LOGS) == "true"
            debug("Remove all files in #{thl_dir}")
            cmd_result("rm -f #{thl_dir}/*")
          else
            debug("Rename #{thl_dir}")
            cmd_result("mv #{thl_dir} #{thl_dir}_#{get_additional_property(ResetClusterCommand::ARCHIVE_LOGS_SUFFIX)}")
            mkdir_if_absent(thl_dir)
          end
        end
        
        relay_dir = @config.getProperty([REPL_SERVICES, rs_alias, REPL_RELAY_LOG_DIR])
        if File.exist?(relay_dir)
          if get_additional_property(ResetClusterCommand::DELETE_LOGS) == "true"
            debug("Remove all files in #{relay_dir}")
            cmd_result("rm -f #{relay_dir}/*")
          else
            debug("Rename #{relay_dir}")
            cmd_result("mv #{relay_dir} #{relay_dir}_#{get_additional_property(ResetClusterCommand::ARCHIVE_LOGS_SUFFIX)}")
            mkdir_if_absent(relay_dir)
          end
        end
        
        backup_dir = @config.getProperty([REPL_SERVICES, rs_alias, REPL_BACKUP_STORAGE_DIR])
        debug("Remove all files in #{backup_dir}")
        cmd_result("#{get_root_prefix()} rm -rf #{backup_dir}/*")
        
        if @config.getProperty(HOST_ENABLE_REPLICATOR) == "true"
          if get_additional_property(ResetClusterCommand::DELETE_LOGS) == "true"
            debug("Remove all files in #{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/log")
            FileUtils.rm(Dir.glob("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/log/*"))
          else
            debug("Rename files in #{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/log")
            cmd_result("cd #{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/log; for i in *; do mv $i ${i}\"_#{get_additional_property(ResetClusterCommand::ARCHIVE_LOGS_SUFFIX)}\"; done")
          end
        end
        
        if @config.getProperty(HOST_ENABLE_MANAGER) == "true"
          if get_additional_property(ResetClusterCommand::DELETE_LOGS) == "true"
            debug("Remove all files in #{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-manager/log")
            FileUtils.rm(Dir.glob("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-manager/log/*"))
          else
            debug("Rename files in #{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-manager/log")
            cmd_result("cd #{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-manager/log; for i in *; do mv $i ${i}\"_#{get_additional_property(ResetClusterCommand::ARCHIVE_LOGS_SUFFIX)}\"; done")
          end
        end
      }
    end
    
    if is_manager?()
      @config.getPropertyOr(DATASERVICES, {}).keys().each{
        |comp_ds_alias|

        if comp_ds_alias == DEFAULTS
          next
        end

        if @config.getProperty([DATASERVICES, comp_ds_alias, DATASERVICE_IS_COMPOSITE]) == "false"
          next
        end

        unless include_dataservice?(comp_ds_alias)
          next
        end

        mkdir_if_absent("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/cluster-home/conf/cluster/#{comp_ds_alias}/service")
        mkdir_if_absent("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/cluster-home/conf/cluster/#{comp_ds_alias}/datasource")
        mkdir_if_absent("#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/cluster-home/conf/cluster/#{comp_ds_alias}/extension")

        @config.getProperty([DATASERVICES, comp_ds_alias, DATASERVICE_COMPOSITE_DATASOURCES]).to_s().split(",").each{
          |ds_alias|
          
          path = "#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/cluster-home/conf/cluster/#{comp_ds_alias}/datasource/#{ds_alias}.properties"
          unless File.exist?(path)
            if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_RELAY_SOURCE]).to_s() != ""
              ds_role = "slave"
            else
              ds_role = "master"
            end
            
            File.open(path, "w") {
              |f|
              f.puts "
appliedLatency=-1.0
precedence=1
name=#{ds_alias}
state=OFFLINE
url=jdbc\:t-router\://#{ds_alias}/${DBNAME}
alertMessage=
isAvailable=true
role=#{ds_role}
isComposite=true
alertStatus=OK
alertTime=#{Time.now().strftime("%s000")}
dataServiceName=#{comp_ds_alias}
vendor=continuent
driver=com.continuent.tungsten.router.jdbc.TSRDriver
host=#{ds_alias}"
            }
          end
        }
      }
    end
  end
end