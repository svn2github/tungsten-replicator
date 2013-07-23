class ReplicationServices < GroupConfigurePrompt
  def initialize
    super(REPL_SERVICES, "Enter replication service information for @value", "replication service", "replication services", "SERVICE")
    
    ReplicationServicePrompt.subclasses().each{
      |klass|
      self.add_prompt(klass.new())
    }
    
    DatasourcePrompt.subclasses().each{
      |klass|
      self.add_prompt(klass.new())
    }
  end
end

# Prompts that include this module will be collected for each dataservice 
# across interactive mode, the configure-service script and the
# tungsten-installer script
module ReplicationServicePrompt
  include GroupConfigurePromptMember
  include HashPromptDefaultsModule
  include NoManagerRestart
  include NoConnectorRestart
  
  def get_datasource
    ConfigureDatabasePlatform.build([@parent_group.name, get_member()], @config)
  end
  
  def get_applier_datasource
    get_datasource()
  end
  
  def get_extractor_datasource
    get_datasource()
  end
  
  def get_applier_key(key)
    get_member_key(key)
  end
  
  def get_extractor_key(key)
    get_member_key(key)
  end
  
  def get_host_key(key)
    [HOSTS, @config.getProperty(get_member_key(DEPLOYMENT_HOST)), key]
  end
  
  def get_dataservice_key(key)
    [DATASERVICES, @config.getProperty(get_member_key(DEPLOYMENT_DATASERVICE)), key]
  end
  
  def get_dataservice_alias
    if self.is_a?(HashPromptMemberModule)
      get_member()
    else
      @config.getProperty(get_member_key(DEPLOYMENT_DATASERVICE))
    end
  end
  
  def get_topology
    Topology.build(get_dataservice_alias(), @config)
  end
  
  def get_userid
    @config.getProperty(get_host_key(USERID))
  end
  
  def get_hostname
    @config.getProperty(get_host_key(HOST))
  end
  
  def self.included(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end

  def self.subclasses
    @subclasses || []
  end
  
  def allow_group_default
    true
  end
  
  def get_hash_prompt_key
    return [DATASERVICE_REPLICATION_OPTIONS, @config.getProperty(get_member_key(DEPLOYMENT_DATASERVICE)), @name]
  end
  
  def get_command_line_argument()
    super.gsub("repl-", "")
  end
  
  def get_command_line_aliases
    super() + super().collect{|al| al.gsub("repl-", "")} + [@name.gsub("_", "-")]
  end
  
  def get_display_member
    "#{@parent_group.singular.capitalize} #{get_hostname()} - #{@config.getProperty(get_dataservice_key(DATASERVICENAME))}"
  end
end

class ReplicationServiceDeploymentHost < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt

  def initialize
    super(DEPLOYMENT_HOST, 
      "On what host would you like to deploy this replication service?", 
      PV_IDENTIFIER)
    @weight = -1
  end
  
  def get_disabled_value
    @config.getProperty(DEPLOYMENT_HOST)
  end
  
  def get_default_value
    @config.getProperty(DEPLOYMENT_HOST)
  end
  
  def is_valid?
    super()
    
    unless @config.getProperty(HOSTS).has_key?(get_value())
      raise ConfigurePromptError.new(self, "Host #{get_value()} does not exist in the configuration file", get_value())
    end
  end
  
  def enabled_for_command_line?()
    false
  end
end

class ReplicationDataservice < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(DEPLOYMENT_DATASERVICE, "The dataservice replicated by this replicator",
      PV_ANY)
  end
  
  def get_disabled_value
    @config.getProperty(DEPLOYMENT_DATASERVICE)
  end
  
  def get_default_value
    @config.getProperty(DEPLOYMENT_DATASERVICE)
  end
  
  def is_valid?
    super()
    
    unless @config.getProperty(DATASERVICES).has_key?(get_value())
      raise ConfigurePromptError.new(self, "Data service #{get_value()} does not exist in the configuration file", get_value())
    end
  end
  
  def enabled_for_command_line?()
    false
  end
  
  def required?
    false
  end
end

class ReplicationServiceName < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(DEPLOYMENT_SERVICE, "What is the replication service name?", 
      PV_IDENTIFIER)
  end
  
  def load_default_value
    @default = @config.getProperty(get_dataservice_key(DATASERVICENAME))
  end
end

class LocalReplicationServiceName < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(DSNAME, "What is the local service name?", PV_IDENTIFIER)
  end
  
  def load_default_value
    @default = @config.getProperty(get_dataservice_key(DATASERVICENAME))
  end
end

class ReplicationRMIInterface < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_RMI_INTERFACE, "Replication RMI listen interface", 
      PV_ANY)
  end
end

class ReplicationRMIAddress < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  include NoSystemDefault
  
  def initialize
    super(REPL_RMI_ADDRESS, "Replication RMI listen address", 
      PV_ANY)
  end
  
  def load_default_value
    if (iface = @config.getProperty(get_member_key(REPL_RMI_INTERFACE))) != nil
      @default = Configurator.instance.get_interface_address(iface)
    else
      @default = "0.0.0.0"
    end
  end
end

class ReplicationRMIPort < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_RMI_PORT, "Replication RMI listen port", 
      PV_INTEGER, 10000)
  end
  
  PortForManagers.register(REPL_SERVICES, REPL_RMI_PORT, REPL_RMI_RETURN_PORT)
end

class ReplicationReturnRMIPort < ConfigurePrompt
  include ReplicationServicePrompt
  include HiddenValueModule
  
  def initialize
    super(REPL_RMI_RETURN_PORT, "Replication RMI return port", 
      PV_INTEGER)
  end
  
  def load_default_value
    @default = (@config.getProperty(get_member_key(REPL_RMI_PORT)).to_i() + 1).to_s
  end
end

class ReplicationServiceType < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SVC_SERVICE_TYPE, "What is the replication service type? (local|remote)", 
      PropertyValidator.new("local|remote",
      "Value must be local or remote"))
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_svc_service_type'))
    super()
  end
  
  def load_default_value
    if @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_M
      @default = "local"
    elsif @config.getProperty(get_member_key(REPL_SVC_ENABLE_SLAVE_THL_LISTENER)) == "false"
      @default = "remote"
    else
      @default = "local"
    end
  end
end

class ReplicationServiceEnableTHLListener < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SVC_ENABLE_SLAVE_THL_LISTENER, "Should this service allow THL connections?",
      PV_BOOLEAN, "true")
  end
end

class ReplicationServiceRole < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_ROLE, "What is the replication role for this service? (#{REPL_ROLE_M}|#{REPL_ROLE_S}|#{REPL_ROLE_R})",
      nil, REPL_ROLE_S)
  end
  
  def load_default_value
    topology = Topology.build(get_dataservice_alias(), @config)
    @default = topology.get_role(@config.getProperty(get_host_key(HOST)))
  end
end

class ReplicationServiceTHLInterface < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SVC_THL_INTERFACE, "Listen interface to use for THL operations", 
      PV_ANY)
  end
  
  def required?
    false
  end
end

class ReplicationServiceTHLAddress < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  include NoSystemDefault
  
  def initialize
    super(REPL_SVC_THL_ADDRESS, "Listen address to use for THL operations", 
      PV_ANY)
  end
  
  def load_default_value
    if (iface = @config.getProperty(get_member_key(REPL_SVC_THL_INTERFACE))) != nil
      @default = Configurator.instance.get_interface_address(iface)
    else
      @default = "0.0.0.0"
    end
  end
end

class ReplicationServiceTHLMaster < ConfigurePrompt
  include ReplicationServicePrompt
  include HiddenValueModule
  
  def initialize
    super(REPL_MASTERHOST, "What is the master host for this service?", 
      PV_MULTIHOSTPORT)
  end
    
  def enabled?
    super() && [REPL_ROLE_S, REPL_ROLE_ARCHIVE].include?(@config.getProperty(get_member_key(REPL_ROLE)))
  end
  
  def get_template_value(transform_values_method)
    if [REPL_ROLE_S, REPL_ROLE_ARCHIVE].include?(@config.getProperty(get_member_key(REPL_ROLE)))
      return super(transform_values_method)
    else
      relay_source = @config.getProperty(get_dataservice_key(DATASERVICE_RELAY_SOURCE))
      
      if relay_source.to_s == ""
        return "localhost"
      else
        return @config.getProperty([DATASERVICES, relay_source, DATASERVICE_MASTER_MEMBER])
      end
    end
  end
  
  def load_default_value
    @default = @config.getProperty(get_dataservice_key(DATASERVICE_MASTER_MEMBER))
  end
end

class ReplicationServiceTHLMasterPort < ConfigurePrompt
  include ReplicationServicePrompt
  include HiddenValueModule
  
  def initialize
    super(REPL_MASTERPORT, 
      "Master THL port", PV_INTEGER)
  end
  
  def enabled?
    super() && [REPL_ROLE_S, REPL_ROLE_ARCHIVE].include?(@config.getProperty(get_member_key(REPL_ROLE)))
  end
  
  def get_template_value(transform_values_method)
    if [REPL_ROLE_S, REPL_ROLE_ARCHIVE].include?(@config.getProperty(get_member_key(REPL_ROLE)))
      return super(transform_values_method)
    else
      relay_source = @config.getProperty(get_dataservice_key(DATASERVICE_RELAY_SOURCE))
      
      if relay_source.to_s == ""
        return super(transform_values_method)
      else
        return @config.getProperty([DATASERVICES, relay_source, DATASERVICE_THL_PORT])
      end
    end
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(REPL_SVC_THL_PORT))
  end
end

class ReplicationServiceTHLMasterURI < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_MASTER_URI, "Master THL URI", PV_ANY)
  end
  
  def get_template_value(transform_values_method)
    topology = Topology.build(get_dataservice_alias(), @config)
    return topology.get_master_thl_uri(@config.getProperty(get_host_key(HOST)))
  end
end

class ReplicationServiceTHLPort < ConfigurePrompt
  include ReplicationServicePrompt
  include HiddenValueModule
  
  def initialize
    super(REPL_SVC_THL_PORT, 
      "Port to use for THL operations", PV_INTEGER)
  end
  
  def load_default_value
    @default = @config.getProperty(get_dataservice_key(DATASERVICE_THL_PORT))
  end
  
  PortForReplicators.register(REPL_SERVICES, REPL_SVC_THL_PORT)
end

class ReplciationServiceTHLReadOnly < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_SVC_THL_READ_ONLY, "Should the THL files be opened as read-only", PV_BOOLEAN)
  end
  
  def get_default_value
    if @config.getTemplateValue(get_member_key(REPL_ROLE)) == REPL_ROLE_LOCAL_PRE
      "true"
    else
      "false"
    end
  end
end

class ReplicationShardIDMode < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SVC_SHARD_DEFAULT_DB, 
      "Mode for setting the shard ID from the default db (stringent|relaxed)", 
      PropertyValidator.new("stringent|relaxed", 
      "Value must be stringent or relaxed"), "stringent")
  end
end

class ReplicationAllowUnsafeSQL < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_SVC_ALLOW_BIDI_UNSAFE, 
      "Allow unsafe SQL from remote service (true|false)", PV_BOOLEAN, "false")
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_svc_allow_bidi_unsafe'))
    super()
  end
end

class ReplicationAllowAllSQL < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SVC_ALLOW_ANY_SERVICE, 
      "Replicate from any service (true|false)", 
      PV_BOOLEAN, "false")
  end
end

class ReplicationServiceAutoEnable < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_AUTOENABLE, "Auto-enable services after start-up", 
      PV_BOOLEAN, "true")
  end
end

class MasterPreferredRole < ConfigurePrompt
  include ReplicationServicePrompt

  def initialize
    super(REPL_MASTER_PREFERRED_ROLE, "Preferred role for master THL when connecting as a slave (master, slave, etc.)",
      PV_ANY, "")
  end
  
  def get_default_value
    @default = get_topology().master_preferred_role()
  end
end

class ReplicationServiceChannels < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_SVC_CHANNELS, "Number of replication channels to use for services",
      PV_INTEGER, 1)
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_svc_channels'))
    super()
  end
end

class ReplicationServiceParallelizationType < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_SVC_PARALLELIZATION_TYPE, "Method for implementing parallel apply (disk|memory|none)",
      PropertyValidator.new("disk|memory|none", 
        "Value must be disk, memory, or none"), "none")
  end
end

class ReplicationServiceParallelizationStoreClass < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_SVC_PARALLELIZATION_STORE_CLASS, "Class name for parallel queue storage", PV_ANY)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(REPL_SVC_PARALLELIZATION_TYPE)) == "memory"
      "com.continuent.tungsten.replicator.storage.parallel.ParallelQueueStore"
    elsif @config.getProperty(get_member_key(REPL_SVC_PARALLELIZATION_TYPE)) == "disk"
      "com.continuent.tungsten.replicator.thl.THLParallelQueue"
    else
      "com.continuent.tungsten.replicator.storage.InMemoryQueueStore"
    end
  end
end

class ReplicationServiceParallelizationApplierClass < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_SVC_PARALLELIZATION_APPLIER_CLASS, "Class name for parallel queue storage", PV_ANY)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(REPL_SVC_PARALLELIZATION_TYPE)) == "memory"
      "com.continuent.tungsten.replicator.storage.parallel.ParallelQueueApplier"
    elsif @config.getProperty(get_member_key(REPL_SVC_PARALLELIZATION_TYPE)) == "disk"
      "com.continuent.tungsten.replicator.thl.THLParallelQueueApplier"
    else
      "com.continuent.tungsten.replicator.storage.InMemoryQueueAdapter"
    end
  end
end

class ReplicationServiceParallelizationExtractorClass < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_SVC_PARALLELIZATION_EXTRACTOR_CLASS, "Class name for parallel queue storage", PV_ANY)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(REPL_SVC_PARALLELIZATION_TYPE)) == "memory"
      "com.continuent.tungsten.replicator.storage.parallel.ParallelQueueExtractor"
    elsif @config.getProperty(get_member_key(REPL_SVC_PARALLELIZATION_TYPE)) == "disk"
      "com.continuent.tungsten.replicator.thl.THLParallelQueueExtractor"
    else
      "com.continuent.tungsten.replicator.storage.InMemoryQueueAdapter"
    end
  end
end

class ReplicationServiceBufferSize < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_BUFFER_SIZE, "Replicator block commit size (min 1, max 100)",
      PV_REPL_BUFFER_SIZE, 10)
  end
end

class ReplicationServiceApplierBufferSize < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SVC_APPLIER_BUFFER_SIZE, "Applier block commit size (min 1)",
      PV_ANY, nil)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(BATCH_ENABLED)) == "true"
      return "10000"
    else
      return "${replicator.global.buffer.size}"
    end
  end
end

class ReplicationServiceSlaveTakeover < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_SVC_NATIVE_SLAVE_TAKEOVER, "Takeover native replication",
      PV_BOOLEAN, "false")
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_svc_native_slave_takeover'))
    super()
  end
  
  def get_command_line_argument_value
    "true"
  end
end

class BackupMethod < ConfigurePrompt
  include ReplicationServicePrompt

  def initialize
    super(REPL_BACKUP_METHOD, "Database backup method", nil)
  end
  
  def load_default_value
    @default = get_applier_datasource().get_default_backup_method()
  end
  
  def get_prompt
    "Database backup method (#{get_applier_datasource().get_valid_backup_methods()})"
  end
  
  def accept?(raw_value)
    @validator = PropertyValidator.new(get_applier_datasource().get_valid_backup_methods(),
      "Value must be #{get_applier_datasource().get_valid_backup_methods().split('|').join(', ')}")    
    super(raw_value)
  end
end

class BackupConfigurePrompt < ConfigurePrompt
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_BACKUP_METHOD)) != "none"
  end
end

class ScriptBackupConfigurePrompt < ConfigurePrompt
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_BACKUP_METHOD)) == "script"
  end
end

class ReplicationServiceBackupStorageDirectory < BackupConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_BACKUP_STORAGE_DIR, "Backup permanent shared storage", PV_FILENAME)
    self.extend(NotTungstenInstallerPrompt)
  end
  
  def load_default_value
    if @config.getProperty(get_member_key(DEPLOYMENT_DATASERVICE)).to_s == ""
      return
    end
    
    @default = @config.getProperty(get_host_key(REPL_BACKUP_STORAGE_DIR))
  end
end

class BackupStorageTempDirectory < BackupConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_BACKUP_DUMP_DIR, "Backup temporary dump directory", PV_FILENAME, "/tmp")
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_backup_dump_dir'))
    super()
  end
end

class BackupStorageRetention < BackupConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_BACKUP_RETENTION, "Number of backups to retain", PV_INTEGER, "3")
  end
end

class BackupScriptPathConfigurePrompt < ScriptBackupConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_BACKUP_SCRIPT, "What is the path to the backup script", PV_FILENAME)
  end
end

class BackupScriptCommandPrefixConfigurePrompt < BackupConfigurePrompt
  include ReplicationServicePrompt
  include HiddenValueModule
  
  def initialize
    super(REPL_BACKUP_COMMAND_PREFIX, "Use sudo when running the backup script?", PV_BOOLEAN, "false")
  end
  
  def load_default_value
    @default = @config.getPropertyOr(get_host_key(ROOT_PREFIX), "false")
  end
  
  def get_template_value(transform_values_method)
    if get_value() == "true"
      "sudo"
    else
      ""
    end
  end
end

class BackupScriptOnlineConfigurePrompt < ScriptBackupConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_BACKUP_ONLINE, "Does the backup script support backing up a datasource while it is ONLINE", PV_BOOLEAN, "false")
  end
end

class THLStorageChecksum < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_THL_DO_CHECKSUM, "Execute checksum operations on THL log files", 
      PV_BOOLEAN, "false")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class THLStorageConnectionTimeout < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_THL_LOG_CONNECTION_TIMEOUT, "Number of seconds to wait for a connection to the THL log", 
      PV_INTEGER, "600")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class THLStorageFileSize < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_THL_LOG_FILE_SIZE, "File size in bytes for THL disk logs", 
      PV_INTEGER, "100000000")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class THLStorageRetention < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_THL_LOG_RETENTION, "How long do you want to keep THL files?", 
      PV_ANY, "7d")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class THLSSL < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_ENABLE_THL_SSL, "Enable SSL encryption of THL communication for this service", PV_BOOLEAN, "false")
    add_command_line_alias("thl-ssl")
  end
end

class THLProtocol < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_THL_PROTOCOL, "Protocol to use for THL communication with this service", PV_ANY)
  end
  
  def load_default_value
    if @config.getProperty(get_member_key(REPL_ENABLE_THL_SSL)) == "true"
      @default = "thls"
    else
      @default = "thl"
    end
  end
end

class THLStorageConsistency < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_CONSISTENCY_POLICY, "Should the replicator stop or warn if a consistency check fails?", 
      PV_ANY, "stop")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class THLStorageFsync < ConfigurePrompt
  include ReplicationServicePrompt

  def initialize
    super(REPL_THL_LOG_FSYNC, "Fsync THL records on commit.  More reliable operation but adds latency to replication when using low-performance storage",
      PV_BOOLEAN, "false")
  end

  def enabled?
    super() && @config.getProperty(get_host_key(REPL_LOG_TYPE)) == "disk"
  end
end

class LogSlaveUpdates < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(LOG_SLAVE_UPDATES, "Should slaves log updates to binlog", 
      PV_BOOLEAN, "false")
  end
end

class SlavePrivilegedUpdates < ConfigurePrompt
  include ReplicationServicePrompt

  def initialize
    super(SLAVE_PRIVILEGED_UPDATES, "Does login for slave update have superuser privileges",
      PV_BOOLEAN, "true")
  end
end

class ReplicationServiceClusterEnabled < ConfigurePrompt
  include ReplicationServicePrompt
  include HiddenValueModule
  
  def initialize
    super(REPL_SVC_CLUSTER_ENABLED, "Should this replication service be used for HA?", 
      PV_BOOLEAN)
  end
  
  def load_default_value
    if get_topology().use_management?()
      @default = "true"
    else
      @default = "false"
    end
  end
end

class PrefetchEnabled < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(PREFETCH_ENABLED, "Should the replicator service be setup as a prefetch applier", 
      PV_BOOLEAN, "false")
  end
end

module PrefetchModule
  def enabled?
    super() && @config.getProperty(get_member_key(PREFETCH_ENABLED)) == "true"
  end
  
  def enabled_for_config?
    super() && @config.getProperty(get_member_key(PREFETCH_ENABLED)) == "true"
  end
end

class PrefetchMaxTimeAhead < ConfigurePrompt
  include ReplicationServicePrompt
  include PrefetchModule
  
  def initialize
    super(PREFETCH_MAX_TIME_AHEAD, "Maximum number of seconds that the prefetch applier can get in front of the standard applier", 
      PV_INTEGER, 60)
  end
end

class PrefetchMinTimeAhead < ConfigurePrompt
  include ReplicationServicePrompt
  include PrefetchModule
  
  def initialize
    super(PREFETCH_MIN_TIME_AHEAD, "Minimum number of seconds that the prefetch applier must be in front of the standard applier", 
      PV_INTEGER, 60)
  end
end

class PrefetchSleepTime < ConfigurePrompt
  include ReplicationServicePrompt
  include PrefetchModule
  
  def initialize
    super(PREFETCH_SLEEP_TIME, "How long to wait when the prefetch applier gets too far ahead", 
      PV_INTEGER, 200)
  end
end

class PrefetchFilterSchemaName < ConfigurePrompt
  include ReplicationServicePrompt
  include PrefetchModule
  
  def initialize
    super(PREFETCH_SCHEMA, "Schema to watch for timing prefetch progress", 
      PV_IDENTIFIER)
  end
  
  def get_default_value
    "tungsten_#{@config.getProperty(get_member_key(DSNAME))}"
  end
end

class BatchEnabled < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(BATCH_ENABLED, "Should the replicator service use a batch applier", 
      PV_BOOLEAN, "false")
  end
end

module BatchModule
  def enabled?
    super() && @config.getProperty(get_member_key(BATCH_ENABLED)) == "true"
  end
  
  def enabled_for_config?
    super() && @config.getProperty(get_member_key(BATCH_ENABLED)) == "true"
  end
end

class BatchLoadTemplate < ConfigurePrompt
  include ReplicationServicePrompt
  include BatchModule

  def initialize
    super(BATCH_LOAD_TEMPLATE, "Value for the loadBatchTemplate property", 
      PV_IDENTIFIER, "mysql")
  end
end

class ReplicationServiceConfigFile < ConfigurePrompt
  include ReplicationServicePrompt
  include HiddenValueModule
  include NoSystemDefault
  
  def initialize
    super(REPL_SVC_CONFIG_FILE, "Path to replication service static properties file", 
      PV_FILENAME)
  end
  
  def get_default_value
    "#{@config.getProperty(PREPARE_DIRECTORY)}/tungsten-replicator/conf/static-#{@config.getProperty(get_member_key(DEPLOYMENT_SERVICE))}.properties"
  end
end

class ReplicationServiceDynamicConfigFile < ConfigurePrompt
  include ReplicationServicePrompt
  include HiddenValueModule
  include NoSystemDefault
  
  def initialize
    super(REPL_SVC_DYNAMIC_CONFIG, "Path to replication service dynamic properties file", 
      PV_FILENAME)
  end
  
  def get_default_value
    "#{@config.getProperty(PREPARE_DIRECTORY)}/tungsten-replicator/conf/dynamic-#{@config.getProperty(get_member_key(DEPLOYMENT_SERVICE))}.properties"
  end
end

class JavaMemorySize < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_JAVA_MEM_SIZE, "Replicator Java heap memory size in Mb (min 128)",
      PV_JAVA_MEM_SIZE, 1024)
  end
end

class JavaFileEncoding < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_JAVA_FILE_ENCODING, "Java platform charset (esp. for heterogeneous replication)",
      PV_ANY, "")
  end
  
  def required?
    false
  end
end

class JavaUserTimezone < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_JAVA_USER_TIMEZONE, "Java VM Timezone (esp. for cross-site replication)",
      PV_ANY, "")
  end
  
  def required?
    false
  end
end

class JavaGarbageCollection < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_JAVA_ENABLE_CONCURRENT_GC, "Replicator Java uses concurrent garbage collection",
      PV_BOOLEAN, "false")
  end
  
  def get_template_value(transform_values_method)
    if get_value() == "true"
      ""
    else
      "#"
    end
  end
end

class ReplicationServicePipelines < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_PIPELINES, "Replication service allowed pipelines")
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_DI
      if @config.getProperty(get_member_key(PREFETCH_ENABLED)) == "true"
        return REPL_ROLE_DI_PRE
      else
        return REPL_ROLE_DI
      end
	  else
	    if @config.getProperty(get_member_key(PREFETCH_ENABLED)) == "true"
	      if @config.getProperty(get_member_key(DSNAME)) != @config.getProperty(get_member_key(DEPLOYMENT_SERVICE))
          return REPL_ROLE_LOCAL_PRE
        else
          return REPL_ROLE_S_PRE
        end
      elsif @config.getProperty(get_member_key(RELAY_ENABLED)) == "true"
        return REPL_ROLE_S_RELAY
	    end
	    
	    begin
	      extractor_template = get_extractor_datasource().get_extractor_template()
	    rescue
	      if @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_S
	        return REPL_ROLE_S
	      else
	        raise "Unable to extract from #{get_extractor_datasource.get_connection_summary}"
	      end
	    end
	  
	    return "master,slave"
	  end
  end
end

class ReplicationServiceApplierConfig < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_SVC_APPLIER_CONFIG, "Replication service applier config properties")
  end
  
  def get_template_value(transform_values_method)
    transformer = Transformer.new(@config.getProperty(PREPARE_DIRECTORY) + "/" + 
      get_applier_datasource().get_applier_template())
    transformer.set_fixed_properties(@config.getTemplateValue(get_member_key(FIXED_PROPERTY_STRINGS)))
    transformer.transform_values(transform_values_method)
    
    return transformer.to_s
  end
end

class ReplicationServiceExtractorConfig < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_SVC_EXTRACTOR_CONFIG, "Replication service extractor config properties")
  end
  
  def get_template_value(transform_values_method)
    transformer = Transformer.new(@config.getProperty(PREPARE_DIRECTORY) + "/" + 
      get_extractor_datasource().get_extractor_template())
    transformer.set_fixed_properties(@config.getTemplateValue(get_member_key(FIXED_PROPERTY_STRINGS)))
    transformer.transform_values(transform_values_method)
    
    return transformer.to_s
  end
end

class ReplicationServiceFilterConfig < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_SVC_FILTER_CONFIG, "Replication service filter config properties")
  end
  
  def get_template_value(transform_values_method)
    output_lines = []
    
    Dir[@config.getProperty(PREPARE_DIRECTORY) + '/tungsten-replicator/samples/conf/filters/default/*.tpl'].sort().each do |file| 
      transformer = Transformer.new(file)
      transformer.set_fixed_properties(@config.getTemplateValue(get_member_key(FIXED_PROPERTY_STRINGS)))
      transformer.transform_values(transform_values_method)
      
      output_lines = output_lines + transformer.to_a + [""]
    end
    
    if get_applier_datasource().class != get_extractor_datasource.class
      Dir[@config.getProperty(PREPARE_DIRECTORY) + "/tungsten-replicator/samples/conf/filters/#{get_extractor_datasource().get_uri_scheme()}/*.tpl"].sort().each do |file| 
        transformer = Transformer.new(file)
        transformer.set_fixed_properties(@config.getTemplateValue(get_member_key(FIXED_PROPERTY_STRINGS)))
        transformer.transform_values(transform_values_method)
      
        output_lines = output_lines + transformer.to_a + [""]
      end
    end
    
    Dir[@config.getProperty(PREPARE_DIRECTORY) + "/tungsten-replicator/samples/conf/filters/#{get_applier_datasource().get_uri_scheme()}/*.tpl"].sort().each do |file| 
      transformer = Transformer.new(file)
      transformer.set_fixed_properties(@config.getTemplateValue(get_member_key(FIXED_PROPERTY_STRINGS)))
      transformer.transform_values(transform_values_method)
      
      output_lines = output_lines + transformer.to_a + [""]
    end
    
    return output_lines.join("\n")
  end
end

class ReplicationServiceBackupConfig < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_SVC_BACKUP_CONFIG, "Replication service backup config properties")
  end
  
  def get_template_value(transform_values_method)
    output_lines = []
    
    Dir[@config.getProperty(PREPARE_DIRECTORY) + '/tungsten-replicator/samples/conf/backup_methods/default/*.tpl'].sort().each do |file| 
      transformer = Transformer.new(file)
      transformer.set_fixed_properties(@config.getTemplateValue(get_member_key(FIXED_PROPERTY_STRINGS)))
      transformer.transform_values(transform_values_method)
      
      output_lines = output_lines + transformer.to_a + [""]
    end
    
    Dir[@config.getProperty(PREPARE_DIRECTORY) + "/tungsten-replicator/samples/conf/backup_methods/#{get_applier_datasource().get_uri_scheme()}/*.tpl"].sort().each do |file| 
      transformer = Transformer.new(file)
      transformer.set_fixed_properties(@config.getTemplateValue(get_member_key(FIXED_PROPERTY_STRINGS)))
      transformer.transform_values(transform_values_method)
      
      output_lines = output_lines + transformer.to_a + [""]
    end
    
    return output_lines.join("\n")
  end
end

class ReplicationServiceExtractorFilters < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SVC_EXTRACTOR_FILTERS, "Replication service extractor filters")
  end
  
  def get_template_value(transform_values_method)
    (get_value().to_s().split(",") + get_extractor_datasource().get_extractor_filters()).join(",")
  end
  
  def required?
    false
  end
end

class ReplicationServiceTHLFilters < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SVC_THL_FILTERS, "Replication service THL filters")
  end
  
  def get_template_value(transform_values_method)
    (get_value().to_s().split(",") + get_extractor_datasource().get_thl_filters()).join(",")
  end
  
  def required?
    false
  end
end

class ReplicationServiceApplierFilters < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SVC_APPLIER_FILTERS, "Replication service applier filters")
  end
  
  def get_template_value(transform_values_method)
    (get_value().to_s().split(",") + get_applier_datasource().get_applier_filters()).join(",")
  end
  
  def required?
    false
  end
end

class ReplicationServiceSchema < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_SVC_SCHEMA, "Replication service schema", PV_IDENTIFIER)
  end
  
  def load_default_value
    @default = @config.getProperty(get_dataservice_key(DATASERVICE_SCHEMA))
  end
  
  def required?
    false
  end
end

class ReplicationServiceTableEngine < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SVC_TABLE_ENGINE, "Replication service table engine")
  end
  
  def get_default_value
    get_applier_datasource().get_default_table_engine()
  end
  
  def get_validator
    PropertyValidator.new(get_applier_datasource().get_allowed_table_engines().join("|"), 
      "Value must be #{get_applier_datasource().get_allowed_table_engines().join(',')}")
  end
  
  def get_usage_prompt
    engines = get_applier_datasource().get_allowed_table_engines()
    if engines.size > 1
      get_prompt() + " (#{engines.join('|')})"
    else
      super()
    end
  end
end

class ReplicationServiceEnableShardComments < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_SVC_ENABLE_MASTER_SERVICE_COMMENTS, "Add a comment to extracted events with the current service name", PV_BOOLEAN, "false")
  end
end

class ReplicationServiceTHLStorageDirectory < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_LOG_DIR, "Replicator log directory", PV_FILENAME)
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
  
  def load_default_value
    if @config.getProperty(get_member_key(DEPLOYMENT_DATASERVICE)).to_s == ""
      return
    end
    
    if @config.getProperty(get_member_key(PREFETCH_ENABLED)) == "true"
      @default = get_directory(@config.getProperty(get_member_key(DSNAME)))
    else
      @default = get_directory(@config.getProperty(get_member_key(DEPLOYMENT_DATASERVICE)))
    end
  end
  
  def get_directory(svc_name)
    @config.getProperty(get_host_key(REPL_LOG_DIR)) + "/" + svc_name
  end
end

class ReplicationServiceRelayLogStorageDirectory < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_RELAY_LOG_DIR, "Directory for logs transferred from the master",
		  PV_FILENAME)
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def load_default_value
    if @config.getProperty(get_member_key(DEPLOYMENT_DATASERVICE)).to_s == ""
      return
    end
    
    @default = get_directory(@config.getProperty(get_member_key(DEPLOYMENT_DATASERVICE)))
  end
  
  def get_directory(svc_name)
    @config.getProperty(get_host_key(REPL_RELAY_LOG_DIR)) + "/" + svc_name
  end
end

class ReplicationServiceGlobalProperties < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(FIXED_PROPERTY_STRINGS, "Fixed properties for this replication service")
  end
  
  def get_value(allow_default = true, allow_disabled = false)
    if allow_default && (enabled_for_config?() || allow_disabled)
      value = get_default_value()
    else
      value = super()
    end
        
    value
  end
  
  def load_default_value
    @default = []
  end
  
  def get_default_value
    if get_member() == DEFAULTS
      return super()
    end
    
    values = @config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS))
    
    defaults = get_group_default_value()
    if defaults.is_a?(Array)
      values = values + defaults
    end
    
    ds_defaults = @config.getNestedProperty(get_hash_prompt_key())
    if ds_defaults.is_a?(Array)
      values = values + ds_defaults
    end
    
    ds_defaults = @config.getNestedProperty([DATASERVICE_HOST_OPTIONS, get_dataservice_alias(), FIXED_PROPERTY_STRINGS])
    if ds_defaults.is_a?(Array)
      values = values + ds_defaults
    end
    
    ds_values = @config.getTemplateValue(get_dataservice_key(FIXED_PROPERTY_STRINGS))
    if ds_values.is_a?(Array)
      values = values + ds_values
    end
    
    my_value = @config.getNestedProperty(get_name())
    if my_value.is_a?(Array)
      values = values + my_value
    end
    
    return values
  end
  
  def required?
    false
  end
  
  def build_command_line_argument(values)
    args = []
    
    if values.is_a?(Array)
      values.each{
        |v|
        args << "--property=#{v}"
      }
    elsif values.to_s() != ""
      args << "--property=#{values}"
    end
    
    if args.length == 0
      raise IgnoreError
    else
      return args
    end
  end
end
