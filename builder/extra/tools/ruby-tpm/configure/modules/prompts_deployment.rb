class ClusterHosts < GroupConfigurePrompt
  def initialize
    super(HOSTS, "Enter host information for @value", 
      "host", "hosts", "HOST")
      
    ClusterHostPrompt.subclasses().each{
      |klass|
      self.add_prompt(klass.new())
    }
  end
  
  def update_deprecated_keys()
    each_member{
      |member|
      
      @config.setProperty([HOSTS, member, 'shell_startup_script'], nil)
    }
    
    super()
  end
  
  def get_new_alias_prompt
    TemporaryPrompt.new("What host would you like to configure?  Enter nothing to stop entering #{@plural}.")
  end
  
  def validate_new_alias(new_alias)
    super(to_identifier(new_alias))
  end
  
  def add_alias(new_alias)
    fixed_alias = to_identifier(new_alias)
    
    super(fixed_alias)
    
    @config.setProperty([get_name(), fixed_alias, HOST], new_alias)
  end
end

# Prompts that include this module will be collected for each host 
# across interactive mode, the configure script and the
# tungsten-installer script
module ClusterHostPrompt
  include GroupConfigurePromptMember
  include HashPromptDefaultsModule
  
  def self.included(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end

  def self.subclasses
    @subclasses || []
  end
  
  def get_command_line_argument()
    super.gsub("repl-", "")
  end
  
  def get_command_line_aliases
    super() + super().collect{|al| al.gsub("repl-", "")} + [@name.gsub("_", "-")]
  end
  
  def get_userid
    @config.getProperty(get_member_key(USERID))
  end
  
  def get_hostname
    @config.getProperty(get_member_key(HOST))
  end
  
  def get_host_alias
    get_member()
  end
  
  def allow_group_default
    true
  end
  
  def get_hash_prompt_key
    return [DATASERVICE_HOST_OPTIONS, @config.getProperty(get_member_key(DEPLOYMENT_DATASERVICE)), @name]
  end
end

class DeploymentServicePrompt < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  
  def initialize
    super(DEPLOYMENT_SERVICE, "Deployment Service", 
      PV_ANY)
  end
end

class DeploymentCommandPrompt < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include NoStoredConfigValue
  include NoReplicatorRestart
  include NoManagerRestart
  include NoConnectorRestart
  
  def initialize
    super(DEPLOYMENT_COMMAND, "Current command being run", PV_ANY)
  end
end

class StagingHost < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include NoReplicatorRestart
  include NoManagerRestart
  include NoConnectorRestart
  
  def initialize
    super(STAGING_HOST, "Host being used to install", PV_ANY)
  end
end

class StagingUser < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include NoReplicatorRestart
  include NoManagerRestart
  include NoConnectorRestart
  
  def initialize
    super(STAGING_USER, "User being used to install", PV_ANY)
  end
end

class StagingDirectory < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include NoReplicatorRestart
  include NoManagerRestart
  include NoConnectorRestart
  
  def initialize
    super(STAGING_DIRECTORY, "Directory being used to install", PV_ANY)
  end
end

class DeploymentHost < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  
  def initialize
    super(DEPLOYMENT_HOST, "Host alias for the host to be deployed here", PV_ANY)
  end
end

class RemotePackagePath < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include NoStoredServerConfigValue
  include NoReplicatorRestart
  include NoManagerRestart
  include NoConnectorRestart
  
  def initialize
    super(REMOTE_PACKAGE_PATH, "Path on the server to use for running tpm commands", PV_FILENAME)
  end
end

class DeployCurrentPackagePrompt < ConfigurePrompt
  include AdvancedPromptModule
  include NoTemplateValuePrompt
  include NoStoredServerConfigValue
  
  def initialize
    super(DEPLOY_CURRENT_PACKAGE, "Deploy the current Tungsten package", PV_BOOLEAN, "true")
    self.extend(NotTungstenUpdatePrompt)
  end
end

class DeployPackageURIPrompt < ConfigurePrompt
  include AdvancedPromptModule
  include NoTemplateValuePrompt
  include NoStoredServerConfigValue

  def initialize
    super(DEPLOY_PACKAGE_URI, "URL for the Tungsten package to deploy", PV_URI)
    self.extend(NotTungstenUpdatePrompt)
  end

  def enabled?
    @config.getProperty(DEPLOY_CURRENT_PACKAGE) != "true"
  end
  
  def load_default_value
    if enabled?
      @default = "https://s3.amazonaws.com/releases.continuent.com/#{Configurator.instance.get_release_name()}.tar.gz"
    else
      @default = nil
    end
  end
  
  def accept?(raw_value)
    value = super(raw_value)
    if value.to_s == ""
      return value
    end
    
    uri = URI::parse(value)
    if uri.scheme == "http" || uri.scheme == "https"
      unless value =~ /.tar.gz/
        raise "Only files ending in .tar.gz may be fetched using #{uri.scheme.upcase}"
      end
      
      return value
    elsif uri.scheme == "file"
      if (uri.host == "localhost")
        package_basename = File.basename(uri.path)
        if package_basename =~ /.tar.gz$/
          return value
        elsif package_basename =~ /.tar$/
          return value
        elsif File.extname(uri.path) == ""
          return value
        else
          raise "#{package_basename} is not a directory or recognized archive file"
        end
      elsif (uri.host == "remote")
        package_basename = File.basename(uri.path)
        if package_basename =~ /.tar.gz$/
          return value
        elsif package_basename =~ /.tar$/
          return value
        elsif File.extname(uri.path) == ""
          return value
        else
          raise "#{package_basename} is not a directory or recognized archive file"
        end
      end
    else
      raise "#{uri.scheme.upcase()} is an unrecognized scheme for the deployment package"
    end
  end
end

class ConfigTargetBasenamePrompt < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  
  def initialize
    super(CONFIG_TARGET_BASENAME, "", 
      PV_ANY)
  end
  
  def load_default_value
    if "#{@config.getProperty(HOME_DIRECTORY)}/#{RELEASES_DIRECTORY_NAME}/#{Configurator.instance.get_basename()}" == Configurator.instance.get_base_path()
      @default = Configurator.instance.get_basename()
    elsif Configurator.instance.is_locked?()
      @default = Configurator.instance.get_basename()
    else
      @default = Configurator.instance.get_unique_basename()
    end
  end
  
  def save_value?(v)
    true
  end
end

class HostPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(HOST, "DNS hostname", PV_HOSTNAME)
  end
  
  def load_default_value
    @default = get_member()
  end
  
  def allow_group_default
    false
  end
end

class UserIDPrompt < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(USERID, "System User", 
      PV_IDENTIFIER, Configurator.instance.whoami())
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('userid'))
    super()
  end
end

class HomeDirectoryPrompt < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(HOME_DIRECTORY, "Installation directory", PV_FILENAME)
    self.extend(NotTungstenUpdatePrompt)
    override_command_line_argument("install-directory")
  end
  
  def load_default_value
    @default = Configurator.instance.get_continuent_root()
  end
  
  def validate_value(value)
    super(value)
    
    if value == Configurator.instance.get_base_path()
      error("You must specify a separate location to deploy Continuent Tungsten")
    end
  end
  
  def save_value?(v)
    true
  end
end

class BaseDirectoryPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(CURRENT_RELEASE_DIRECTORY, "Directory for the latest release", PV_FILENAME)
  end
  
  def load_default_value
    if @config.getProperty(get_member_key(HOME_DIRECTORY)) == Configurator.instance.get_base_path()
      @default = @config.getProperty(get_member_key(HOME_DIRECTORY))
    else
      @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/#{Configurator::CURRENT_RELEASE_DIRECTORY}"
    end
  end
  
  def enabled_for_command_line?()
    false
  end
end

class ReleasesDirectoryPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(RELEASES_DIRECTORY, "Directory for storing releases", PV_FILENAME)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/#{RELEASES_DIRECTORY_NAME}"
  end
end

class LogsDirectoryPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(LOGS_DIRECTORY, "Directory for storing logs", PV_FILENAME)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/#{LOGS_DIRECTORY_NAME}"
  end
end

class ConfigDirectoryPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(CONFIG_DIRECTORY, "Directory for storing the current config", PV_FILENAME)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/#{CONFIG_DIRECTORY_NAME}"
  end
end

class TempDirectoryPrompt < ConfigurePrompt
  include AdvancedPromptModule
  include ClusterHostPrompt
  include NoConnectorRestart
  include NoReplicatorRestart
  include NoManagerRestart
  
  def initialize
    super(TEMP_DIRECTORY, "Temporary Directory",
      PV_FILENAME, "/tmp")
  end
  
  def load_default_value
    if @default != nil
      return
    end
    
    if get_member() == DEFAULTS
      return
    end
    
    ["TMPDIR", "TMP", "TEMP"].each{|var|      
      begin
        Timeout.timeout(5){
          val = ssh_result("echo $#{var}", @config.getProperty(get_member_key(HOST)), @config.getProperty(get_member_key(USERID)))
          if val != ""
            @default = val
          end
        }
      rescue Timeout::Error
      rescue RemoteCommandError
      rescue CommandError
      rescue MessageError
      end
    }
    
    if @default.to_s == ""
      @default = "/tmp"
    end
    
    @config.setProperty([SYSTEM] + get_name().split('.'), @default)
  end
  
  def get_timeout_length
    20
  end
end

class TargetDirectoryPrompt < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include ClusterHostPrompt
  
  def initialize
    super(TARGET_DIRECTORY, "Target for committing the deployment",
      PV_FILENAME)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/#{RELEASES_DIRECTORY_NAME}/#{@config.getProperty(get_member_key(TARGET_BASENAME))}"
  end
end

class TargetBasenamePrompt < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include ClusterHostPrompt
  
  def initialize
    super(TARGET_BASENAME, "Target for writing the deployment",
      PV_ANY)
  end
  
  def load_default_value
    if (@default = @config.getProperty(CONFIG_TARGET_BASENAME))
      return
    end
    
    if @config.getProperty(get_member_key(HOME_DIRECTORY)) == Configurator.instance.get_base_path()
      @default = File.basename(Configurator.instance.get_base_path())
    elsif "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/#{RELEASES_DIRECTORY_NAME}/#{Configurator.instance.get_basename()}" == Configurator.instance.get_base_path()
      @default = File.basename(Configurator.instance.get_base_path())
    else
      if @config.getProperty(DEPLOY_CURRENT_PACKAGE) == "true"
        @default = File.basename(Configurator.instance.get_package_path())
      else
        uri = URI::parse(@config.getProperty(get_member_key(DEPLOY_PACKAGE_URI)))
        package_basename = File.basename(uri.path)          
        if package_basename =~ /.tar.gz$/
          package_basename = File.basename(package_basename, ".tar.gz")
        elsif package_basename =~ /.tar$/
          package_basename = File.basename(package_basename, ".tar")
        end
        
        @default = package_basename
      end
    end
  end
end

class PrepareDirectoryPrompt < ConfigurePrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  include ClusterHostPrompt
  include NoSystemDefault
  
  def initialize
    super(PREPARE_DIRECTORY, "Target for preparing the deployment",
      PV_FILENAME)
  end
  
  def get_default_value
    target_directory = @config.getProperty(get_member_key(TARGET_DIRECTORY))
    if File.exist?(target_directory)
      return target_directory
    else
      return "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/#{RELEASES_DIRECTORY_NAME}/#{PREPARE_RELEASE_DIRECTORY}/#{@config.getProperty(get_member_key(TARGET_BASENAME))}"
    end
  end
end

class DirectoryLockPrompt < ConfigurePrompt
  include ConstantValueModule
  include ClusterHostPrompt
  include NoSystemDefault
  
  def initialize
    super(DIRECTORY_LOCK_FILE, "Filename for locking a directory from further configuration",
      PV_FILENAME)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(PREPARE_DIRECTORY))}/#{DIRECTORY_LOCK_FILENAME}"
  end
end

class HostEnableManager < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(HOST_ENABLE_MANAGER, "Enable the manager", PV_BOOLEAN)
  end
  
  def load_default_value
    @default = false.to_s
    
    if get_member() == DEFAULTS
      @default = false.to_s
    else
      @config.getPropertyOr([REPL_SERVICES], {}).each_key{
        |rs_alias|
        
        if rs_alias == DEFAULTS
          next
        end
        
        if @config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_HOST]) == get_member()
          topology = Topology.build(@config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_DATASERVICE]), @config)
          
          if topology.use_management?()
            @default = true.to_s
          end
        end
      }
    end
  end
end

class HostEnableReplicator < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(HOST_ENABLE_REPLICATOR, "Enable the replicator", PV_BOOLEAN)
  end
  
  def load_default_value
    @default = false.to_s
    
    if get_member() == DEFAULTS
      @default = false.to_s
    else
      @config.getPropertyOr([REPL_SERVICES], {}).each_key{
        |rs_alias|
        
        if rs_alias == DEFAULTS
          next
        end
        
        if @config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_HOST]) == get_member()
          @default = true.to_s
        end
      }
    end
  end
end

class HostEnableConnector < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(HOST_ENABLE_CONNECTOR, "Enable the connector", PV_BOOLEAN)
  end
  
  def load_default_value
    @default = false.to_s
    
    if get_member() == DEFAULTS
      @default = true.to_s
    else
      @config.getPropertyOr([CONNECTORS], {}).each_key{
        |h_alias|
        
        if h_alias == DEFAULTS
          next
        end
        
        if @config.getProperty([CONNECTORS, h_alias, DEPLOYMENT_HOST]) == get_member()
          @default = true.to_s
        end
      }
    end
  end
end

class HostDefaultDataserviceName < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(DEPLOYMENT_DATASERVICE, "Name of the default dataservice for this host", PV_ANY)
  end
  
  def allow_group_default
    false
  end
  
  def load_default_value
    @default = DEFAULT_SERVICE_NAME
    
    non_cluster_ds_alias = nil
    @config.getNestedPropertyOr([REPL_SERVICES], {}).each_key{
      |rs_alias|
      if rs_alias == DEFAULTS
        next
      end
      
      if @config.getNestedProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_HOST]) == get_member()
        ds_alias = @config.getNestedProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_DATASERVICE])
        if ds_alias != ""
          if Topology.build(ds_alias, @config).use_management?()
            @default = ds_alias
            return
          else
            non_cluster_ds_alias = ds_alias
          end
        end
      end
    }
    
    @config.getNestedPropertyOr([CONNECTORS], {}).each_key{
      |h_alias|
      if h_alias == DEFAULTS
        next
      end
      
      if @config.getNestedProperty([CONNECTORS, h_alias, DEPLOYMENT_HOST]) == get_member()
        ds_aliases = @config.getNestedProperty([CONNECTORS, h_alias, DEPLOYMENT_DATASERVICE])
        if ! ds_aliases.kind_of?(Array)
           ds_aliases=Array(ds_aliases);
        end
        if ds_aliases.size() > 0
          @default = ds_aliases.at(0)
          return
        end
      end
    }
    
    if non_cluster_ds_alias != nil
      @default = non_cluster_ds_alias
      return
    end
  end
end

class DatasourceDBType < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    validator = PropertyValidator.new(ConfigureDatabasePlatform.get_types().join("|"), 
      "Value must be #{ConfigureDatabasePlatform.get_types().join(',')}")
      
    super(REPL_DBTYPE, "Database type (#{ConfigureDatabasePlatform.get_types().join(',')})", 
        validator)
  end
  
  def load_default_value
    case @config.getProperty(get_host_key(USERID))
    when "postgres"
      @default = "postgresql-wal"
    when "enterprisedb"
      @default = "postgresql-wal"
    else
      if @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_ARCHIVE
        @default = ""
      else
        @default = "mysql"
      end
    end
  end
  
  def get_disabled_value
    get_default_value()
  end
  
  def enabled_for_load_default_value?
    return enabled?()
  end
end

class RootCommandPrefixPrompt < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(ROOT_PREFIX, "Run root commands using sudo", 
      PV_BOOLEAN, "false")
  end
  
  def load_default_value
    if @config.getProperty(USERID) != "root" && @config.getProperty(HOST_ENABLE_MANAGER) == "true"
      @default = "true"
    else
      @default = "false"
    end
  end
  
  def get_template_value(transform_values_method)
    if get_value() == "true"
      "sudo -n"
    else
      ""
    end
  end
  
  def get_command_line_argument()
    "enable-sudo-access"
  end
  
  def get_command_line_aliases
    [@name.gsub("_", "-")]
  end
end

class THLStorageType < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_LOG_TYPE, "Replicator event log storage (dbms|disk)",
      PV_LOGTYPE, "disk")
  end
  
  def get_disabled_value
    "disk"
  end
end

class THLStorageDirectory < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(REPL_LOG_DIR, "Replicator log directory", PV_FILENAME)
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/thl"
  end
  
  def get_output_usage_value
    if @config.getProperty(get_member_key(HOME_DIRECTORY)).to_s == ""
      "<home directory>/thl"
    else
      super()
    end
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_log_dir'))
    super()
  end
end

class RelayLogStorageDirectory < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(REPL_RELAY_LOG_DIR, "Directory for logs transferred from the master",
		  PV_FILENAME)
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/relay"
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_relay_log_dir'))
    super()
  end

  def get_output_usage_value
    if @config.getProperty(get_member_key(HOME_DIRECTORY)).to_s == ""
      "<home directory>/relay"
    else
      super()
    end
  end
end

class BackupStorageDirectory < BackupConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(REPL_BACKUP_STORAGE_DIR, "Permanent backup storage directory", PV_FILENAME)
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/backups"
  end
  
  def get_output_usage_value
    if @config.getProperty(get_member_key(HOME_DIRECTORY)).to_s == ""
      "<home directory>/backups"
    else
      super()
    end
  end
end

class InstallServicesPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  
  def initialize
    super(SVC_INSTALL, "Install service start scripts", 
      PV_BOOLEAN, "false")
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def enabled?
    (@config.getProperty(get_member_key(USERID)) == "root" || 
      @config.getProperty(get_member_key(ROOT_PREFIX)) == "true")
  end
end

class StartServicesPrompt < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(SVC_START, "Start the services after configuration", 
      PV_BOOLEAN)
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(SVC_REPORT))
  end
  
  def get_command_line_argument_value
    "true"
  end
end

class ReportServicesPrompt < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(SVC_REPORT, "Start the services and report out the status after configuration", 
      PV_BOOLEAN, "false")
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def get_command_line_argument_value
    "true"
  end
end

class ProfileScriptPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include NoConnectorRestart
  include NoReplicatorRestart
  include NoManagerRestart
  
  def initialize
    super(PROFILE_SCRIPT, "Append commands to include env.sh in this profile script", PV_ANY, "")
  end
  
  def required?
    false
  end
end

class HostServicePathReplicator < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(SVC_PATH_REPLICATOR, "Path to the replicator service command", PV_FILENAME)
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(CURRENT_RELEASE_DIRECTORY)) + "/tungsten-replicator/bin/replicator"
  end
end

class HostServicePathManager < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(SVC_PATH_MANAGER, "Path to the manager service command", PV_FILENAME)
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(CURRENT_RELEASE_DIRECTORY)) + "/tungsten-manager/bin/manager"
  end
end

class HostServicePathConnector < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(SVC_PATH_CONNECTOR, "Path to the connector service command", PV_FILENAME)
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(CURRENT_RELEASE_DIRECTORY)) + "/tungsten-connector/bin/connector"
  end
end

class HostGlobalProperties < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(FIXED_PROPERTY_STRINGS, "Fixed properties for this host")
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
    
    values = []
    
    defaults = get_group_default_value()
    if defaults.is_a?(Array)
      values = values + defaults
    end
    
    ds_defaults = @config.getNestedProperty(get_hash_prompt_key())
    if ds_defaults.is_a?(Array)
      values = values + ds_defaults
    end
    
    host_value = @config.getNestedProperty(get_name())
    if host_value.is_a?(Array)
      values = values + host_value
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

class HostSkippedChecks < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(SKIPPED_VALIDATION_CLASSES, "Skipped validation classes for this host")
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
    
    values = []
    
    defaults = get_group_default_value()
    if defaults.is_a?(Array)
      values = values + defaults
    end
    
    ds_defaults = @config.getNestedProperty(get_hash_prompt_key())
    if ds_defaults.is_a?(Array)
      values = values + ds_defaults
    end
    
    host_value = @config.getNestedProperty(get_name())
    if host_value.is_a?(Array)
      values = values + host_value
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
        args << "--skip-validation-check=#{v}"
      }
    elsif values.to_s() != ""
      args << "--skip-validation-check=#{values}"
    end
    
    if args.length == 0
      raise IgnoreError
    else
      return args
    end
  end
end

class HostSkippedWarnings < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(SKIPPED_VALIDATION_WARNINGS, "Skipped validation warnings for this host")
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
    
    values = []
    
    defaults = get_group_default_value()
    if defaults.is_a?(Array)
      values = values + defaults
    end
    
    ds_defaults = @config.getNestedProperty(get_hash_prompt_key())
    if ds_defaults.is_a?(Array)
      values = values + ds_defaults
    end
    
    host_value = @config.getNestedProperty(get_name())
    if host_value.is_a?(Array)
      values = values + host_value
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
        args << "--skip-validation-warnings=#{v}"
      }
    elsif values.to_s() != ""
      args << "--skip-validation-warnings=#{values}"
    end
    
    if args.length == 0
      raise IgnoreError
    else
      return args
    end
  end
end

class HostPreferredPath < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  
  def initialize
    super(PREFERRED_PATH, "Additional command path", PV_ANY, "")
  end
end

class HostSkipStatemap < ConfigurePrompt
  include ClusterHostPrompt
  include CommercialPrompt
  
  def initialize
    super(SKIP_STATEMAP, "Do not copy the cluster-home/conf/statemap.properties from the previous install", PV_BOOLEAN, "false")
  end
end

class HostDataServiceName < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(DATASERVICENAME, "Name of this dataservice", PV_IDENTIFIER, DEFAULT_SERVICE_NAME)
    self.extend(NotTungstenUpdatePrompt)
  end
  
  def allow_group_default
    false
  end
  
  def load_default_value
    @config.getPropertyOr(DATASERVICES, {}).each_key{
      |ds_alias|
      
      if ds_alias == DEFAULTS
        next
      end
      
      ds_members = @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_MEMBERS], "").split(',')
      if ds_members.include?(@config.getProperty(get_member_key(HOST)))
        if Topology.build(ds_alias, @config).use_management?()
          @default = @config.getProperty([DATASERVICES, ds_alias, DATASERVICENAME])
        end
      end
    }
  end
end

class HostSecurityDirectory < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(SECURITY_DIRECTORY, "Storage directory for the Java security/encryption files", PV_FILENAME)
  end
  
  def load_default_value
    @default = "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/share"
  end
end

class HostEnableRMIAuthentication < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(ENABLE_RMI_AUTHENTICATION, "Enable RMI authentication for the services running on this host", PV_BOOLEAN, "false")
    add_command_line_alias("rmi-authentication")
  end
end

class HostEnableRMISSL < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(ENABLE_RMI_SSL, "Enable SSL encryption of RMI communication on this host", PV_BOOLEAN, "false")
    add_command_line_alias("rmi-ssl")
  end
end

class HostRMIUser < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(RMI_USER, "The username for RMI authentication", PV_ANY, "tungsten")
  end
end

class HostJavaKeystorePassword < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(JAVA_KEYSTORE_PASSWORD, "The password for unlocking the tungsten_keystore.jks file in the security directory", PV_ANY, "tungsten")
  end
end

class HostJavaTruststorePassword < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(JAVA_TRUSTSTORE_PASSWORD, "The password for unlocking the tungsten_truststore.jks file in the security directory", PV_ANY, "tungsten")
  end
end

class HostPortsForUsers < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  include NoSystemDefault
  
  def initialize
    super(PORTS_FOR_USERS, "Ports opened on this server for all users and applications", PV_ANY)
  end
  
  def load_default_value
    @default = []
    host_alias = @config.getProperty(DEPLOYMENT_HOST)
    
    (PortForUsers.paths||[]).each{
      |path|
      @config.getPropertyOr(path[:gr], {}).each_key{
        |g_key|
        if g_key == DEFAULTS
          next
        end
        
        if @config.getProperty([path[:gr], g_key, DEPLOYMENT_HOST]) == host_alias
          min = @config.getProperty([path[:gr], g_key, path[:min]]).to_i()
          
          if path[:max] != nil
            p = min
            max = @config.getProperty([path[:gr], g_key, path[:max]]).to_i()
            while p <= max
              @default << p
              p = p+1
            end
          else
            @default << min
          end
        end
      }
    }
    
    @default = @default.uniq().sort()
  end
end

class HostPortsForConnectors < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  include NoSystemDefault
  
  def initialize
    super(PORTS_FOR_CONNECTORS, "Ports opened on this server for connector communications", PV_ANY)
  end
  
  def load_default_value
    @default = []
    host_alias = @config.getProperty(DEPLOYMENT_HOST)
    
    (PortForConnectors.paths||[]).each{
      |path|
      @config.getPropertyOr(path[:gr], {}).each_key{
        |g_key|
        if g_key == DEFAULTS
          next
        end
        
        if @config.getProperty([path[:gr], g_key, DEPLOYMENT_HOST]) == host_alias
          min = @config.getProperty([path[:gr], g_key, path[:min]]).to_i()
          
          if path[:max] != nil
            p = min
            max = @config.getProperty([path[:gr], g_key, path[:max]]).to_i()
            while p <= max
              @default << p
              p = p+1
            end
          else
            @default << min
          end
        end
      }
    }
    
    @default = @default.uniq().sort()
  end
end

class HostPortsForManagers < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  include NoSystemDefault
  
  def initialize
    super(PORTS_FOR_MANAGERS, "Ports opened on this server for manager communications", PV_ANY)
  end
  
  def load_default_value
    @default = []
    host_alias = @config.getProperty(DEPLOYMENT_HOST)
    
    (PortForManagers.paths||[]).each{
      |path|
      @config.getPropertyOr(path[:gr], {}).each_key{
        |g_key|
        if g_key == DEFAULTS
          next
        end
        
        if @config.getProperty([path[:gr], g_key, DEPLOYMENT_HOST]) == host_alias
          min = @config.getProperty([path[:gr], g_key, path[:min]]).to_i()
          
          if path[:max] != nil
            p = min
            max = @config.getProperty([path[:gr], g_key, path[:max]]).to_i()
            while p <= max
              @default << p
              p = p+1
            end
          else
            @default << min
          end
        end
      }
    }
    
    @default = @default.uniq().sort()
  end
end

class HostPortsForReplicators < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  include NoSystemDefault
  
  def initialize
    super(PORTS_FOR_REPLICATORS, "Ports opened on this server for replicator communications", PV_ANY)
  end
  
  def load_default_value
    @default = []
    host_alias = @config.getProperty(DEPLOYMENT_HOST)
    
    (PortForReplicators.paths||[]).each{
      |path|
      @config.getPropertyOr(path[:gr], {}).each_key{
        |g_key|
        if g_key == DEFAULTS
          next
        end
        
        if @config.getProperty([path[:gr], g_key, DEPLOYMENT_HOST]) == host_alias
          min = @config.getProperty([path[:gr], g_key, path[:min]]).to_i()
          
          if path[:max] != nil
            p = min
            max = @config.getProperty([path[:gr], g_key, path[:max]]).to_i()
            while p <= max
              @default << p
              p = p+1
            end
          else
            @default << min
          end
        end
      }
    }
    
    @default = @default.uniq().sort()
  end
end