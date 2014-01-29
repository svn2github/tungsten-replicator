# Prompts that include this module will be collected for each datasource 
# across interactive mode, the tungsten-installer script
module DatasourcePrompt
  include GroupConfigurePromptMember
  include HashPromptDefaultsModule
  include NoManagerRestart
  include NoConnectorRestart
  
  def self.included(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end

  def self.subclasses
    @subclasses || []
  end
  
  def get_datasource
    ConfigureDatabasePlatform.build([@parent_group.name, get_member()], @config)
  end
  
  def get_command_line_argument()
    super.gsub("repl-", "")
  end
  
  def get_command_line_aliases
    super() + super().collect{|al| al.gsub("repl-", "")} + [@name.gsub("_", "-")]
  end
  
  def get_host_alias
    @config.getProperty(get_member_key(DEPLOYMENT_HOST))
  end
  
  def get_host_key(key)
    if get_member() == DEFAULTS
      return [HOSTS, DEFAULTS, key]
    end
    
    host_alias = get_host_alias()
    if host_alias == nil
      raise "Unable to find the host alias for this replication service (#{get_member()}:#{get_name()})"
    end
    
    [HOSTS, host_alias, key]
  end
  
  def get_dataservice_alias
    @config.getProperty(get_member_key(DEPLOYMENT_DATASERVICE))
  end
  
  def get_dataservice_key(key)
    [DATASERVICES, get_dataservice_alias(), key]
  end
  
  def get_topology
    Topology.build(get_dataservice_alias(), @config)
  end
  
  def get_userid
    host_alias = get_host_alias()
    if host_alias == nil
      super
    end
    
    @config.getProperty([HOSTS, host_alias, USERID])
  end
  
  def get_hostname
    host_alias = get_host_alias()
    if host_alias == nil
      super
    end
    
    @config.getProperty([HOSTS, host_alias, HOST])
  end
  
  def allow_group_default
    true
  end
  
  def get_hash_prompt_key
    return [DATASERVICE_REPLICATION_OPTIONS, @config.getProperty(get_member_key(DEPLOYMENT_DATASERVICE)), @name]
  end
  
  def enabled_for_command_dataservice?
    host_alias = get_host_alias()
    @config.getPropertyOr(REPL_SERVICES, []).keys().each{
      |rs_alias|
      
      if @config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_HOST]) == host_alias
        if Configurator.instance.command.include_dataservice?(@config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_DATASERVICE]))
          return true
        end
      end
    }
    
    return false
  end
  
  def get_display_member
    "Host #{get_hostname()}"
  end
end

class DatasourceDBHost < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_DBHOST, "Database server hostname", PV_HOSTNAME)
    override_command_line_argument("replication-host")
  end
  
  def load_default_value
    @default = @config.getPropertyOr(get_host_key(HOST), Configurator.instance.hostname())
  end
  
  def allow_group_default
    false
  end
end

class DatasourceDBPort < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_DBPORT, "Database server port", PV_INTEGER)
    override_command_line_argument("replication-port")
  end
  
  def load_default_value
    @default = get_datasource().get_default_port()
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_dbport'))
    super()
  end
  
  PortForConnectors.register(REPL_SERVICES, REPL_DBPORT)
  PortForManagers.register(REPL_SERVICES, REPL_DBPORT)
end

class DatasourceDBUser < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_DBLOGIN, "Database login for Tungsten", 
      PV_IDENTIFIER, Configurator.instance.whoami())
    override_command_line_argument("replication-user")
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_admin_login'))
    super()
  end
end

class DatasourceDBPassword < ConfigurePrompt
  include DatasourcePrompt
  include ManagerRestart
  include PrivateArgumentModule
  
  def initialize
    super(REPL_DBPASSWORD, "Database password", 
      PV_ANY, "")
    override_command_line_argument("replication-password")
  end
    
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_admin_password'))
    super()
  end
end

class DatasourceInitScript < ConfigurePrompt
  include DatasourcePrompt
  include AdvancedPromptModule
  include FindFilesystemDefaultModule

  def initialize
    super(REPL_BOOT_SCRIPT, "Database start script", PV_FILENAME)
  end
  
  def load_default_value
    @default = get_datasource.get_default_start_script()
  end
  
  def required?
    (get_default_value() != nil)
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_boot_script'))
    super()
  end
  
  def get_search_paths
    ["/etc/init.d/mysql", "/etc/init.d/mysqld", "/etc/init.d/postgres"]
  end
end

class DatasourceInitServiceName< ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule

  def initialize
    super(REPL_BOOT_SERVICE_NAME, "Database service name", PV_ANY)
  end
  
  def load_default_value
    boot_script = @config.getProperty(get_member_key(REPL_BOOT_SCRIPT))
    
    unless boot_script.to_s() == ""
      @default = File.basename(boot_script)
    end
  end
  
  def required?
    (Configurator.instance.is_enterprise?())
  end
end

class DatasourceVersion < ConfigurePrompt
  include DatasourcePrompt
  include HiddenValueModule
  
  def initialize
    super(REPL_DBVERSION, "DB version for the replication database", PV_ANY)
  end
  
  def load_default_value
    if (ds = get_datasource())
      @default = ds.getVersion()
    else
      @default = nil
    end
  end
end

class DatasourceMasterLogDirectory < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_MASTER_LOGDIR, "Master log directory", 
      PV_FILENAME)
  end
  
  def load_default_value
    @default = get_datasource().get_default_master_log_directory()
  end
  
  def required?
    (get_default_value() != nil)
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_mysql_binlog_dir'))
    super()
  end
end

class DatasourceMasterLogPattern < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_MASTER_LOGPATTERN, "Master log filename pattern", PV_ANY, "mysql-bin")
  end
  
  def load_default_value
    @default = get_datasource().get_default_master_log_pattern()
  end

  def required?
    (get_default_value() != nil)
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_mysql_binlog_pattern'))
    super()
  end
end

class DatasourceDisableRelayLogs < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_DISABLE_RELAY_LOGS, "Disable the use of relay-logs?",
      PV_BOOLEAN)
  end
  
  def load_default_value
    topology = get_topology()
    
    begin
      @default = topology.disable_relay_logs?()
    rescue
      @default = "true"
    end
  end
  
  def get_template_value(transform_values_method)
    v = super(transform_values_method)
    
    if v == "false"
      "true"
    else
      "false"
    end
  end
end

class DirectDatasourceDBType < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    validator = PropertyValidator.new(ConfigureDatabasePlatform.get_types().join("|"), 
      "Value must be #{ConfigureDatabasePlatform.get_types().join(',')}")
      
    super(EXTRACTOR_REPL_DBTYPE, "Database type (#{ConfigureDatabasePlatform.get_types().join(',')})", 
        validator)
  end
  
  def get_default_value
    case Configurator.instance.whoami()
    when "postgres"
      return "postgresql"
    when "enterprisedb"
      return "postgresql"
    else
      return "mysql"
    end
  end
end

class DirectDatasourceDBHost < ConfigurePrompt
  include DatasourcePrompt

  def initialize
    super(EXTRACTOR_REPL_DBHOST, "Database server hostname", PV_HOSTNAME)
  end

  def load_default_value
    if get_topology().is_a?(DirectTopology)
      @default = @config.getProperty(get_dataservice_key(DATASERVICE_MASTER_MEMBER))
    else
      @default = @config.getProperty(get_member_key(REPL_DBHOST))
    end
  end

  def allow_group_default
    false
  end
end

class DirectDatasourceDBPort < ConfigurePrompt
  include DatasourcePrompt

  def initialize
    super(EXTRACTOR_REPL_DBPORT, "Database server port", PV_INTEGER)
    override_command_line_argument("direct-replication-port")
  end

  def load_default_value
    @default = @config.getProperty(get_member_key(REPL_DBPORT))
  end

  PortForConnectors.register(REPL_SERVICES, EXTRACTOR_REPL_DBPORT)
  PortForManagers.register(REPL_SERVICES, EXTRACTOR_REPL_DBPORT)
end

class DirectDatasourceDBUser < ConfigurePrompt
  include DatasourcePrompt

  def initialize
    super(EXTRACTOR_REPL_DBLOGIN, "Database login for Tungsten", 
      PV_IDENTIFIER, Configurator.instance.whoami())
    override_command_line_argument("direct-replication-user")
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(REPL_DBLOGIN))
  end
end

class DirectDatasourceDBPassword < ConfigurePrompt
  include DatasourcePrompt
  include PrivateArgumentModule

  def initialize
    super(EXTRACTOR_REPL_DBPASSWORD, "Database password", 
      PV_ANY, "")
    override_command_line_argument("direct-replication-password")
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(REPL_DBPASSWORD))
  end
end

class DirectDatasourceMasterLogDirectory < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(EXTRACTOR_REPL_MASTER_LOGDIR, "Master log directory", 
      PV_FILENAME)
  end
  
  def load_default_value
    @default = get_datasource().get_default_master_log_directory()
  end
  
  def required?
    (get_default_value() != nil)
  end
end

class DirectDatasourceMasterLogPattern < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(EXTRACTOR_REPL_MASTER_LOGPATTERN, "Master log filename pattern", PV_ANY)
  end
  
  def load_default_value
    @default = get_datasource().get_default_master_log_pattern()
  end

  def required?
    (get_default_value() != nil)
  end
end

class DirectDatasourceDisableRelayLogs < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(EXTRACTOR_REPL_DISABLE_RELAY_LOGS, "Disable the use of relay-logs?",
      PV_BOOLEAN)
  end
  
  def get_template_value(transform_values_method)
    @config.getTemplateValue(get_member_key(REPL_DISABLE_RELAY_LOGS), transform_values_method)
  end
end

class DatasourceTHLURL < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBTHLURL, "Datasource THL URL")
  end
  
  def get_template_value(transform_values_method)
    get_datasource().get_thl_uri()
  end
end

class DatasourceJDBCURL < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBJDBCURL, "Datasource JDBC URL")
  end
  
  def get_template_value(transform_values_method)
    get_datasource().getJdbcUrl()
  end
end

class DatasourceJDBCDriver < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBJDBCDRIVER, "Datasource JDBC Driver")
  end
  
  def get_template_value(transform_values_method)
    get_datasource().getJdbcDriver()
  end
end

class DatasourceVendor < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBJDBCVENDOR, "Datasource Vendor")
  end
  
  def get_template_value(transform_values_method)
    get_datasource().getVendor()
  end
end

class DatasourceJDBCScheme < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBJDBCSCHEME, "Datasource JDBC Scheme")
  end
  
  def get_template_value(transform_values_method)
    get_datasource().getJdbcScheme()
  end
end

class DatasourceBackupAgents < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBBACKUPAGENTS, "Datasource Backup Agents")
  end
  
  def get_template_value(transform_values_method)
    get_datasource().get_backup_agents().join(",")
  end
end

class DatasourceDefaultBackupAgent < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBDEFAULTBACKUPAGENT, "Datasource Default Backup Agent")
  end
  
  def get_template_value(transform_values_method)
    get_datasource().get_default_backup_agent().split(",").at(0).to_s()
  end
end