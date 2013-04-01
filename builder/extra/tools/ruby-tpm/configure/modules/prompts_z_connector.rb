CONNECTORS = "connectors"
HOST_ENABLE_CONNECTOR = "host_enable_connector"
CONN_LISTEN_INTERFACE = "connector_listen_interface"
CONN_LISTEN_ADDRESS = "connector_listen_address"
CONN_LISTEN_PORT = "connector_listen_port"
CONN_CLIENTLOGIN = "connector_user"
CONN_CLIENTPASSWORD = "connector_password"
CONN_CLIENTDEFAULTDB = "connector_default_schema"
CONN_DB_PROTOCOL = "connector_db_protocol"
CONN_DB_VERSION = "connector_db_version"
CONN_DELETE_USER_MAP = "connector_delete_user_map"
CONN_AUTORECONNECT = "connector_autoreconnect"
CONN_RWSPLITTING = "connector_rwsplitting"
CONN_SMARTSCALE = "connector_smartscale"
CONN_SMARTSCALE_SESSIONID = "connector_smartscale_sessionid"
CONN_DRIVEROPTIONS = "connector_driver_options"
CONN_SLAVE_STATUS_IS_RELATIVE = "connector_relative_slave_status"
CONN_PASSWORD_LINES = "connector_password_lines"
CONN_DIRECT_LINES = "connector_direct_lines"
CONN_RW_ADDRESSES = "connector_rw_addresses"
CONN_RO_ADDRESSES = "connector_ro_addresses"
ROUTER_JMX_PORT = "router_jmx_port"
ROUTER_GATEWAY_PORT = "router_gateway_port"
ROUTER_GATEWAY_RETURN_PORT = "router_gateway_return_port"
ROUTER_WAITFOR_DISCONNECT_TIMEOUT = "connector_disconnect_timeout"
ROUTER_KEEP_ALIVE_TIMEOUT = "connector_keepalive_timeout"
ROUTER_DELAY_BEFORE_OFFLINE = "connector_delay_before_offline"
CONN_JAVA_MEM_SIZE = "conn_java_mem_size"
CONN_JAVA_ENABLE_CONCURRENT_GC = "conn_java_enable_concurrent_gc"

class Connectors < GroupConfigurePrompt
  def initialize
    super(CONNECTORS, "Enter connector information for @value", 
      "connector", "connectors", "CONNECTOR")
      
    ConnectorPrompt.subclasses().each{
      |klass|
      self.add_prompt(klass.new())
    }
  end
end

module ConnectorPrompt
  include GroupConfigurePromptMember
  include HashPromptDefaultsModule
  include NoReplicatorRestart
  include NoManagerRestart
  include CommercialPrompt
  
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
  
  def get_dataservice()
    if self.is_a?(HashPromptMemberModule)
      get_member()
    else
      ds_aliases =@config.getPropertyOr(get_member_key(DEPLOYMENT_DATASERVICE), [])
      if ! ds_aliases.kind_of?(Array)
         ds_aliases=Array(ds_aliases);
      end  
      ds_aliases.at(0)
    end
  end
  
  def get_repl_service_key(dataservice, key)
    repl_service_alias = "#{dataservice}_#{@config.getProperty(get_member_key(DEPLOYMENT_HOST))}"
    unless @config.getPropertyOr([REPL_SERVICES], {}).keys().include?(repl_service_alias)
      return nil
    end
    
    [REPL_SERVICES, repl_service_alias, key]
  end
  
  def get_dataservice_key(key)
    return [DATASERVICES, get_dataservice(), key]
  end
  
  def get_hash_prompt_key
    return [DATASERVICE_CONNECTOR_OPTIONS, get_dataservice(), @name]
  end
end

class ConnectorDeploymentHost < ConfigurePrompt
  include ConnectorPrompt
  include ConstantValueModule
  include NoTemplateValuePrompt

  def initialize
    super(DEPLOYMENT_HOST, 
      "On what host would you like to deploy this connector?", 
      PV_IDENTIFIER)
    @weight = -1
  end
  
  def load_default_value
    @default = @config.getProperty(DEPLOYMENT_HOST)
  end
  
  def is_valid?
    super()
    
    unless @config.getProperty(HOSTS).has_key?(get_value())
      raise ConfigurePromptError.new(self, "Host #{get_value()} does not exist in the configuration file", get_value())
    end
  end
end

class ConnectorDataservice < ConfigurePrompt
  include ConnectorPrompt
  include ConstantValueModule
  include NoTemplateValuePrompt
  
  def initialize
    super(DEPLOYMENT_DATASERVICE, "The dataaservice(s) used by this connector",
      PV_ANY)
  end
  
  def get_default_value
    @config.getProperty(DEPLOYMENT_DATASERVICE)
  end
  
  def is_valid?
    super()
    
    get_value().to_a().each{
      |ds_alias|
      
      unless @config.getProperty(DATASERVICES).has_key?(ds_alias)
        raise ConfigurePromptError.new(self, "Data service #{ds_alias} does not exist in the configuration file", get_value().to_a().join(","))
      end
    }
  end
end

class ConnectorLogin < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(CONN_CLIENTLOGIN, "Database username for the connector", PV_ANY)
    override_command_line_argument("application-user")
  end
  
  def load_default_value
    repl_service_key = get_repl_service_key(get_dataservice(), REPL_DBLOGIN)
    
    if repl_service_key != nil
      @default = @config.getPropertyOr(repl_service_key, nil)
    else
      @default = nil
    end
    
    if @default.to_s == ""
      @default = @config.getNestedProperty([DATASERVICE_REPLICATION_OPTIONS, get_dataservice(), REPL_DBLOGIN])
    end
    
    if @default.to_s == ""
      @default = @config.getProperty([REPL_SERVICES, DEFAULTS, REPL_DBLOGIN])
    end
  end
end

class ConnectorPassword < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(CONN_CLIENTPASSWORD, "Database password for the connector", PV_ANY)
    override_command_line_argument("application-password")
  end
  
  def load_default_value
    repl_service_key = get_repl_service_key(get_dataservice(), REPL_DBPASSWORD)
    
    if repl_service_key != nil
      @default = @config.getPropertyOr(repl_service_key, nil)
    else
      @default = nil
    end
    
    if @default.to_s == ""
      @default = @config.getNestedProperty([DATASERVICE_REPLICATION_OPTIONS, get_dataservice(), REPL_DBPASSWORD])
    end

    if @default.to_s == ""
      @default = @config.getProperty([REPL_SERVICES, DEFAULTS, REPL_DBPASSWORD])
    end
  end
end

class ConnectorListenInterface < ConfigurePrompt
  include ConnectorPrompt
  include AdvancedPromptModule
  
  def initialize
    super(CONN_LISTEN_INTERFACE, "Listen interface to use for the connector", 
      PV_ANY)
  end
  
  def required?
    false
  end
end

class ConnectorListenAddress < ConfigurePrompt
  include ConnectorPrompt
  include ConstantValueModule
  include NoSystemDefault
  
  def initialize
    super(CONN_LISTEN_ADDRESS, "Listen address to use for the connector", 
      PV_ANY)
  end
  
  def load_default_value
    if (iface = @config.getProperty(get_member_key(CONN_LISTEN_INTERFACE))) != nil
      @default = Configurator.instance.get_interface_address(iface)
    else
      @default = "0.0.0.0"
    end
  end
end

class ConnectorListenPort < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(CONN_LISTEN_PORT, "Port for the connector to listen on", PV_INTEGER, "9999")
    override_command_line_argument("application-port")
  end
  
  PortForUsers.register(CONNECTORS, CONN_LISTEN_PORT)
end

class ConnectorDefaultSchema < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(CONN_CLIENTDEFAULTDB, "Default schema for the connector to use", PV_ANY)
  end
  
  def load_default_value
    @default = @config.getProperty(get_dataservice_key(DATASERVICE_SCHEMA))
  end
end

class ConnectorDBProtocol < ConfigurePrompt
  include ConnectorPrompt
  include ConstantValueModule
  
  def initialize
    super(CONN_DB_PROTOCOL, "DB protocol for the connector to use", PV_ANY)
  end
  
  def load_default_value
    ds_alias = get_dataservice()
    @config.getPropertyOr([REPL_SERVICES], {}).each_key{
      |rs_alias|
      
      if @config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_DATASERVICE]) == ds_alias
        dbtype = @config.getProperty([REPL_SERVICES, rs_alias, REPL_DBTYPE])
        case dbtype
        when DBMS_POSTGRESQL_WAL
          @default = "postgresql"
        else
          @default = dbtype
        end
        return
      end
    }
    
    @default = @config.getProperty([REPL_SERVICES, DEFAULTS, REPL_DBTYPE]).to_s()
  end
end

class ConnectorDBVersion < ConfigurePrompt
  include ConnectorPrompt
  include HiddenValueModule
  
  def initialize
    super(CONN_DB_VERSION, "DB version for the connector to display", PV_ANY)
  end
  
  def load_default_value
    ds_alias = get_dataservice()
    @config.getPropertyOr([REPL_SERVICES], {}).each_key{
      |rs_alias|
      if rs_alias == DEFAULTS
        next
      end
      
      if @config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_DATASERVICE]) == ds_alias
        @default = @config.getProperty([REPL_SERVICES, rs_alias, REPL_DBVERSION]).to_s() + "-tungsten"
        return
      end
    }
    
    @default = @config.getProperty([REPL_SERVICES, DEFAULTS, REPL_DBVERSION]).to_s() + "-tungsten"
  end
end

class ConnectorOverwriteUserMap < ConfigurePrompt
  include ConnectorPrompt
  include AdvancedPromptModule
  include NoStoredConfigValue
  
  def initialize
    super(CONN_DELETE_USER_MAP, "Overwrite an existing user.map file", PV_BOOLEAN, "false")
  end
  
  def get_command_line_argument_value
    "true"
  end
end

class ConnectorUserMapPasswordLines < ConfigurePrompt
  include ConnectorPrompt
  include ConstantValueModule
  
  def initialize
    super(CONN_PASSWORD_LINES, "Connector user.map password lines", PV_ANY, "")
  end
  
  def get_template_value(transform_values_method)
    ds_alias = get_dataservice()
    composite_ds_alias = nil
    @config.getPropertyOr(DATASERVICES, {}).keys().each{
      |ds|
      
      unless include_dataservice?(ds)
        next
      end
      
      unless @config.getProperty([DATASERVICES, ds, DATASERVICE_IS_COMPOSITE]) == "true"
        next
      end
      
      if @config.getProperty([DATASERVICES, ds, DATASERVICE_COMPOSITE_DATASOURCES]).to_s().split(",").include?(ds_alias)
        composite_ds_alias = ds
      end
    }
    
    lines = []
    
    if composite_ds_alias
      lines << "#{@config.getProperty(CONN_CLIENTLOGIN)} #{@config.getPropertyOr(CONN_CLIENTPASSWORD, "-")} #{composite_ds_alias} #{ds_alias}"
    else
      lines << "#{@config.getProperty(CONN_CLIENTLOGIN)} #{@config.getPropertyOr(CONN_CLIENTPASSWORD, "-")} #{ds_alias}"
    end
    
    return lines.join("\n")
  end
end

class ConnectorUserMapDirectLines < ConfigurePrompt
  include ConnectorPrompt
  include ConstantValueModule
  
  def initialize
    super(CONN_DIRECT_LINES, "Connector user.map @direct lines", PV_ANY, "")
  end
  
  def get_template_value(transform_values_method)
    if @config.getProperty(get_member_key(CONN_RWSPLITTING)) == "true"
      prefix = ""
    else
      prefix = "#"
    end
    
    lines = []
    
    lines << "#{prefix}@direct #{@config.getProperty(CONN_CLIENTLOGIN)}"
    
    return lines.join("\n")
  end
end

class ConnectorRWAddresses < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(CONN_RW_ADDRESSES, "Connector addresses that should receive a r/w connection", PV_ANY, "")
  end
  
  def get_template_value(transform_values_method)
    lines = []
    
    get_value().to_s().split(",").each{
      |rw_address|
      if rw_address == ""
        next
      end
      
      lines << "@hostoption #{rw_address} qos=RW_STRICT"
    }
    
    return lines.join("\n")
  end
end

class ConnectorROAddresses < ConfigurePrompt
  include ConnectorPrompt

  def initialize
    super(CONN_RO_ADDRESSES, "Connector addresses that should receive a r/o connection", PV_ANY, "")
  end
  
  def get_template_value(transform_values_method)
    lines = []
    
    get_value().to_s().split(",").each{
      |ro_address|
      if ro_address == ""
        next
      end
      
      lines << "@hostoption #{ro_address} qos=RO_RELAXED"
    }
    
    return lines.join("\n")
  end
end

class ConnectorRWSplitting < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(CONN_RWSPLITTING, "Enable DirectReads R/W splitting in the connector", PV_BOOLEAN, "false")
  end
end

class ConnectorSmartScale < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(CONN_SMARTSCALE, "Enable SmartScale R/W splitting in the connector", PV_BOOLEAN, "false")
  end
end

class ConnectorSmartScaleSession < ConfigurePrompt
  include ConnectorPrompt

  def initialize
    super(CONN_SMARTSCALE_SESSIONID, "The default session ID to use with smart scale", PropertyValidator.new("DATABASE|USER|CONNECTION|PROVIDED_IN_DBNAME", 
        "Value must be either of DATABASE, USER, CONNECTION, PROVIDED_IN_DBNAME"), "DATABASE")
  end
  
  def accept?(raw_value)
    if (@config.getProperty(get_member_key(CONN_SMARTSCALE)) == "true")
      @validator = PropertyValidator.new("DATABASE|USER|CONNECTION|PROVIDED_IN_DBNAME", 
        "Value must be either of DATABASE, USER, CONNECTION, PROVIDED_IN_DBNAME")
    else
      @validator = PropertyValidator.new("DATABASE|USER|CONNECTION|PROVIDED_IN_DBNAME|", 
        "Value must be either of DATABASE, USER, CONNECTION, PROVIDED_IN_DBNAME")
    end
    
    return super(raw_value)
  end
end

class ConnectorAutoReconnect < ConfigurePrompt
  include ConnectorPrompt
  
  def initialize
    super(CONN_AUTORECONNECT, "Enable auto-reconnect in the connector", PV_BOOLEAN, "true")
  end
end

class ConnectorRelativeSlaveStatus < ConfigurePrompt
  include ConnectorPrompt
  include HiddenValueModule
  
  def initialize
    super(CONN_SLAVE_STATUS_IS_RELATIVE, "Display slave status using relative time", PV_BOOLEAN, "false")
  end
end

class ConnectorDisconnectTimeout < ConfigurePrompt
  include ConnectorPrompt
  include HiddenValueModule
  
  def initialize
    super(ROUTER_WAITFOR_DISCONNECT_TIMEOUT, "Time to wait for active connection to disconnect before forcing them closed", PV_INTEGER, "5")
  end
end

class ConnectorKeepaliveTimeout < ConfigurePrompt
  include ConnectorPrompt
  include HiddenValueModule
  
  def initialize
    super(ROUTER_KEEP_ALIVE_TIMEOUT, "Time to wait for a manager to respond to a keep-alive request", PV_INTEGER)
  end
  
  def load_default_value
    host = @config.getProperty(get_member_key(DEPLOYMENT_HOST))
    
    @default = 30000
    @config.getPropertyOr(MANAGERS, {}).keys().each{
      |m_alias|
      if (@config.getProperty([MANAGERS, m_alias, DEPLOYMENT_HOST]) == host)
        mgr_interval = @config.getPropertyOr([MANAGERS, m_alias, MGR_MONITOR_INTERVAL], 0).to_i
        @default = [@default, 3*mgr_interval].max()
      end
    }
  end
end

class ConnectorDelayBeforeOffline < ConfigurePrompt
  include ConnectorPrompt
  include HiddenValueModule
  
  def initialize
    super(ROUTER_DELAY_BEFORE_OFFLINE, "Time to wait to take a router offline after losing the connection to a manager", PV_INTEGER, "600")
  end
end

class RouterGatewayPort < ConfigurePrompt
  include ConnectorPrompt
  include AdvancedPromptModule
  
  def initialize
    super(ROUTER_GATEWAY_PORT, "The router gateway port", PV_INTEGER, "11999")
  end
  
  PortForManagers.register(CONNECTORS, ROUTER_GATEWAY_PORT, ROUTER_GATEWAY_RETURN_PORT)
  PortForConnectors.register(CONNECTORS, ROUTER_GATEWAY_PORT, ROUTER_GATEWAY_RETURN_PORT)
end

class RouterGatewayReturnPort < ConfigurePrompt
  include ConnectorPrompt
  include HiddenValueModule
  
  def initialize
    super(ROUTER_GATEWAY_RETURN_PORT, "The router gateway return port", PV_INTEGER)
  end
  
  def load_default_value
    @default = (@config.getProperty(get_member_key(ROUTER_GATEWAY_PORT)).to_i() + 1).to_s
  end
end

class RouterJMXPort < ConfigurePrompt
  include ConnectorPrompt
  include AdvancedPromptModule
  
  def initialize
    super(ROUTER_JMX_PORT, "The router jmx port", PV_INTEGER, "10999")
  end
  
  PortForManagers.register(CONNECTORS, ROUTER_JMX_PORT)
end

class ConnectorJavaMemorySize < ConfigurePrompt
  include ConnectorPrompt
  include AdvancedPromptModule
  
  def initialize
    super(CONN_JAVA_MEM_SIZE, "Connector Java heap memory size in Mb (min 128)",
      PV_JAVA_MEM_SIZE, 256)
  end
end

class ConnectorJavaGarbageCollection < ConfigurePrompt
  include ConnectorPrompt
  include AdvancedPromptModule
  
  def initialize
    super(CONN_JAVA_ENABLE_CONCURRENT_GC, "Connector Java uses concurrent garbage collection",
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