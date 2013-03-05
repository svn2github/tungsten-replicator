DBMS_ORACLE = "oracle"

# Oracle-specific parameters.
REPL_ORACLE_SERVICE = "repl_datasource_oracle_service"
REPL_ORACLE_DSPORT = "repl_oracle_dslisten_port"
REPL_ORACLE_HOME = "repl_oracle_home"
REPL_ORACLE_LICENSE = "repl_oracle_license"
REPL_ORACLE_SCHEMA = "repl_oracle_schema"
REPL_ORACLE_LICENSED_SLAVE = "repl_oracle_licensed_slave"
REPL_ORACLE_SCAN = "repl_datasource_oracle_scan"

class OracleDatabasePlatform < ConfigureDatabasePlatform
  def get_uri_scheme
    DBMS_ORACLE
  end
  
  def get_default_backup_method
    "none"
  end
  
  def get_valid_backup_methods
    "none|script"
  end
  
  def get_applier_template
    if @config.getPropertyOr([DATASOURCES, @ds_alias, REPL_ORACLE_SCAN], "") == ""
      "tungsten-replicator/samples/conf/appliers/#{get_uri_scheme()}.tpl"
    else
      "tungsten-replicator/samples/conf/appliers/oracle-scan.tpl"
    end
	end
  
  def get_thl_uri
    if @config.getPropertyOr([DATASOURCES, @ds_alias, REPL_ORACLE_SCAN], "") == ""
	    "jdbc:oracle:thin:@${replicator.global.db.host}:${replicator.global.db.port}:${replicator.applier.oracle.service}"
    else
  	  "jdbc:oracle:thin:@//${replicator.applier.oracle.scan}:${replicator.global.db.port}:${replicator.applier.oracle.service}"
	  end
	end
  
  def get_default_port
    "1521"
  end
  
  def get_default_start_script
    nil
  end
  
  def getBasicJdbcUrl()
    if @config.getPropertyOr([DATASOURCES, @ds_alias, REPL_ORACLE_SCAN], "") == ""
      "jdbc:oracle:thin:@${replicator.global.db.host}:${replicator.global.db.port}"
    else
  	  "jdbc:oracle:thin:@//${replicator.applier.oracle.scan}:${replicator.global.db.port}"
    end
  end
  
  def getJdbcUrl()
    if @config.getPropertyOr([DATASOURCES, @ds_alias, REPL_ORACLE_SCAN], "") == ""
	    "jdbc:oracle:thin:@${replicator.global.db.host}:${replicator.global.db.port}:${replicator.applier.oracle.service}"
    else
  	  "jdbc:oracle:thin:@//${replicator.applier.oracle.scan}:${replicator.global.db.port}:${replicator.applier.oracle.service}"
	  end
  end
  
  def getJdbcDriver()
    "oracle.jdbc.driver.OracleDriver"
  end
  
  def getVendor()
    "oracle"
  end

  def get_extractor_template
    raise "Unable to use OracleDatabasePlatform as an extractor"
  end
  
	def get_applier_filters()
	  ["nocreatedbifnotexists","dbupper"] + super()
	end
	
	def get_default_master_log_directory
    nil
  end
  
  def get_default_master_log_pattern
    nil
  end
  
  def get_replication_schema
    "${replicator.global.db.user}"
  end
end

#
# Prompts
#

class OracleConfigurePrompt < ConfigurePrompt
  def get_default_value
    begin
      if Configurator.instance.display_help? && !Configurator.instance.display_preview?
        raise ""
      end
      
      get_oracle_default_value()
    rescue => e
      super()
    end
  end
  
  def get_oracle_default_value
    raise "Undefined function"
  end
  
  # Execute mysql command and return result to client. 
  def oracle(command, hostname = nil)
    user = @config.getProperty(REPL_DBLOGIN)
    password = @config.getProperty(REPL_DBPASSWORD)
    port = @config.getProperty(REPL_DBPORT)
    if hostname == nil
      hosts = @config.getProperty(HOSTS).split(",")
      hostname = hosts[0]
    end

    raise "Update this to build the proper command"
  end
  
  def enabled?
    super() && (get_datasource().is_a?(OracleDatabasePlatform))
  end
  
  def enabled_for_config?
    super() && (get_datasource().is_a?(OracleDatabasePlatform))
  end
end

class OracleService < OracleConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_ORACLE_SERVICE, "Oracle Service", 
      PV_IDENTIFIER)
  end
end

class OracleSCAN < OracleConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_ORACLE_SCAN, "Oracle SCAN", PV_IDENTIFIER)
  end
  
  def required?
    false
  end
end

#
# Validation
#

class OracleValidationCheck < ConfigureValidationCheck
  def get_variable(name)
    oracle("show #{name}").chomp.strip;
  end
  
  def enabled?
    super() && @config.getProperty(REPL_DBTYPE) == DBMS_ORACLE
  end
end