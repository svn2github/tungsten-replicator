DBMS_MYSQL = "mysql"

# MySQL-specific parameters
GLOBAL_REPL_MYSQL_CONNECTOR_PATH = "global_mysql_connectorj_path"
REPL_MYSQL_CONNECTOR_PATH = "mysql_connectorj_path"
REPL_MYSQL_DATADIR = "repl_datasource_mysql_data_directory"
REPL_MYSQL_IBDATADIR = "repl_datasource_mysql_ibdata_directory"
REPL_MYSQL_IBLOGDIR = "repl_datasource_mysql_iblog_directory"
REPL_MYSQL_RO_SLAVE = "repl_mysql_ro_slave"
REPL_MYSQL_SERVER_ID = "repl_mysql_server_id"
REPL_MYSQL_ENABLE_ENUMTOSTRING = "repl_mysql_enable_enumtostring"
REPL_MYSQL_XTRABACKUP_DIR = "repl_mysql_xtrabackup_dir"
REPL_MYSQL_XTRABACKUP_FILE = "repl_mysql_xtrabackup_file"
REPL_MYSQL_XTRABACKUP_TMP_DIR = "repl_mysql_xtrabackup_tmp_dir"
REPL_MYSQL_XTRABACKUP_TMP_FILE = "repl_mysql_xtrabackup_tmp_file"
REPL_MYSQL_XTRABACKUP_RESTORE_TO_DATADIR = "repl_xtrabackup_restore_to_datadir"
REPL_MYSQL_USE_BYTES_FOR_STRING = "repl_mysql_use_bytes_for_string"
REPL_MYSQL_CONF = "repl_datasource_mysql_conf"
REPL_MYSQL_COMMAND = "repl_datasource_mysql_command"
REPL_MYSQL_SERVICE_CONF = "repl_datasource_mysql_service_conf"

class MySQLDatabasePlatform < ConfigureDatabasePlatform
  def get_uri_scheme
    DBMS_MYSQL
  end
  
  def get_default_backup_method
    path_to_xtrabackup = cmd_result("which innobackupex-1.5.1", true)
	  
	  if path_to_xtrabackup.to_s() != "" && @config.getProperty(ROOT_PREFIX) != "false"
	    "xtrabackup-full"
	  else
	    "mysqldump"
	  end
  end
  
  def get_valid_backup_methods
    "none|mysqldump|xtrabackup|xtrabackup-incremental|script"
  end
  
  # Execute mysql command and return result to client. 
  def run(command)
    if Configurator.instance.get_ip_addresses(@host) == false
      return ""
    end
    
    begin
      if Configurator.instance.is_localhost?(@host)
        mysql_cmd = @config.getProperty(@prefix + [REPL_MYSQL_COMMAND])
        unless File.executable?(mysql_cmd)
          mysql_cmd = "mysql"
        end
      else
        mysql_cmd = "mysql"
      end
      
      # Provisional workaround for MySQL 5.6 non-removable warning (Issue#445)
      tmp = Tempfile.new('options')
      tmp << "[client]\n"
      tmp << "user=#{@username}\n"
      tmp << "password=#{@password}\n"
      tmp << "port=#{@port}\n"
      tmp.flush
      
      Timeout.timeout(5) {
        return cmd_result("#{mysql_cmd} --defaults-file=#{tmp.path} -h#{@host} -e \"#{command}\"")
      }
    rescue Timeout::Error
    rescue RemoteCommandNotAllowed
    rescue => e
    end
    
    return ""
  end
  
  def run_remote(command,remote_host)
    
    begin
      if Configurator.instance.is_localhost?(@host)
        mysql_cmd = @config.getProperty(@prefix + [REPL_MYSQL_COMMAND])
        unless File.executable?(mysql_cmd)
          mysql_cmd = "mysql"
        end
      else
        mysql_cmd = "mysql"
      end
      
      # Provisional workaround for MySQL 5.6 non-removable warning (Issue#445)
      tmp = Tempfile.new('options')
      tmp << "[client]\n"
      tmp << "user=#{@username}\n"
      tmp << "password=#{@password}\n"
      tmp << "port=#{@port}\n"
      tmp.flush
      
      Timeout.timeout(5) {
        return cmd_result("#{mysql_cmd} --defaults-file=#{tmp.path} -h#{remote_host} -e \"#{command}\"")
      }
    rescue Timeout::Error
    rescue RemoteCommandNotAllowed
    rescue => e
    end
    
    return ""
  end
  
  def get_value(command, column = nil)
    response = run(command + "\\\\G")
    
    response.split("\n").each{ | response_line |
      parts = response_line.chomp.split(":")
      if (parts.length != 2)
        next
      end
      parts[0] = parts[0].strip;
      parts[1] = parts[1].strip;
      
      if parts[0] == column || column == nil
        return parts[1]
      end
    }
    
    return nil
  end
  
   def get_value_a(command, column = nil)
   
    cols = Array.new
    response = run(command + "\\\\G")
    
    response.split("\n").each{ | response_line |
      parts = response_line.chomp.split(":")
      if (parts.length != 2)
        next
      end
      parts[0] = parts[0].strip;
      parts[1] = parts[1].strip;
      
      if parts[0] == column || column == nil
        cols.push parts[1]
      end
    }
    
    return cols
  end
  
  def get_thl_uri
	  "jdbc:mysql:thin://${replicator.global.db.host}:${replicator.global.db.port}/${replicator.schema}?createDB=true"
	end
  
  def check_thl_schema(thl_schema)
    schemas = run("SHOW SCHEMAS LIKE '#{thl_schema}'")
    if schemas != ""
      raise "THL schema #{thl_schema} already exists at #{get_connection_summary()}"
    end
  end
  
  def get_default_master_log_directory
    h_alias = to_identifier(@host)
    if @config.getProperty([HOSTS, h_alias]) != nil
      begin
        logbin = ssh_result("my_print_defaults --config-file=#{@config.getProperty(@prefix + [REPL_MYSQL_CONF])} mysqld | grep '^--log[_-]bin'", @host, @config.getProperty([HOSTS, h_alias, USERID])).split("=")[-1].strip()

        if logbin.to_s() != ""
          logdir = File.dirname(logbin)
          unless logdir == "."
            return logdir
          end
        end
      rescue CommandError
      end

      begin
        datadir = ssh_result("my_print_defaults --config-file=#{@config.getProperty(@prefix + [REPL_MYSQL_CONF])} mysqld | grep '^--datadir'", @host, @config.getProperty([HOSTS, h_alias, USERID])).split("=")[-1].strip()

        if datadir.to_s() != ""
          return datadir
        end
      rescue CommandError
      end
    end

    datadir = get_value("SHOW VARIABLES LIKE 'datadir'", "Value")
    if datadir == nil
      return "/var/lib/mysql/"
    end

    return datadir
  end
  
  def get_default_master_log_pattern
    begin
      master_file = get_value("SHOW MASTER STATUS", "File")
      master_file_parts = master_file.split(".")
    
      if master_file_parts.length() > 1
        master_file_parts.pop()
        return master_file_parts.join(".")
      else
        raise IgnoreError
      end
    rescue
    end
    
    return "mysql-bin"
  end
  
  def get_default_port
    begin
      h_alias = to_identifier(@host)
      if @config.getProperty([HOSTS, h_alias]) != nil
        return ssh_result("my_print_defaults --config-file=#{@config.getProperty(@prefix + [REPL_MYSQL_CONF])} mysqld | grep '^--port'", @host, @config.getProperty([HOSTS, h_alias, USERID])).split("=")[-1].strip()
      end
    rescue CommandError
    end
    
    return "3306"
  end
  
  def get_default_start_script
     ["/etc/init.d/mysql", "/etc/init.d/mysqld"].each{|init|
      Timeout.timeout(30){
        begin
          exists = cmd_result("if [ -f #{init} ]; then echo 0; else echo 1; fi")
          if exists.to_i == 0
             return init
           
          end
        rescue CommandError
        end
      }
    }
   return "/etc/init.d/mysql" 
  end
  
  def getJdbcUrl()
    "jdbc:mysql://${replicator.global.db.host}:${replicator.global.db.port}/${DBNAME}?jdbcCompliantTruncation=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&allowMultiQueries=true&yearIsDateType=false"
  end
  
  def getJdbcDriver()
    "com.mysql.jdbc.Driver"
  end
  
  def getVendor()
    "mysql"
  end
  
  def getVersion()
    if (v = get_value("SHOW VARIABLES LIKE 'version'", "Value")) == nil
      "5.1"
    else
      v
    end
  end
	
	def get_thl_filters()
	  if @config.getProperty(REPL_MYSQL_ENABLE_ENUMTOSTRING) == "true"
	    ["enumtostring"]
	  else
	    []
	  end
	end

	def get_applier_filters()
	  ["mysqlsessions"] + super()
	end
	
	def get_backup_agents()
	  agent = @config.getProperty(REPL_BACKUP_METHOD)
	  path_to_xtrabackup = cmd_result("which innobackupex-1.5.1", true)
	  
	  if agent == "script"
	    agents = ["script"]
	  else
	    agents = []
	  end
	  
	  if path_to_xtrabackup.to_s() != "" && @config.getProperty(ROOT_PREFIX) != "false"
	    agents << "xtrabackup-full"
	    agents << "xtrabackup-incremental"
	    agents << "xtrabackup"
  	  agents << "mysqldump"
  	else  
  	  agents << "mysqldump"
  	  
  	  unless @config.getProperty(ROOT_PREFIX) != "false"
  	    agents << "xtrabackup-full"
	      agents << "xtrabackup-incremental"
	      agents << "xtrabackup"
	    end
	  end
	  
	  return agents
	end
	
	def get_default_backup_agent()
    path_to_xtrabackup = cmd_result("which innobackupex-1.5.1", true)
	  
	  if path_to_xtrabackup.to_s() != ""
	    "xtrabackup-full"
	  else
	    "mysqldump"
	  end
	end
	
	def create_tungsten_schema(schema_name = nil)
	  sql_file = "#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tools/ruby-tpm/configure/sql/tungsten_schema.sql"
	  if schema_name == nil
	    schema_name = @config.getProperty(@prefix + [REPL_SVC_SCHEMA])
	  end
	  
	  begin
	    check_thl_schema(schema_name)
	  rescue
	    raise "Unable to create the schema '#{schema_name}' because it already exists"
	  end
	  
	  run("SET SQL_LOG_BIN=0; CREATE SCHEMA #{schema_name}; USE #{schema_name}; source #{sql_file};")
  end
  
  def drop_tungsten_schema(schema_name)
    self.run("drop schema if exists #{schema_name}")
  end
end

#
# Prompts
#

class MySQLConfigurePrompt < ConfigurePrompt
  def load_default_value
    begin
      @default = get_mysql_default_value()
    rescue => e
      super()
    end
  end
  
  def get_mysql_default_value
    raise "Undefined function"
  end
  
  def enabled?
    super() && (get_datasource().is_a?(MySQLDatabasePlatform))
  end
  
  def enabled_for_config?
    super() && (get_datasource().is_a?(MySQLDatabasePlatform))
  end
end

class MySQLDataDirectory < MySQLConfigurePrompt
  include DatasourcePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_MYSQL_DATADIR, "MySQL data directory", 
      PV_FILENAME, "/var/lib/mysql/")
  end
  
  def get_mysql_default_value
    begin
      datadir = ssh_result("my_print_defaults --config-file=#{@config.getProperty(get_member_key(REPL_MYSQL_CONF))} mysqld | grep '^--datadir'", @config.getProperty(get_host_key(HOST)), @config.getProperty(get_host_key(USERID))).split("=")[-1].strip()

      if datadir.to_s() != ""
        return datadir
      end
    rescue CommandError
    end
    
    datadir = get_datasource().get_value("SHOW VARIABLES LIKE 'datadir'", "Value")
    if datadir == nil
      raise "Unable to determine datadir"
    end
    
    return datadir
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_mysql_data_dir'))
    super()
  end
end

class MySQLInnoDBDataDirectory < MySQLConfigurePrompt
  include DatasourcePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_MYSQL_IBDATADIR, "MySQL InnoDB data directory", 
      PV_FILENAME_OR_EMPTY)
  end
  
  def get_mysql_default_value
    begin
      val = get_datasource().get_value("SHOW VARIABLES LIKE 'innodb_data_home_dir'", "Value")
      unless val[0,1] == "/"
        return @config.getProperty(get_member_key(REPL_MYSQL_DATADIR)) + "/" + val
      else
        return val
      end
    rescue CommandError
    end
    
    return ""
  end
  
  def required?
    false
  end
end

class MySQLInnoDBLogDirectory < MySQLConfigurePrompt
  include DatasourcePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_MYSQL_IBLOGDIR, "MySQL InnoDB log directory", 
      PV_FILENAME_OR_EMPTY)
  end
  
  def get_mysql_default_value
    begin
      val = get_datasource().get_value("SHOW VARIABLES LIKE 'innodb_log_group_home_dir'", "Value")
      unless val[0,1] == "/"
        return @config.getProperty(get_member_key(REPL_MYSQL_DATADIR)) + "/" + val
      else
        return val
      end
    rescue CommandError
    end
    
    return ""
  end
  
  def enabled?
    super() && (get_datasource().is_a?(MySQLDatabasePlatform))
  end
  
  def enabled_for_config?
    super() && (get_datasource().is_a?(MySQLDatabasePlatform))
  end
  
  def required?
    false
  end
end

class MySQLCommand < ConfigurePrompt
  include DatasourcePrompt
  include HiddenValueModule
  
  def initialize
    super(REPL_MYSQL_COMMAND, "MySQL command", 
      PV_FILENAME)
  end
  
  def load_default_value
    begin
      path = ssh_result("which mysql 2>/dev/null", @config.getProperty(get_host_key(HOST)), @config.getProperty(get_host_key(USERID)))
    rescue CommandError
      path = ""
    end
    
    if path == ""
      path = find_command(["/bin/mysql", "/usr/bin/mysql", "/usr/local/bin/mysql"], @config.getProperty(get_host_key(HOST)), @config.getProperty(get_host_key(USERID))).to_s
    end
    
    if path == ""
      path = "/usr/bin/mysql"
    end
    
    @default = path
  end
  
  def enabled?
    super() && (get_datasource().is_a?(MySQLDatabasePlatform))
  end
  
  def enabled_for_config?
    super() && (get_datasource().is_a?(MySQLDatabasePlatform))
  end
end

class MySQLConfFile < ConfigurePrompt
  include DatasourcePrompt
  include FindFilesystemDefaultModule
  
  def initialize
    super(REPL_MYSQL_CONF, "MySQL config file", 
      PV_FILENAME)
  end
  
  def get_search_paths
    ["/etc/my.cnf", "/etc/mysql/my.cnf"]
  end
  
  def enabled?
    super() && (get_datasource().is_a?(MySQLDatabasePlatform))
  end
  
  def enabled_for_config?
    super() && (get_datasource().is_a?(MySQLDatabasePlatform))
  end
end

class MySQLServiceConfigFile < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_MYSQL_SERVICE_CONF, "Path to my.cnf file customized for this service", PV_FILENAME)
  end
  
  def load_default_value
    @default = @config.getProperty(get_host_key(HOME_DIRECTORY)) + "/share/.my.#{@config.getProperty(get_member_key(DEPLOYMENT_SERVICE))}.cnf"
  end
end

class MySQLServerID < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_MYSQL_SERVER_ID, "MySQL server ID", 
      PV_INTEGER)
    self.extend(NotTungstenInstallerPrompt)
  end
  
  def load_default_value
    begin
      server_id = get_applier_datasource().get_value("SHOW VARIABLES LIKE 'server_id'", "Value")
      if server_id == nil
        raise "Unable to determine server_id"
      end

      @default = server_id
    rescue => e
      super()
    end
  end
  
  def enabled?
    super() && (get_extractor_datasource().is_a?(MySQLDatabasePlatform))
  end
  
  def enabled_for_config?
    super() && (get_extractor_datasource().is_a?(MySQLDatabasePlatform))
  end
  
  def required?
    false
  end
end

class MySQLReadOnlySlaves < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_MYSQL_RO_SLAVE, "Slaves are read-only?", 
      PV_BOOLEAN, "true")
  end
end

class MySQLEnableEnumToString < ConfigurePrompt
  include ReplicationServicePrompt
  include HiddenValueModule
  
  def initialize
    super(REPL_MYSQL_ENABLE_ENUMTOSTRING, "Expand ENUM values into their text values?", 
      PV_BOOLEAN, "false")
  end
  
  def load_default_value
    if get_extractor_datasource().class != get_applier_datasource().class
      @default = "true"
    end
    
    super()
  end
end

class MySQLUseBytesForStrings < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_MYSQL_USE_BYTES_FOR_STRING, "Transfer strings as their byte representation?", 
      PV_BOOLEAN, "true")
  end
  
  def load_default_value
    if get_extractor_datasource().class != get_applier_datasource().class
      @default = "false"
    end
    
    super()
  end
end

class MySQLConnectorJPath < ConfigurePrompt
  include ClusterHostPrompt
  include NoStoredServerConfigValue
  
  def initialize
    super(REPL_MYSQL_CONNECTOR_PATH, "Path to MySQL Connector/J", 
      PV_FILENAME)
  end
  
  def load_default_value
    if File.exists?("/opt/mysql/connectorJ/mysql-connector-java-5.1.17-bin.jar")
      @default = "/opt/mysql/connectorJ/mysql-connector-java-5.1.17-bin.jar"
    else
      @default = nil
    end
  end
  
  def required?
    false
  end
  
  def validate_value(value)
    super(value)
    if is_valid?()
      unless File.exists?(value) || File.exists?("#{@config.getProperty(HOME_DIRECTORY)}/share/mysql-connector-java.jar")
        error("The file #{value} does not exist")
      end
    end
    
    is_valid?()
  end
  
  def accept?(raw_value)
    value = super(raw_value)
    if value.to_s == ""
      return value
    end
    
    unless File.extname(value) == ".jar"
      raise "The file #{value} does not end in jar.  Make sure you give a path to the jar file and not the archive."
    end
    
    return value
  end
  
  def value_is_different?(old_cfg)
    old_value = old_cfg.getProperty(get_name())
    if File.basename(get_value()) != File.basename(old_value)
      true
    else
      false
    end
  end
end

class MySQLGlobalConnectorJPath < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  include NoStoredServerConfigValue
  
  def initialize
    super(GLOBAL_REPL_MYSQL_CONNECTOR_PATH, "Path to MySQL Connector/J", 
      PV_FILENAME)
  end
  
  def value_is_different?(old_cfg)
    old_value = old_cfg.getProperty(get_name())
    if File.basename(get_value()) != File.basename(old_value)
      true
    else
      false
    end
  end
end

class MySQLXtrabackupIncrementalDirectory < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_MYSQL_XTRABACKUP_DIR, "Directory to use for storing xtrabackup full & incremental backups", PV_FILENAME)
  end
  
  def load_default_value
    dir = @config.getProperty(get_member_key(REPL_BACKUP_STORAGE_DIR))
    if dir.to_s != ""
      @default = dir + "/xtrabackup"
    else
      @default = nil
    end
  end
end

class MySQLXtrabackupFile < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_MYSQL_XTRABACKUP_FILE, "File to use for xtrabackup packaging", PV_FILENAME)
  end
  
  def load_default_value
    @default = @config.getProperty(get_member_key(REPL_BACKUP_STORAGE_DIR)) + "/innobackup.tar"
  end
end

class MySQLXtrabackupTempDirectory < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_MYSQL_XTRABACKUP_TMP_DIR, "Directory to use for xtrabackup temp files", PV_FILENAME)
  end
  
  def load_default_value
    dir = @config.getProperty(get_member_key(REPL_BACKUP_STORAGE_DIR))
    if dir.to_s != ""
      @default = dir + "/innobackup"
    else
      @default = nil
    end
  end
end

class MySQLXtrabackupTempFile < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_MYSQL_XTRABACKUP_TMP_FILE, "File to use for xtrabackup packaging", PV_FILENAME)
  end
  
  def load_default_value
    dir = @config.getProperty(get_member_key(REPL_BACKUP_STORAGE_DIR))
    if dir.to_s != ""
      @default = dir + "/innobackup.tar"
    else
      @default = nil
    end
  end
end

class MySQLXtrabackupRestoreToDataDir < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_MYSQL_XTRABACKUP_RESTORE_TO_DATADIR, "Restore directly to the MySQL data directory by default", PV_BOOLEAN, "false")
  end
end

#
# Validation
#

module MySQLApplierCheck
  def enabled?
    super() && (get_applier_datasource().is_a?(MySQLDatabasePlatform))
  end
end

module MySQLExtractorCheck
  def enabled?
    super() && (get_extractor_datasource().is_a?(MySQLDatabasePlatform))
  end
end

module MySQLCheck
  def enabled?
    super() && (get_extractor_datasource().is_a?(MySQLDatabasePlatform) || get_applier_datasource().is_a?(MySQLDatabasePlatform))
  end
end

class MySQLAvailableCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLCheck
  
  def set_vars
    @title = "mysql in path"
    @fatal_on_error = true
  end
  
  def validate
    begin
      path = ssh_result("which #{@config.getProperty(get_applier_key(REPL_MYSQL_COMMAND))} 2>/dev/null", @config.getProperty(get_host_key(HOST)), @config.getProperty(get_host_key(USERID)))
    rescue CommandError
      path = ""
    end
    
    if path == ""
      error("Unable to find mysql in the path")
      help("Try adding the --datasource-mysql-command argument to your configuration")
    end
  end
end

class MySQLClientCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck

  def set_vars
    @title = "MySQL client check"
    @fatal_on_error = true
  end
  
  def validate
    debug("Checking for an accessible mysql binary")
    mysql = cmd_result("which #{@config.getProperty(get_applier_key(REPL_MYSQL_COMMAND))}")
    debug("MySQL client path: #{mysql}")
    
    if mysql == ""
      error("Unable to find the MySQL binary at #{@config.getProperty(get_applier_key(REPL_MYSQL_COMMAND))}")
      help("Try adding the --datasource-mysql-command argument to your configuration")
    else
      debug("Determine the version of MySQL")
      mysql_version = cmd_result("#{mysql} --version")
      info("MySQL client version: #{mysql_version}")
    end
  end
end

class MySQLLoginCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck

  def set_vars
    @title = "Replication credentials login check"
    @fatal_on_error = true
  end
  
  def validate
    login_output = get_applier_datasource.run("select 'ALIVE' as 'Return Value'")
    if login_output =~ /ALIVE/
      info("MySQL server and login is OK for #{get_applier_datasource.get_connection_summary()}")
    else
      error("Unable to connect to the MySQL server using #{get_applier_datasource.get_connection_summary()}")
      
      if get_applier_datasource().password.to_s() == ""
        help("Try specifying a password for #{get_applier_datasource.get_connection_summary()}")
      end
    end
  end
end

class MySQLPermissionsCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck

  
  def set_vars
    @title = "Replication user permissions check"
  end
  
  def validate
    has_missing_priv = false
    
    user = get_applier_datasource.get_value("select user()", "user()")
    grants = get_applier_datasource.get_value("show grants")
    
    info("Checking user permissions: #{grants}")
    unless grants =~ /ALL PRIVILEGES/
      has_missing_priv = true
    end
    
    unless grants =~ /WITH GRANT OPTION/
      has_missing_priv = true
    end

    if has_missing_priv
      error("The database user is missing some privileges or the grant option. Run 'mysql -u#{@config.getProperty(get_member_key(REPL_DBLOGIN))} -p#{@config.getProperty(get_member_key(REPL_DBPASSWORD))} -h#{@config.getProperty(get_member_key(REPL_DBHOST))} -e\"GRANT ALL ON *.* to #{user} WITH GRANT OPTION\"'")
    else
      info("All privileges configured correctly")
    end
    
    #Check the system user can connect remotely to all the other instances
    #The managers need to do this TUC-1146
    @config.getPropertyOr('dataservice_hosts', '').split(",").each do |remoteHost|
      login_output = get_applier_datasource.run_remote("select 'ALIVE' as 'Return Value'",remoteHost)
      if login_output =~ /ALIVE/
        info("Able to logon remotely to #{remoteHost} MySQL Instance")
      else
        error("Unable to connect to the MySQL server on #{remoteHost}")
        help("The management process needs to be able to connect to remote database servers to verify status")
      end
    end
  end
end

class MySQLInnoDBEnabledCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLCheck

  def set_vars
    @title = "InnoDB enabled check"
  end
  
  def validate
    enabled = get_applier_datasource.get_value("select IF(SUPPORT, 'DEFAULT', 'YES') as Value  from information_schema.engines where engine='InnoDB'", "Value")
    have_innodb = get_applier_datasource.get_value("SHOW VARIABLES LIKE 'have_innodb'", "Value")
    if enabled != "YES" && have_innodb != "YES"
      help("Check that the MySQL user has SELECT from information_schema privilege")
      help("Remove \"skip-innodb\" from the MySQL configuration file.")
      error("InnoDB is not enabled on #{get_applier_datasource.get_connection_summary}")
    end
  end
end

class MySQLBinaryLogsEnabledCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLExtractorCheck

  def set_vars
    @title = "Binary logs enabled check"
  end
  
  def validate
    log_bin = get_extractor_datasource.get_value("show variables like 'log_bin'", "Value")
    if log_bin != "ON"
      help("Check that the MySQL user can run \"show variables like 'log_bin'\"")
      help("Add \"log-bin=mysql-bin\" to the MySQL configuration file.")
      error("Binary logs are not enabled on #{get_extractor_datasource.get_connection_summary}")
    end
  end
end

class MySQLConfigFileCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  
  def set_vars
    @title = "MySQL config file is available"
    @fatal_on_error = true
  end
  
  def validate
    conf_file = @config.getProperty(get_applier_key(REPL_MYSQL_CONF))
    
    if Configurator.instance.is_localhost?(@config.getProperty(get_applier_key(REPL_DBHOST)))
      if conf_file.to_s() == ""
        error("Unable to find the MySQL config file")
        help("Specify the --datasource-mysql-conf argument with the path to your my.cnf")
      else
        unless File.exists?(conf_file)
          error("The MySQL config file '#{conf_file}' does not exist")
          help("Specify the --datasource-mysql-conf argument with the path to your my.cnf")
        else
          unless File.readable?(conf_file)
            error("The MySQL config file '#{conf_file}' is not readable")
            help("Specify the --datasource-mysql-conf argument with the path to your my.cnf")
          end
        end
      end
    else
      warning("Unable to check the MySQL config file '#{conf_file}'")
    end
  end
end

class MySQLApplierServerIDCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  
  def set_vars
    @title = "MySQL server id"
  end
  
  def validate
    server_id = @config.getProperty(get_member_key(REPL_MYSQL_SERVER_ID))
    if server_id.to_i <= 0
      error("The server-id '#{server_id}' for #{get_applier_datasource.get_connection_summary()} is too small")
    elsif server_id.to_i > 4294967296
      error("The server-id '#{server_id}' for #{get_applier_datasource.get_connection_summary()} is too large")
    end
    
    retrieved_server_id = get_applier_datasource.get_value("SHOW VARIABLES LIKE 'server_id'", "Value")
    if server_id.to_i != retrieved_server_id.to_i
      error("The server-id '#{server_id}' does not match the the server-id from #{get_applier_datasource.get_connection_summary()} '#{retrieved_server_id}'")
    end
    
    if Configurator.instance.is_localhost?(@config.getProperty(get_applier_key(REPL_DBHOST)))
      conf_file = @config.getProperty(get_applier_key(REPL_MYSQL_CONF))
      if File.exists?(conf_file) && File.readable?(conf_file)
        my_print_defaults = which('my_print_defaults')
        unless my_print_defaults
             debug("Unable to find my_print_defaults in the current path")
             return
        end
           
        begin
            conf_file_results = cmd_result("#{my_print_defaults} --config-file=#{conf_file} mysqld | grep '^--server[-_]id'").split("=")[-1].strip()
        rescue CommandError
        end
        if !conf_file_results
           error("The MySQL config file '#{conf_file}' does not include a value for server-id")
           help("Check the file to ensure a value is given and that it is not commented out")
        end
      else
        error("The MySQL config file '#{conf_file}' is not readable")
      end
    else
      warning("Unable to check for a configured server-id in '#{conf_file}' on #{get_applier_datasource.get_connection_summary}")
    end
  end
end

class MySQLApplierPortCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  
  def set_vars
    @title = "Compare the MySQL port to the one in my.cnf"
  end
  
  def validate
    port = @config.getProperty(get_applier_key(REPL_DBPORT))
    
    unless Configurator.instance.is_localhost?(@config.getProperty(get_applier_key(REPL_DBHOST)))
      warning("Unable to check for a configured port in '#{conf_file}' on #{get_applier_datasource.get_connection_summary}")
      return
    end
    
    conf_file = @config.getProperty(get_applier_key(REPL_MYSQL_CONF))
    unless File.exists?(conf_file) && File.readable?(conf_file)
      error("The MySQL config file '#{conf_file}' is not readable")
      help("Specify the --datasource-mysql-conf argument with the path to your my.cnf")
      return
    end
    
    my_print_defaults = which('my_print_defaults')
    unless my_print_defaults
      debug("Unable to find my_print_defaults in the current path")
      return
    end
    
    begin
      conf_file_results = cmd_result("#{my_print_defaults} --config-file=#{conf_file} mysqld | grep '^--port'").split("=")[-1].strip()
    rescue CommandError
    end
    
    if conf_file_results
      if conf_file_results != port
        error("You have configured the replication service to use a port that does not match the my.cnf file")
        help("Specify the --datasource-mysql-conf argument with the path to your my.cnf file or configure the replication service to use the correct port")
      end
    elsif port != "3306"
      error("You have configured the replication service to use a non-standard port but the my.cnf file does not define a port")
      help("Specify the --datasource-mysql-conf argument with the path to your my.cnf file or configure the replication service to use the correct port")
    end
  end
end

class MySQLReadableLogsCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLExtractorCheck

  def set_vars
    @title = "Readable binary logs check"
  end
  
  def validate
    master_file = get_extractor_datasource.get_value("show master status", "File")
    if master_file == nil
      help("Check that the MySQL user can run \"show master status\"")
      help("Add \"log-bin=mysql-bin\" to the MySQL configuration file.")
      raise "Unable to determine current binlog file."
    end
    
    info("Check readability of #{@config.getProperty(get_member_key(REPL_MASTER_LOGDIR))}/#{master_file}")
    unless File.readable?("#{@config.getProperty(get_member_key(REPL_MASTER_LOGDIR))}/#{master_file}")
      error("Unable to read current binlog file.  Check that this system user can read #{@config.getProperty(get_member_key(REPL_MASTER_LOGDIR))}/#{master_file}.")
    else
      info("The system user is able to read binary logs")
    end
  end
  
  def enabled?
    super() && 
      (get_extractor_datasource().host == 
        @config.getProperty([HOSTS, @config.getProperty(DEPLOYMENT_HOST), HOST])) && 
      (@config.getProperty(get_member_key(REPL_DISABLE_RELAY_LOGS)) == "true")
  end
end

class MySQLApplierLogsCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  
  def set_vars
    @title = "Compare the configured logs directory to the one used by MySQL"
  end
  
  def validate
    dir = @config.getProperty(get_applier_key(REPL_MASTER_LOGDIR))
    
    unless Configurator.instance.is_localhost?(@config.getProperty(get_applier_key(REPL_DBHOST)))
      warning("Unable to check the configured log directory in '#{conf_file}' on #{get_applier_datasource.get_connection_summary}")
      return
    end
    
    conf_file = @config.getProperty(get_applier_key(REPL_MYSQL_CONF))
    unless File.exists?(conf_file) && File.readable?(conf_file)
      error("The MySQL config file '#{conf_file}' is not readable")
      return
    end
    
    my_print_defaults = which('my_print_defaults')
    unless my_print_defaults
      debug("Unable to find my_print_defaults in the current path")
      return
    end
    
    begin
      conf_file_results = cmd_result("#{my_print_defaults} --config-file=#{conf_file} mysqld | grep '^--log[_-]bin'").split("=")[-1].strip()
    rescue CommandError
    end
    
    if conf_file_results =~ /\//
      conf_file_results = File.dirname(conf_file_results)
    else
      begin
        conf_file_results = cmd_result("#{my_print_defaults} --config-file=#{conf_file} mysqld | grep '^--datadir'").split("=")[-1].strip()
      rescue CommandError
      end
      
      conf_file_results = get_applier_datasource().get_value("SHOW VARIABLES LIKE 'datadir'", "Value")
    end
    
    if File.expand_path(conf_file_results) != File.expand_path(dir)
      error("You have configured the replication service to use a binary log directory that does not match the my.cnf file")
      help("Specify the --datasource-mysql-conf argument with the path to your my.cnf file or configure the replication service to use the correct binary log directory")
    end
  end
  
  def enabled?
    super() && 
      (get_applier_datasource().host == 
        @config.getProperty([HOSTS, @config.getProperty(DEPLOYMENT_HOST), HOST])) && 
      (@config.getProperty(get_member_key(REPL_DISABLE_RELAY_LOGS)) == "true")
  end
end

class MySQLSettingsCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck

  def set_vars
    @title = "MySQL settings check"
  end
  
  def validate
    info("Checking sync_binlog setting")
    sync_binlog = get_applier_datasource.get_value("show variables like 'sync_binlog'", "Value")
    if sync_binlog == nil || sync_binlog == "0"
      warning("\"sync_binlog\" is set to 0 in the MySQL configuration file for #{get_applier_datasource.get_connection_summary()} this setting can lead to possible data loss in a server failure")
    end
    
    info("Checking innodb_flush_log_at_trx_commit")
    innodb_flush_log_at_trx_commit = get_applier_datasource.get_value("show variables like 'innodb_flush_log_at_trx_commit'", "Value")
    if innodb_flush_log_at_trx_commit == nil || innodb_flush_log_at_trx_commit == "0" 
      warning(" \"innodb_flush_log_at_trx_commit\" is set to 0 in the MySQL configuration file for #{get_applier_datasource.get_connection_summary()} this setting can lead to possible data loss in a server failure")
    end
    
    info("Checking max_allowed_packet")
    max_allowed_packet = get_applier_datasource.get_value("show variables like 'max_allowed_packet'", "Value")
    if max_allowed_packet == nil || max_allowed_packet.to_i() < (48*1024*1024)
      warning("We suggest adding \"max_allowed_packet=52m\" or greater to the MySQL configuration file for #{get_applier_datasource.get_connection_summary()}")
    end
    
    info("Checking open_files_limit")
    open_files_limit = get_applier_datasource.get_value("show variables like 'open_files_limit'", "Value")
    if open_files_limit == nil || open_files_limit.to_i < 65535
      warning("We suggest adding \"open_files_limit=65535\" to the MySQL configuration file for #{get_applier_datasource.get_connection_summary()}")
      warning("Add '*       -    nofile  65535' to your /etc/security/limits.conf and restart MySQL to make sure the setting takes effect")
    end
    
    
    if @config.getProperty(DEPLOYMENT_COMMAND) =='InstallCommand'
      info("Checking the master is not read_only")
      if @config.getProperty(get_dataservice_key(DATASERVICE_MASTER_MEMBER)) == @config.getProperty(get_host_key(HOST))
         if @config.getProperty(get_dataservice_key(DATASERVICE_RELAY_ENABLED)) == "false"
            read_only = get_applier_datasource.get_value("show variables like 'read_only'", "Value")
            if read_only == 'ON'
               error("The master mysql instance is set to read_only")
            end
         end
      end
    end
       
  end
end

class MySQLNoMySQLReplicationCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  
  def set_vars
    @title = "No MySQL replication check"
  end
  
  def validate
    info("Checking that MySQL replication is not running on the slave datasource")
    slave_sql_running = get_applier_datasource.get_value("SHOW SLAVE STATUS", "Slave_SQL_Running")
    if (slave_sql_running != nil) and (slave_sql_running != "No")
      error("The slave datasource #{get_applier_datasource.get_connection_summary()} has a running slave SQL thread")
    end
  end
  
  def enabled?
    if @config.getProperty(get_member_key(REPL_SVC_NATIVE_SLAVE_TAKEOVER)) == "false"
      super()
    else
      false
    end
  end
end

class MySQLDefaultTableTypeCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  
  def set_vars
    @title = "MySQL default table type check"
  end
  
  def validate
    info("Checking that MySQL uses InnoDB exists")
    
    table_type = get_applier_datasource.get_value("SHOW VARIABLES LIKE 'table_type'", "Value")
    if table_type == nil
      table_type = get_applier_datasource.get_value("SHOW VARIABLES LIKE 'storage_engine'", "Value")
    end
    
    if table_type.downcase() == "myisam"
      error("The datasource #{get_applier_datasource.get_connection_summary()} uses #{table_type} as the default storage engine")
    end
  end
end

class MysqldumpAvailableCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  
  def set_vars
    @title = "Mysqldump method availability check"
  end
  
  def validate
    begin
      path = cmd_result("which mysqldump")
      info("mysqldump found at #{path}")
    rescue CommandError
      error("Unable to find mysqldump in your path")
    end
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_BACKUP_METHOD)) == "mysqldump"
  end
end

class MysqldumpSettingsCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  
  def set_vars
    @title = "Mysqldump method settings check"
  end
  
  def validate
    info("Check for log_slow_queries")
    log_slow_queries = get_applier_datasource.get_value("show variables like 'log_slow_queries'", "Value")
    if log_slow_queries == 'ON'
      warning("'log_slow_queries' is enabled for #{get_applier_datasource.get_connection_summary()} this can cause issues with mysqldump")
    end
    
    info("Check for slow_query_log")
    slow_query_log = get_applier_datasource.get_value("show variables like 'slow_query_log'", "Value")
    if slow_query_log == 'ON'
      warning("'slow_query_log' is enabled for #{get_applier_datasource.get_connection_summary()} this can cause issues with mysqldump")
    end
  end
  
  def enabled?
    # Disable this check until we can determine if there is actually an issue
    # between mysqldump and slow query logs
    false
    #super() && @config.getProperty(get_member_key(REPL_BACKUP_METHOD)) == "mysqldump"
  end
end

class XtrabackupAvailableCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  
  def set_vars
    @title = "Xtrabackup availability check"
  end
  
  def validate
    begin
      path = cmd_result("which innobackupex-1.5.1")
      info("xtrabackup found at #{path}")
    rescue
      error("Unable to find the innobackupex-1.5.1 script for backup")
    end
  end
  
  def enabled?
    super() && ["xtrabackup","xtrabackup-full","xtrabackup-incremental"].include?(@config.getProperty(get_member_key(REPL_BACKUP_METHOD)))
  end
end

class XtrabackupSettingsCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  
  def set_vars
    @title = "Xtrabackup settings"
  end
  
  def validate
    unless Configurator.instance.is_localhost?(@config.getProperty(get_applier_key(REPL_DBHOST)))
      error("Xtrabackup may only be used to backup a database server running on the same host as Tungsten Replicator")
      return
    end
    
    if @config.getProperty(get_host_key(ROOT_PREFIX)) != "true"
      error("You must enable sudo  to use xtrabackup")
      help("Add --root-command-prefix=true to your command")
    end
    
    info("Check for datadir")
    
    conf_file = @config.getProperty(get_applier_key(REPL_MYSQL_CONF))
    if File.exists?(conf_file) && File.readable?(conf_file)
      begin
        conf_file_results = cmd_result("grep ^datadir #{conf_file}").split("=")
      rescue
        error("The MySQL config file '#{conf_file}' does not include a value for datadir")
        help("Check the file to ensure a value is given and that it is not commented out")
      end
    else
      error("The MySQL config file '#{conf_file}' is not readable")
    end
    
    datadir = get_applier_datasource.get_value("show variables like 'datadir'", "Value")
    unless File.directory?(datadir)
      warning("The datadir setting #{datadir} is not readable for #{get_applier_datasource.get_connection_summary()}")
      warning("The #{@config.getProperty(get_host_key(USERID))} user may not have read access to the directory.  Ensure that it exists or specify a valid directory for datadir in your my.cnf file to ensure that all utilities work properly for #{get_applier_datasource.get_connection_summary()}")
    end
    
    unless File.expand_path(@config.getProperty(get_applier_key(REPL_MYSQL_DATADIR))) == File.expand_path(datadir)
      error("The MySQL datadir setting does not match the provided value: #{@config.getProperty(get_applier_key(REPL_MYSQL_DATADIR))}")
      help("Fix the datadir value in my.cnf or use '--datasource-mysql-data-directory=#{datadir}'")
    end
    
    info("Check for binary logs")
    binary_count = cmd_result("#{@config.getTemplateValue(get_member_key(ROOT_PREFIX))} ls #{@config.getProperty(get_applier_key(REPL_MASTER_LOGDIR))}/#{@config.getProperty(get_applier_key(REPL_MASTER_LOGPATTERN))}.* 2>/dev/null | wc -l")
    unless binary_count.to_i > 0
      error("#{@config.getProperty(get_applier_key(REPL_MASTER_LOGDIR))} does not contain any files starting with #{@config.getProperty(get_applier_key(REPL_MASTER_LOGPATTERN))}")
      help("Try providing a value for --datasource-log-directory or --datasource-log-pattern")
    end
  end
  
  def enabled?
    super() && ["xtrabackup", "xtrabackup-incremental"].include?(@config.getProperty(get_member_key(REPL_BACKUP_METHOD)))
  end
end

class XtrabackupDirectoryWriteableCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLCheck
  
  def set_vars
    @title = "Xtrabackup directory writeable check"
  end
  
  def validate
    dir = @config.getProperty(REPL_MYSQL_XTRABACKUP_DIR)
    unless File.writable?(dir)
      if File.exists?(dir)
        if File.directory?(dir)
          error("Unable to write to the Xtrabackup directory (#{dir})")
        else
          error("The Xtrabackup directory (#{dir}) is a file")
        end
      else
        begin
          FileUtils.mkdir_p(dir)
          FileUtils.rmdir(dir)
        rescue => e
          error("Unable to create the Xtrabackup directory (#{dir})")
          error(e.message)
        end
      end
    end
  end

  def enabled?
    super() && ["xtrabackup","xtrabackup-full","xtrabackup-incremental"].include?(@config.getProperty(get_member_key(REPL_BACKUP_METHOD)))
  end
end

class MysqlConnectorCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLCheck
  include ClusteringServiceCheck
  
  def set_vars
    @title = "MySQL Connector/J check"
  end
  
  def validate
    unless File.exists?(@config.getProperty(get_host_key(REPL_MYSQL_CONNECTOR_PATH)).to_s())
      connector_path = "#{@config.getProperty(HOME_DIRECTORY)}/share/mysql-connector-java.jar"
      unless File.exists?(connector_path)
        error("Unable to find a MySQL Connector/J file in the home directory or in the configuration")
        help("Download the MySQL Connector/J file to the installation server and add it to the configuration by running 'tools/tpm configure defaults --mysql-connectorj-path=/path/to/mysql-connector-java-5.1.17-bin.jar")
      end
    end
  end
end

module ConfigureDeploymentStepMySQL
  include DatabaseTypeDeploymentStep
  
  def get_methods
    [
      ConfigureDeploymentMethod.new("deploy_mysql_connectorj_package"),
    ]
  end
  module_function :get_methods
  
  def deploy_replication_dataservice()
    if ["xtrabackup", "xtrabackup-incremental"].include?(@config.getProperty(REPL_BACKUP_METHOD))
      mkdir_if_absent(@config.getProperty(REPL_MYSQL_XTRABACKUP_DIR))
    end
    
    ads = get_applier_datasource()
    if ads.is_a?(MySQLDatabasePlatform)
      File.open(@config.getProperty(get_service_key(REPL_MYSQL_SERVICE_CONF)), "w") {
        |file|
        if @config.getPropertyOr(get_service_key(REPL_MYSQL_CONF), "") != ""
          file.puts("!include #{@config.getProperty(get_service_key(REPL_MYSQL_CONF))}")
          file.puts("")
        end
        
        file.puts("[client]")
        file.puts("user=#{ads.username}")
        file.puts("password=#{ads.password}")
      }
      watch_file(@config.getProperty(get_service_key(REPL_MYSQL_SERVICE_CONF)))
    end
    
    if is_manager?() && (get_applier_datasource().is_a?(MySQLDatabasePlatform) || get_extractor_datasource().is_a?(MySQLDatabasePlatform))
      transformer = Transformer.new(
  		  "#{get_deployment_basedir()}/tungsten-manager/samples/conf/checker.mysqlserver.properties.tpl",
  			"#{get_deployment_basedir()}/tungsten-manager/conf/checker.mysqlserver.properties", "#")
  	  transformer.transform_values(method(:transform_replication_dataservice_values))
      transformer.output
      watch_file(transformer.get_filename())
      
      write_svc_properties("mysql", @config.getProperty(REPL_BOOT_SCRIPT))
      
      if @config.getProperty(REPL_MYSQL_RO_SLAVE) == "true"
        FileUtils.cp("#{get_deployment_basedir()}/tungsten-manager/rules-ext/mysql_readonly.service.properties", 
          "#{get_deployment_basedir()}/cluster-home/conf/cluster/#{@config.getProperty(DATASERVICENAME)}/service/mysql_readonly.properties")
    
        transformer = Transformer.new(
    		  "#{get_deployment_basedir()}/tungsten-manager/samples/conf/mysql_readonly.tpl",
    			"#{get_deployment_basedir()}/cluster-home/bin/mysql_readonly", "#")
    	  transformer.transform_values(method(:transform_replication_dataservice_values))
        transformer.output
        watch_file(transformer.get_filename())
        File.chmod(0755, "#{get_deployment_basedir()}/cluster-home/bin/mysql_readonly")
      else
        FileUtils.rm_f("#{get_deployment_basedir()}/cluster-home/conf/cluster/#{@config.getProperty(DATASERVICENAME)}/service/mysql_readonly.properties")
        FileUtils.rm_f("#{get_deployment_basedir()}/cluster-home/bin/mysql_readonly")
      end
    end
    
    super()
  end
  
  def deploy_mysql_connectorj_package
    connector_path = "#{@config.getProperty(HOME_DIRECTORY)}/share/mysql-connector-java.jar"
    connector = @config.getProperty(REPL_MYSQL_CONNECTOR_PATH)
    
    if connector.to_s != ""
  		if connector != nil and connector != "" and File.exist?(connector)
    		info "Deploying MySQL Connector/J..."
    		FileUtils.cp(connector, File.dirname(connector_path))
        if File.dirname(connector_path) + "/" + File.basename(connector) !=    connector_path
    		  FileUtils.ln_sf(File.dirname(connector_path) + "/" + File.basename(connector), connector_path)
        end
    	end
  	end
  	
  	if File.exist?(connector_path)
      FileUtils.ln_sf(connector_path, "#{get_deployment_basedir()}/tungsten-replicator/lib/")
      FileUtils.ln_sf(connector_path, "#{get_deployment_basedir()}/tungsten-connector/lib/")
      FileUtils.ln_sf(connector_path, "#{get_deployment_basedir()}/tungsten-manager/lib/")
      FileUtils.ln_sf(connector_path, "#{get_deployment_basedir()}/bristlecone/lib-ext/")
		end
	end
end

class MySQLConnectorPermissionsCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  include ClusteringServiceCheck

  def set_vars
    @title = "Connector Mysql user permissions check"
  end
  
  def validate
   
    connuser = @config.getProperty('connector_user')
    connpassword = @config.getProperty('connector_password')

    
    if get_applier_datasource.get_value("select user from mysql.user where user='#{connuser}'") == nil
      error("The user specified in --application-user (#{connuser}) does not exist")
      help("Ensure the user '#{connuser}' exists on all of the instances in the cluster being installed")
    else 
    
        hosts=get_applier_datasource.get_value_a("select host from mysql.user where user='#{connuser}'",'host')
        
        hosts.each do |host|
        
            if get_applier_datasource.get_value("select super_priv from mysql.user where user='#{connuser}' and host='#{host}'") == 'Y'
                error("The user specified in --application-user (#{connuser}@#{host}) has super privileges and can not be safely used as a application-user")
                help("The user #{connuser} has the SUPER privilege. This is not safe as it allows the application to write to READ_ONLY slave.Revoke this privilege using REVOKE SUPER on *.* from '#{connuser}'@'#{host}' ")
            end
            
            if get_applier_datasource.get_value("select 'OK' from mysql.user where user='#{connuser}' and host='#{host}' and  password=password('#{connpassword}')")  != 'OK'
                error("Password specifed for #{connuser}@#{host} does not match the running instance on #{get_applier_datasource.get_connection_summary()}")
            end
            
            if @config.getProperty('connector_smartscale') == 'true'
                if get_applier_datasource.get_value("select Repl_client_priv from mysql.user where user='#{connuser}' and host='#{host}'") == 'N'
                    error("The user specified in --application-user (#{connuser}@#{host}) does not have REPLICATION CLIENT privileges and SMARTSCALE in enabled")
                    help("When SmartScale is enabled, all application users require the REPLICATION CLIENT  privilege. Grant it to the user via GRANT REPLICATION CLIENT on *.* to '#{connuser}'@#{host}")
                end
            end
        end
    end
   end
end

class MySQLPasswordSettingCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck

  def set_vars
    @title = "MySQL Password settings check"
  end
  
  def validate
       info("Checking old_passwords")
       if get_applier_datasource.get_value("select min(length(password)) from mysql.user where length(password)>0") == '16'
         error("old_passwords exist in mysql.user - Currently this is not supported")
         help("Review https://docs.continuent.com/wiki/display/TEDOC/Changing+MySQL+old+passwords for more information on this problem")
       end
  end
end

class MySQLTriggerCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck

  def set_vars
    @title = "MySQL Trigger check"
  end

  def validate
    info("Checking for MySQL triggers")
    if get_applier_datasource.get_value("select count(*) from information_schema.TRIGGERS;").to_i > 0
      warning("Triggers exist within this instance this can cause problems with replication")
    end
  end
end

class MySQLMyISAMCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck

  def set_vars
    @title = "MySQL MyISAM check"
  end

  def validate
    info("Checking for MySQL MyISAM tables")
    if get_applier_datasource.get_value("select count(*) from information_schema.TABLES where table_schema not in ('mysql','information_schema') and engine='MyISAM'").to_i > 0
      warning("MyISAM tables exist within this instance - These tables are not crash safe and may lead to data loss in a failover")
    end
  end
end

class MySQLCheckSumCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  
  def set_vars
    @title = "MySQL 5.6 binlog Checksum Check"
  end
  
  def validate
    info("Checking that MySQL Binlog Checksum is not enabled")
    checkSum = get_applier_datasource.get_value("show variables like 'binlog_checksum'", "Value")
    if (checkSum == 'CRC32') 
      error("This instance is running with BinLog checksum enabled which is not yet supported")
    end
  end
end

class MySQLDumpCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  
  def set_vars
    @title = "MySQLDump version check"
  end
  
  def validate
    info("Check the mysqldump version matches the running instance version")
    mysqldump=cmd_result("which mysqldump")
    runningVersion = get_applier_datasource.get_value("select @@version").split(".")
    dumpVersion=cmd_result("#{mysqldump} --version | cut -d ' ' -f6").split(".")
     
    if "#{runningVersion[0]}.#{runningVersion[1]}" !=  "#{dumpVersion[0]}.#{dumpVersion[1]}"
      error("The version of Mysqldump in the path does not match the running version of MySQL")
      help("The instance is running #{runningVersion[0]}.#{runningVersion[1]} but the version of mysqldump in the path is #{dumpVersion[0]}.#{dumpVersion[1]} ")
    end
  end
  def enabled?
    super() && ["mysqldump"].include?(@config.getProperty(get_member_key(REPL_BACKUP_METHOD)))
  end
end