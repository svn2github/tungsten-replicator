DBMS_MONGODB = "mongodb"
 
# MongoDB-specific parameters.
class MongoDBDatabasePlatform < ConfigureDatabasePlatform
  def get_uri_scheme
    DBMS_MONGODB
  end

  def get_default_backup_method
    "none"
  end

  def get_valid_backup_methods
    "none|script"
  end

  def get_thl_uri
    nil
  end

  def get_default_port
    "27017"
  end

  def get_default_start_script
    nil
  end

  def getBasicJdbcUrl()
    nil
  end
  
  def getJdbcUrl()
    nil
  end

  def getJdbcDriver()
    nil
  end

  def getVendor()
    "MongoDB"
  end

  def get_extractor_template
    raise "Unable to use MongoDBDatabasePlatform as an extractor"
  end

  def get_applier_filters()
    []
  end

  def get_default_master_log_directory
    nil
  end

  def get_default_master_log_pattern
    nil
  end
end

#
# Prompts
#

class MongoDBConfigurePrompt < ConfigurePrompt
  def get_default_value
    begin
      if Configurator.instance.display_help? && !Configurator.instance.display_preview?
        raise ""
    end

  get_mongodb_default_value()
    rescue => e
      super()
    end
  end

  def get_mongodb_default_value
    raise "Undefined function"
  end

  # Execute mysql command and return result to client. 
  def mongodb(command, hostname = nil)
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
    super() && (get_datasource().is_a?(MongoDBDatabasePlatform))
  end

  def enabled_for_config?
    super() && (get_datasource().is_a?(MongoDBDatabasePlatform))
  end
end

#
# Validation
#
class MongoDBValidationCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  def get_variable(name)
    mongodb("show #{name}").chomp.strip;
  end

  def set_vars
    @title = "MongoDB parallel apply check"
  end

  def enabled?
    super() && (get_extractor_datasource().is_a?(MongoDBDatabasePlatform))
  end

  def validate
    debug("Checking to ensure parallel apply is disabled")
    ptype = @config.getProperty(get_member_key(REPL_SVC_PARALLELIZATION_TYPE))
    if ptype == "disk" || ptype == "memory"
      error("The MongoDB applier does not support parallel apply")
      help("Use --svc-parallelization-type=none or omit the option entirely")
    end
  end
end 
