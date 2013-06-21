class TungstenInstall
  def initialize(base_path)
    unless File.exists?("#{base_path}/.manifest") && File.exists?("#{base_path}/.lock")
      raise "Unable to use #{base_path} because it is not an installed Tungsten directory"
    end
    
    @root = TU.cmd_result("cat #{base_path}/.lock")
    TU.debug("Initialize #{self.class.name} from #{@root}")
    @settings = {}
    @topology = nil
    @has_tpm = (TU.cmd_result("#{tpm()} query staging") != "")
    
    # Preload settings about this installation
    if use_tpm?()
      # Pull the values from tpm
      settings([
        "user",
        "host_name",
        HOST_ENABLE_REPLICATOR,
        HOST_ENABLE_MANAGER,
        HOST_ENABLE_CONNECTOR,
        REPL_RMI_PORT,
        MGR_RMI_PORT,
        MGR_API,
        MGR_API_PORT,
        MGR_API_ADDRESS,
        "preferred_path"
        ])
    else
      # Read the values from files
      setting(HOST_ENABLE_REPLICATOR, "true")
      setting(HOST_ENABLE_MANAGER, "false")
      setting(HOST_ENABLE_CONNECTOR, "false")
      setting(REPL_RMI_PORT, TU.cmd_result("grep rmi_port #{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/conf/services.properties | grep -v '^#' | awk -F= '{print $2}' | tr -d ' '"))
      setting("host_name", TU.cmd_result("egrep '^replicator.host=' tungsten/tungsten-replicator/conf/services.properties | awk -F= '{print $2}'"))
    end
  end
  
  def root
    @root
  end
  
  def hostname
    setting("host_name")
  end
  
  def user
    setting("user")
  end
  
  def dataservices
    if use_tpm?()
      TU.cmd_result("#{tpm()} query dataservices | awk -F \" \" '{print $1}'").split("\n")
    else
      TU.cmd_result("egrep \"^service.name\" #{root()}/tungsten/conf/static-* | awk -F \"=\" '{print $2}'").split("\n")
    end
  end
  
  def default_dataservice
    if is_manager?()
      setting("dataservice_name")
    elsif is_replicator?()
      local_services = TU.cmd_result("egrep -l \"^replicator.service.type=local\" #{root()}/tungsten-replicator/conf/static*")
      if local_services.size() == 0
        dataservices().get(0)
      else
        TU.cmd_result("egrep \"^service.name\" #{local_services[0]} | awk -F \"=\" '{print $2}'")
      end
    else
      dataservices()[0]
    end
  end
  
  def tpm
    "#{@root}/#{CURRENT_RELEASE_DIRECTORY}/tools/tpm"
  end
  
  def setting(key, v = nil)
    if v == nil
      return settings([key])[key]
    else
      @settings[key] = v
      
      return v
    end
  end
  
  def settings(keys)
    remaining_keys = keys - @settings.keys()
    if remaining_keys.size() > 0
      if use_tpm?()    
        begin
          JSON.parse(TU.cmd_result("#{tpm()} query values #{remaining_keys.join(' ')}")).each{
            |k, v|
            @settings[k] = v
          }
        rescue => e
          TU.exception(e)
          raise "Unable to load tpm values #{keys.join(' ')}"
        end
      else
        TU.debug("Unable to autodetect settings because tpm was not used to install this directory")
      end
    end
    
    return_values = {}
    keys.each{
      |k|
      return_values[k] = @settings[k]
    }
    return_values
  end
  
  def cctrl
    "#{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-manager/bin/cctrl -expert -port #{setting(MGR_RMI_PORT)}"
  end
  
  def mgr_api_uri
    if setting(MGR_API_ADDRESS) == "0.0.0.0"
      "#{hostname()}:#{setting(MGR_API_PORT)}"
    else
      "#{setting(MGR_API_ADDRESS)}:#{setting(MGR_API_PORT)}"
    end
  end
  
  def status(dataservice = nil)
    if dataservice == nil
      dataservice = default_dataservice()
    end
    
    unless dataservices().include?(dataservice)
      raise "Unable to provide a status for #{dataservice} because it is not defined on this host"
    end
    
    return TungstenStatus.new(self, dataservice)
  end
  
  def topology(dataservice = nil)
    if dataservice == nil
      dataservice = default_dataservice()
    end
    
    unless dataservices().include?(dataservice)
      raise "Unable to provide a topology for #{dataservice} because it is not defined on this host"
    end
    
    return TungstenTopology.new(self, dataservice)
  end
  
  def trepctl(service)
    "#{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/bin/trepctl -port #{setting(REPL_RMI_PORT)} -service #{service}"
  end
  
  def trepctl_value(service, key)
    TU.cmd_result("#{trepctl(service)} status | grep #{key} | awk -F: '{print $2}' | tr -d ' '")
  end
  
  def trepctl_property(service, key)
    properties = JSON.parse(TU.cmd_result("#{trepctl(service)} properties -filter #{key}"))
    if properties.has_key?(key)
      return properties[key]
    else
      raise "Unable to find a value for #{key} in the output of `trepctl -service #{service} properties`."
    end
  end
  
  def thl(service)
    "#{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/bin/thl -service #{service}"
  end
  
  def service_path(component)
    "#{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-#{component}/bin/#{component}"
  end
  
  def is_running?(component)
    begin
      TU.cmd_result("#{service_path(component)} status")
      return true
    rescue CommandError
      return false
    end
  end
  
  def is_replicator?
    (setting(HOST_ENABLE_REPLICATOR) == "true")
  end
  
  def is_manager?
    (setting(HOST_ENABLE_MANAGER) == "true")
  end
  
  def is_connector?
    (setting(HOST_ENABLE_CONNECTOR) == "true")
  end
  
  def is_commercial?
    File.exists?("#{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-manager")
  end
  
  def use_tpm?
    @has_tpm
  end
  
  def ensure_cctrl(cmd, max_tries = 5)
    i=0
    while (i<max_tries)
      begin
        return TU.cmd_result("echo #{cmd}| #{cctrl}")
      rescue CommandError
        TU.debug(e)
      end
      i+=1
    end
    
    raise "Unable to execute '#{cmd}' in cctrl"
  end
  
  def inherit_path
    if setting("preferred_path") != ""
      ENV['PATH'] = setting("preferred_path") + ":" + ENV['PATH']
    end
  end
  
  def self.get(path)
    @@instances ||= {}
    unless @@instances.has_key?(path)
      @@instances[path] = TungstenInstall.new(path)
    end
    return @@instances[path]
  end
  
  class TungstenTopology
    attr_reader :datasources, :master, :connectors, :dataservices, :type

    def initialize(install, dataservice)
      @install = install
      @name = dataservice
      
      unless @install.use_tpm?()
        raise "Unable to parse the topology for #{@name} from #{@install.hostname()}:#{@install.root()} because tpm was not used for installation"
      end
      
      values = @install.settings([
        "dataservices.#{@name}.dataservice_hosts",
        "dataservices.#{@name}.dataservice_master_host",
        "dataservices.#{@name}.dataservice_connectors",
        "dataservices.#{@name}.dataservice_composite_datasources",
        "dataservices.#{@name}.dataservice_topology"
      ])
      
      @type = values["dataservices.#{@name}.dataservice_topology"]
      @members = values["dataservices.#{@name}.dataservice_hosts"].to_s().split(",")
      @master = values["dataservices.#{@name}.dataservice_master_host"]
      @connectors = values["dataservices.#{@name}.dataservice_connectors"].to_s().split(",")
      @dataservices = values["dataservices.#{@name}.dataservice_composite_datasources"].to_s().split(",")
    end
    
    def is_composite?
      (@dataservices.size() > 0)
    end
    
    def to_hash
      {
        :hostname => @install.hostname(),
        :root => @install.root(),
        :is_composite => is_composite?(),
        :type => @type,
        :members => @members,
        :master => @master,
        :connectors => @connectors,
        :dataservices => @dataservices
      }
    end
  end
end