class TungstenInstall
  def initialize(base_path)
    unless File.exists?("#{base_path}/.manifest") && File.exists?("#{base_path}/.lock")
      raise "Unable to use #{base_path} because it is not an installed Tungsten directory"
    end
    
    @root = TU.cmd_result("cat #{base_path}/.lock")
    TU.debug("Initialize #{self.class.name} from #{@root}")
    @settings = {}
    @topology = nil
    @dataservices = nil
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
        MGR_API_PORT
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
    if is_replicator?()
      if @dataservices == nil
        unless is_running?("replicator")
          raise "Unable to get dataservice list because the replicator isn't running"
        end
        @dataservices = TU.cmd_result("#{trepctl()} services | grep serviceName | awk -F: '{print $2}' | tr -d ' '").split("\n")
      end
      @dataservices
    else
      return nil
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
  
  def topology(dataservice = nil)
    if is_manager?()
      unless is_running?("manager")
        return nil
      end
    elsif is_replicator?()
      unless is_running?("replicator")
        return nil
      end
    else
      # This is a connector server
      return nil
    end
    
    return TungstenTopology.new(self, dataservice)
  end
  
  def trepctl(service = nil)
    if service == nil
      "#{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/bin/trepctl -port #{setting(REPL_RMI_PORT)}"
    else
      "#{@root}/#{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/bin/trepctl -port #{setting(REPL_RMI_PORT)} -service #{service}"
    end
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
  
  def manager_api_result(path, return_path = [])
    if setting(MGR_API) != "true"
      raise "Unable to use Manager API because it isn't enabled"
    end
    require "open-uri"
    current_val = JSON.parse(open("http://localhost:#{setting(MGR_API_PORT)}/manager/#{path}") {|f| f.read })
    
    if current_val["commandStatus"] != 0
      raise "There was an error calling '/manager/#{path}' on #{hostname()}: #{current_val['message']}"
    end
    
    return_path_count = return_path.size()
    for i in 0..(return_path_count-1)
      attr_name = return_path[i]
      return current_val[attr_name] if i == (return_path_count-1)
      return nil if current_val[attr_name].nil?
      current_val = current_val[attr_name]
    end
    
    return current_val
  end
  
  def self.get(path)
    @@instances ||= {}
    unless @@instances.has_key?(path)
      @@instances[path] = TungstenInstall.new(path)
    end
    return @@instances[path]
  end
end