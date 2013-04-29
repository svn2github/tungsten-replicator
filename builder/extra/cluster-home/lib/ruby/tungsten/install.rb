DEFAULTS = "defaults"
REPL_RMI_PORT = "repl_rmi_port"
MGR_RMI_PORT = "mgr_rmi_port"
HOST_ENABLE_REPLICATOR = "host_enable_replicator"
HOST_ENABLE_MANAGER = "host_enable_manager"
HOST_ENABLE_CONNECTOR = "host_enable_connector"

class TungstenInstall
  def initialize(base_path)
    unless File.exists?("#{base_path}/.manifest") && File.exists?("#{base_path}/.lock")
      raise "Unable to use #{base_path} because it is not an installed Tungsten directory"
    end
    
    @topology = nil
    @base_path = base_path
    TU.debug("Initialize #{self.class.name} from " + File.expand_path(@base_path))
    
    tpm_values([
      REPL_RMI_PORT,
      MGR_RMI_PORT,
      HOST_ENABLE_REPLICATOR,
      HOST_ENABLE_MANAGER,
      HOST_ENABLE_CONNECTOR])
  end
  
  def get_base_path
    @base_path
  end
  
  def tpm
    "#{@base_path}/tools/tpm"
  end
  
  def tpm_value(key)
    tpm_values([key])[key]
  end
  
  def tpm_values(keys)
    @tpm_cache ||= {}
    
    remaining_keys = keys - @tpm_cache.keys()
    if remaining_keys.size() > 0
      begin
        JSON.parse(TU.cmd_result("#{tpm()} query values #{remaining_keys.join(' ')}")).each{
          |k, v|
          @tpm_cache[k] = v
        }
      rescue => e
        TU.exception(e)
        raise "Unable to load tpm values #{keys.join(' ')}"
      end
    end
    
    return_values = {}
    keys.each{
      |k|
      return_values[k] = @tpm_cache[k]
    }
    return_values
  end
  
  def cctrl
    "#{@base_path}/tungsten-manager/bin/cctrl -expert -port #{tpm_value(MGR_RMI_PORT)}"
  end
  
  def topology(force_reload = false)
    if @topology == nil || force_reload == true
      if @topology == nil
        @topology = TungstenTopology.new(cctrl())
      end
      @topology.parse()
    end
    
    @topology
  end
  
  def trepctl(service)
    "#{@base_path}/tungsten-replciator/bin/trepctl -port #{tpm_value(REPL_RMI_PORT)} -service #{service}"
  end
  
  def thl(service)
    "#{@base_path}/tungsten-replciator/bin/thl -service #{service}"
  end
  
  def is_replicator?
    (tpm_value(HOST_ENABLE_REPLICATOR) == "true")
  end
  
  def is_manager?
    (tpm_value(HOST_ENABLE_MANAGER) == "true")
  end
  
  def is_connector?
    (tpm_value(HOST_ENABLE_CONNECTOR) == "true")
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
  
  def self.get(path)
    @@instances ||= {}
    unless @@instances.has_key?(path)
      @@instances[path] = TungstenInstall.new(path)
    end
    return @@instances[path]
  end
end