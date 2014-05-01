class ManagerChecks < GroupValidationCheck
  include ClusterHostCheck
  
  def initialize
    super(MANAGERS, "manager", "managers")
    
    ManagerCheck.submodules().each{
      |klass|
      
      self.add_check(klass.new())
    }
  end
  
  def set_vars
    @title = "Manager checks"
  end
end

module ManagerCheck
  include GroupValidationCheckMember
  include ManagerEnabledCheck
  
  def get_host_key(key)
    [HOSTS, @config.getProperty(get_member_key(DEPLOYMENT_HOST)), key]
  end
  
  def get_dataservice
    if self.is_a?(HashPromptMemberModule)
      get_member()
    else
      @config.getPropertyOr(get_member_key(DEPLOYMENT_DATASERVICE), nil)
    end
  end
  
  def get_dataservice_key(key)
    return [DATASERVICES, get_dataservice, key]
  end
  
  def self.included(subclass)
    @submodules ||= []
    @submodules << subclass
  end

  def self.submodules
    @submodules || []
  end
end

class VIPEnabledHostAllowsRootCommands < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "VIP-enabled host allows root commands"
  end
  
  def validate
    unless @config.getProperty(get_host_key(ROOT_PREFIX)) == "true"
      error("You have VIP enabled but the software is not configured to use sudo")
      help("Try setting --enable-sudo-access to true")
    end
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(MGR_VIP_ENABLED)) == "true"
  end
end

class VIPEnabledHostArpPath < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "Check VIP-enabled arp path"
  end
  
  def validate
    arp_path = @config.getProperty(get_member_key(MGR_VIP_ARP_PATH))
    unless arp_path && File.exists?(arp_path)
      error("The arp command '#{arp_path}' does not exist")
    end
  end

  def enabled?
    super() && @config.getProperty(get_member_key(MGR_VIP_ENABLED)) == "true"
  end
end

class VIPEnabledHostIfconfigPath < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "Check VIP-enabled ifconfig path"
  end

  def validate
    ifconfig_path = @config.getProperty(get_member_key(MGR_VIP_IFCONFIG_PATH))
    unless ifconfig_path && File.exists?(ifconfig_path)
      error("The ifconfig command '#{ifconfig_path}' does not exist")
    end
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(MGR_VIP_ENABLED)) == "true"
  end
end

class PingSyntaxCheck < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "Check the ping syntax is supported"
  end
  
  def validate
    plat = IO.popen('uname'){ |f| f.gets.strip }
    case plat
    when 'Darwin'
      cmd_array = ["ping", "-c", "1", "-W", "1000", "localhost"]
    else
      cmd_array = ["ping", "-c", "1", "-w", "1000", "localhost"]
    end
    
    begin
      cmd = Escape.shell_command(cmd_array).to_s
      cmd_result(cmd)
    rescue CommandError
      error("Unable to run the ping utility with '#{cmd}'")
    end
  end
end

class ManagerListenerAddressCheck < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "Manager listener address check"
  end
  
  def validate
    addr = @config.getProperty(get_member_key(MGR_LISTEN_ADDRESS))
    if addr.to_s() == ""
      error("Unable to determine the listening address for the manager")
    end
  end
end

class ManagerWitnessNeededCheck < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "Manager Witness is needed check"
  end
  
  def validate
    witnesses = @config.getProperty(DATASERVICE_WITNESSES).to_s()
    repl_members = @config.getProperty(DATASERVICE_REPLICATION_MEMBERS)
    
    if repl_members.to_s().split(",").size() < 3
      if witnesses == ""
        error("This dataservice is configured with less than 3 members and no witnesses. Update the configuration with an active witness for the highest stability. Visit http://docs.continuent.com/ct/host-types for more information.")
      end
    end
    
    if @config.getProperty(ENABLE_ACTIVE_WITNESSES) == "false" && witnesses != ""
      warning("This dataservice is using a passive witness. Continuent Tungsten has support for active witnesses that improve stability over passive witnesses. Visit http://docs.continuent.com/ct/host-types for more information.")
    end
  end
  
  def enabled?
    super() && (@config.getProperty(MGR_VALIDATE_WITNESS) == "true")
  end
end

class ManagerWitnessAvailableCheck < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "Manager Witness is available check"
  end
  
  def validate
    mgr_address = @config.getProperty(get_member_key(MGR_LISTEN_ADDRESS))
    mgr_netmask = nil
    IfconfigWrapper.new().parse().each{
      |iface|
      begin
        iface.networks.each_value{
          |n|
          if n.is_a?(Ipv4Network)
            if n.addr.to_s() == mgr_address
              mgr_netmask = n.mask
            end
          end
        }
      rescue ArgumentError
      end
    }
    
    if mgr_netmask == nil
      error("Unable to identify the netmask for the Manager IP address")
      return
    end
    
    mgr_address_octets = mgr_address.split(".")
    mgr_netmask_octets = mgr_netmask.split(".")
    
    @config.getProperty(DATASERVICE_WITNESSES).to_s().split(",").each{
      |witness|
      witness_ips = Configurator.instance.get_ip_addresses(witness)
      if witness_ips == false
        error("Unable to find an IP address for the passive witness #{witness}. Continuent Tungsten has support for active witnesses that improve stability over passive witnesses. Visit http://docs.continuent.com/ct/host-types for more information.")
        next
      end
      
      debug("Check if witness #{witness} is pingable")
      if Configurator.instance.check_addresses_is_pingable(witness) == false
        error("The passive witness address '#{witness}' is not returning pings. Continuent Tungsten has support for active witnesses that improve stability over passive witnesses. Visit http://docs.continuent.com/ct/host-types for more information.")
        help("Specify a valid hostname or ip address for the passive witness host ")
      end
      
      witness_octets = witness_ips[0].split(".")
      same_network = true
      
      4.times{
        |i|
        
        a = (mgr_address_octets[i].to_i() & mgr_netmask_octets[i].to_i())
        b = (witness_octets[i].to_i() & mgr_netmask_octets[i].to_i())
        if a != b
          same_network = false
        end
      }
      
      if same_network != true
        error("The passive witness address '#{witness}' is not in the same subnet as the manager. Continuent Tungsten has support for active witnesses that improve stability over passive witnesses. Visit http://docs.continuent.com/ct/host-types for more information.")
      end
    }
  end
  
  def enabled?
    super() && (@config.getProperty(DATASERVICE_WITNESSES).to_s() != "") &&
      (@config.getProperty(ENABLE_ACTIVE_WITNESSES) == "false") &&
      (@config.getProperty(MGR_VALIDATE_WITNESS) == "true")
  end
end

class ManagerHeapThresholdCheck < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "Manager Java Heap threshold check"
  end
  
  def validate
    mem = @config.getProperty(get_member_key(MGR_JAVA_MEM_SIZE))
    threshold = @config.getProperty(get_member_key(MGR_HEAP_THRESHOLD))
    
    if threshold.to_i() <= 0
      error("The value for --mgr-heap-threshold must be greater than zero")
    elsif threshold.to_i() >= mem.to_i()
      error("The value for --mgr-heap-threshold must be less than --mgr-java-mem-size")
    end
  end
end