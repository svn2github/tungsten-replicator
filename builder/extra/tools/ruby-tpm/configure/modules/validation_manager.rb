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

class ManagerWitnessAvailableCheck < ConfigureValidationCheck
  include ManagerCheck
  
  def set_vars
    @title = "Manager Witness is available check"
  end
  
  def validate
    @config.getProperty(DATASERVICE_WITNESSES).to_s().split(",").each{|witness|
      witness_ips = Configurator.instance.get_ip_addresses(witness)
      if witness_ips == false
        error("Unable to find an IP address for #{witness}")
        next
      end
      
      debug("Check if witness #{witness} is pingable")
      if Configurator.instance.check_addresses_is_pingable(witness) == false
        error("The manager witness address  '#{witness}' is not returning pings")
        help("Specify a valid hostname or ip address for the witness host ")
      end
    }
  end
  
  def enabled?
    super() && (@config.getProperty(DATASERVICE_WITNESSES).to_s() != "")
  end
end