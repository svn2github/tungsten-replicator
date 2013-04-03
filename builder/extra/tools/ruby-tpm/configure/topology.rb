module Topology
  @@classes = false
  
  def initialize(ds_alias, config)
    @ds_alias = ds_alias
    @config = config
  end
  
  def allow_multiple_masters?
    false
  end
  
  def use_replicator?
    true
  end
  
  def use_management?
    false
  end
  
  def use_connector?
    false
  end
  
  def get_master_thl_uri(hostname)
    rs_alias = @ds_alias + "_" + to_identifier(hostname)
    hosts = @config.getTemplateValue([REPL_SERVICES, rs_alias, REPL_MASTERHOST]).to_s().split(",")
    port = @config.getTemplateValue([REPL_SERVICES, rs_alias, REPL_MASTERPORT])
    
    return _splice_hosts_port(hosts, port)
  end
  
  def get_role(hostname)
    if @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_MASTER_MEMBER]) == hostname
      relay_source = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_RELAY_SOURCE])
      
      if relay_source.to_s == ""
        return REPL_ROLE_M
      else
        return REPL_ROLE_R
      end
    else
      return REPL_ROLE_S
    end
  end
  
  def _splice_hosts_port(hosts, default_port)
    values = []
    
    hosts.each{
      |host|
      
      if host.index(':') == nil
        values << "thl://#{host}:#{default_port}/"
      else
        values << "thl://#{host}"
      end
    }
    
    return values.join(",")
  end
  
  def self.build(ds_alias, config)
    klass = Topology.get_class(config.getProperty([DATASERVICES, ds_alias, DATASERVICE_TOPOLOGY]))
    return klass.new(ds_alias, config)
  end
  
  def self.get_classes
    unless @@classes
      @@classes = {}

      self.subclasses.each{
        |klass|
        begin
          @@classes[klass.get_name()] = klass
        rescue NoMethodError
        end
      }
    end
    
    @@classes
  end
  
  def self.get_types
    return self.get_classes().keys().delete_if{
      |key|
      key.to_s == ""
    }
  end
  
  def self.get_class(name)
    get_classes().each{
      |klass_name,klass|
      
      if klass_name == name
        return klass
      end
    }
    
    raise "Unable to find a topology class for #{name}"
  end
  
  def self.included(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end
  
  def self.subclasses
    @subclasses
  end
end