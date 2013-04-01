class ClusterSlaveTopology
  include Topology
  
  def get_master_thl_uri(hostname)
    values = []
    
    relay_source = @config.getProperty([DATASERVICES, @ds_alias, DATASERVICE_RELAY_SOURCE])
    relay_source.split(",").each{
      |relay_alias|
      
      hosts = @config.getTemplateValue([DATASERVICES, relay_alias, DATASERVICE_MEMBERS]).to_s().split(",")
      port = @config.getTemplateValue([DATASERVICES, relay_alias, DATASERVICE_THL_PORT])
      values << _splice_hosts_port(hosts, port)
    }
    
    return values.join(",")
  end
  
  def get_role(hostname)
    return REPL_ROLE_S
  end
  
  def self.get_name
    'cluster-slave'
  end
end