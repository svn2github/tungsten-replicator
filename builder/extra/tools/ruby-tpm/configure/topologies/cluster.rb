if Configurator.instance.is_enterprise?()
  class ClusterTopology
    include Topology
  
    def use_management?
      true
    end
  
    def use_connector?
      true
    end
    
    def disable_relay_logs?
      "true"
    end
  
    def self.get_name
      'clustered'
    end
  
    def self.is_default?
      true
    end
  end
else
  class ClusterTopology
  end
end