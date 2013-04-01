class FanInTopology
  include Topology
  
  def allow_multiple_masters?
    true
  end
  
  def self.get_name
    'fan-in'
  end
end