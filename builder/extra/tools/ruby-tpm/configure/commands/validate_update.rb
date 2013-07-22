class ValidateUpdateCommand
  include ConfigureCommand
  include ResetConfigPackageModule
  include ClusterCommandModule
  
  def output_command_usage()
    super()
    
    OutputHandler.queue_usage_output?(true)
    display_cluster_options()
    OutputHandler.flush_usage()
  end
  
  def skip_deployment?(v = nil)
    true
  end
  
  def allow_undefined_dataservice?
    true
  end
  
  def use_remote_tools_only?
    true
  end
  
  def allow_check_current_version?
    true
  end
  
  def use_remote_package?
    true
  end
  
  def self.get_command_name
    'validate-update'
  end
  
  def self.get_command_description
    "Validate before updating an existing Tungsten installation"
  end
end