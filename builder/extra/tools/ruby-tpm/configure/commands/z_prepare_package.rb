class PrepareCommand
  include ConfigureCommand
  include RemoteCommand
  include ClusterCommandModule
  include ResetBasenamePackageModule
  
  def get_command_name
    'prepare'
  end
  
  def validate_commit
    return true
  end
  
  def commit
    return true
  end
  
  def enable_log?()
    true
  end
  
  def output_completion_text
    notice <<NOTICE
You have prepared a new installation.  Use the promote tool to activate it.
NOTICE

    super()
  end
  
  def use_remote_package?
    true
  end
  
  def self.get_command_name
    'prepare'
  end
  
  def self.get_command_description
    "Create a new installation directory using the local configuration but do not make it active.  You should run promote after you have ran the prepare command."
  end
end