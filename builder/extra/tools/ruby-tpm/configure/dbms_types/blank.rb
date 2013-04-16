class BlankDatabasePlatform < ConfigureDatabasePlatform
  def get_uri_scheme
    ""
  end
  
  def get_default_master_log_directory
    nil
  end
  
  def get_default_master_log_pattern
    nil
  end
  
  def get_default_port
    ""
  end
  
  def get_thl_uri
    ""
  end
  
  def get_default_start_script
    nil
  end
end