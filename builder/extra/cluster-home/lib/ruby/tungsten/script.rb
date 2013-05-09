class TungstenScript
  def run
    if TU.display_help?()
      display_help()
      cleanup(0)
    end
    
    validate()
    unless TU.is_valid?()
      cleanup(1)
    end
  end
  
  def validate
    debug("Default validation function is not overridden")
  end
  
  def display_help
    unless get_description() == nil
      TU.output(TU.wrapped_lines(get_description()))
      TU.output("")
    end
    
    TU.display_help()
    TU.write_header("Script Options", nil)
    display_script_help()
  end
  
  def get_description
    nil
  end
  
  def display_script_help
  end
  
  def cleanup(code = 0)
    exit(code)
  end
  
  def nagios_ok(msg)
    puts "OK: #{msg}"
    cleanup(0)
  end
  
  def nagios_warning(msg)
    puts "WARNING: #{msg}"
    cleanup(1)
  end
  
  def nagios_critical(msg)
    puts "CRITICAL: #{msg}"
    cleanup(2)
  end
end