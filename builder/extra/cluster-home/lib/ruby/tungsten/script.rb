class TungstenScript
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