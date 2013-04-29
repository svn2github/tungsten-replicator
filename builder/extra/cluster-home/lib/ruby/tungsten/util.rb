class TungstenUtil
  include Singleton
  attr_reader :remaining_arguments
  
  def initialize()
    @logger_threshold = Logger::NOTICE
    @ssh_options = {}
    
    arguments = ARGV.dup
    opts=OptionParser.new
    arguments = arguments.map{|arg|
      newarg = ''
      arg.split("").each{|b| 
        unless b.getbyte(0)<33 || b.getbyte(0)>127 then 
          newarg.concat(b) 
        end
      }
      newarg
    }
    opts.on("-n", "-i", "--info")     {@logger_threshold = Logger::INFO}
    opts.on("--notice")               {@logger_threshold = Logger::NOTICE}
    opts.on("-q", "--quiet")          {@logger_threshold = Logger::WARN}
    opts.on("-v", "--verbose")        {@logger_threshold = Logger::DEBUG}
    opts.on("--net-ssh-option String")  {|val|
                                        val_parts = val.split("=")
                                        if val_parts.length() !=2
                                          error "Invalid value #{val} given for '--net-ssh-option'.  There should be a key/value pair joined by a single =."
                                        end
                                        
                                        if val_parts[0] == "timeout"
                                          val_parts[1] = val_parts[1].to_i
                                        end

                                        @ssh_options[val_parts[0].to_sym] = val_parts[1]
                                      }
    @remaining_arguments = run_option_parser(opts, arguments)
  end
  
  def get_base_path
    return File.expand_path("#{File.dirname(__FILE__)}/../../../..")
  end
  
  def enable_output?
    true
  end
  
  def output(content)
    write(content, nil)
  end
  
  def force_output(content)
    puts(content)
    $stdout.flush()
  end
  
  def write(content="", level=Logger::INFO, hostname = nil, force = false)
    if !enable_log_level?(level) && force == false
      return
    end
    
    unless content == ""
      content = get_log_level_prefix(level, hostname) + content
    end
    
    if enable_output?()
      puts content
      $stdout.flush()
    end
  end
  
  def info(message, hostname = nil)
    write(message, Logger::INFO, hostname)
  end
  
  def notice(message, hostname = nil)
    write(message, Logger::NOTICE, hostname)
  end
  
  def warning(message, hostname = nil)
    write(message, Logger::WARN, hostname)
  end
  
  def error(message, hostname = nil)
    write(message, Logger::ERROR, hostname)
  end
  
  def exception(e, hostname = nil)
    error(e.to_s() + ":\n" + e.backtrace.join("\n"), hostname)
  end
  
  def debug(message, hostname = nil)
    if message.is_a?(StandardError)
      message = message.to_s() + ":\n" + message.backtrace.join("\n")
    end
    write(message, Logger::DEBUG, hostname)
  end
  
  def get_log_level_prefix(level=Logger::INFO, hostname = nil)
    case level
    when Logger::ERROR then prefix = "ERROR"
    when Logger::WARN then prefix = "WARN "
    when Logger::DEBUG then prefix = "DEBUG"
    when Logger::NOTICE then prefix = "NOTE"
    else
      prefix = "INFO "
    end
    
    if hostname == nil
      "#{prefix} >> "
    else
      "#{prefix} >> #{hostname} >> "
    end
  end
  
  def enable_log_level?(level=Logger::INFO)
    if level < @logger_threshold
      false
    else
      true
    end
  end
  
  def set_log_level(level=Logger::INFO)
    @logger_threshold = level
  end
  
  def whoami
    if ENV['USER']
      ENV['USER']
    elsif ENV['LOGNAME']
      ENV['LOGNAME']
    else
      `whoami 2>/dev/null`.chomp
    end
  end
  
  def run_option_parser(opts, arguments, allow_invalid_options = true, invalid_option_prefix = nil)
    remaining_arguments = []
    while arguments.size() > 0
      begin
        arguments = opts.order!(arguments)
        
        # The next argument does not have a dash so the OptionParser
        # ignores it, we will add it to the stack and continue
        if arguments.size() > 0 && (arguments[0] =~ /-.*/) != 0
          remaining_arguments << arguments.shift()
        end
      rescue OptionParser::InvalidOption => io
        if allow_invalid_options
          # Prepend the invalid option onto the arguments array
          remaining_arguments = remaining_arguments + io.recover([])
        
          # The next argument does not have a dash so the OptionParser
          # ignores it, we will add it to the stack and continue
          if arguments.size() > 0 && (arguments[0] =~ /-.*/) != 0
            remaining_arguments << arguments.shift()
          end
        else
          if invalid_option_prefix != nil
            io.reason = invalid_option_prefix
          end
          
          raise io
        end
      rescue => e
        raise "Argument parsing failed: #{e.to_s()}"
      end
    end
    
    remaining_arguments
  end
end