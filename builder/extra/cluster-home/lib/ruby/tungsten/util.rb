class TungstenUtil
  include Singleton
  attr_reader :remaining_arguments
  
  def initialize()
    @logger_threshold = Logger::NOTICE
    @ssh_options = {}
    @display_help = false
    
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
    opts.on("-i", "--info")     {@logger_threshold = Logger::INFO}
    opts.on("-n", "--notice")               {@logger_threshold = Logger::NOTICE}
    opts.on("-q", "--quiet")          {@logger_threshold = Logger::WARN}
    opts.on("-v", "--verbose")        {@logger_threshold = Logger::DEBUG}
    opts.on("-h", "--help")           { @display_help = true }
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
  
  def display_help?
    (@display_help == true)
  end
  
  def display_help
    write_header("Global Options", nil)
    output_usage_line("--quiet, -q")
    output_usage_line("--info, -i")
    output_usage_line("--notice, -n")
    output_usage_line("--verbose, -v")
    output_usage_line("--help, -h", "Display this message")
    output_usage_line("--net-ssh-option=key=value", "Set the Net::SSH option for remote system calls", nil, nil, "Valid options can be found at http://net-ssh.github.com/ssh/v2/api/classes/Net/SSH.html#M000002")
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
  
  def write(content="", level=Logger::INFO, hostname = nil, add_prefix = true)
    if content.is_a?(Array)
      content.each{
        |c|
        write(c, level, hostname, add_prefix)
      }
      return
    end
    
    unless content == "" || level == nil || add_prefix == false
      content = "#{get_log_level_prefix(level, hostname)}#{content}"
    end
    
    if enable_log_level?(level)
      if enable_output?()
        puts content
        $stdout.flush()
      end
    end
  end
  
  # Write a header
  def write_header(content, level=Logger::INFO)
    write("#####################################################################", level, nil, false)
    write("# #{content}", level, nil, false)
    write("#####################################################################", level, nil, false)
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
    if level == nil
      true
    elsif level < @logger_threshold
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
  
  # Returns [width, height] of terminal when detected, nil if not detected.
  # Think of this as a simpler version of Highline's Highline::SystemExtensions.terminal_size()
  def detect_terminal_size
    unless @terminal_size
      if (ENV['COLUMNS'] =~ /^\d+$/) && (ENV['LINES'] =~ /^\d+$/)
        @terminal_size = [ENV['COLUMNS'].to_i, ENV['LINES'].to_i]
      elsif (RUBY_PLATFORM =~ /java/ || (!STDIN.tty? && ENV['TERM'])) && command_exists?('tput')
        @terminal_size = [`tput cols`.to_i, `tput lines`.to_i]
      elsif STDIN.tty? && command_exists?('stty')
        @terminal_size = `stty size`.scan(/\d+/).map { |s| s.to_i }.reverse
      else
        @terminal_size = [80, 30]
      end
    end
    
    return @terminal_size
  rescue => e
    [80, 30]
  end
  
  def output_usage_line(argument, msg = "", default = nil, max_line = nil, additional_help = "")
    if max_line == nil
      max_line = detect_terminal_size()[0]-5
    end

    if msg.is_a?(String)
      msg = msg.split("\n").join(" ")
    else
      msg = msg.to_s()
    end

    msg = msg.gsub(/^\s+/, "").gsub(/\s+$/, $/)

    if default.to_s() != ""
      if msg != ""
        msg += " "
      end

      msg += "[#{default}]"
    end

    if argument.length > 28 || (argument.length + msg.length > max_line)
      output(argument)

      wrapped_lines(msg, 29).each{
        |line|
        output(line)
      }
    else
      output(format("%-29s", argument) + " " + msg)
    end

    if additional_help.to_s != ""
      additional_help = additional_help.split("\n").map!{
        |line|
        line.strip()
      }.join(' ')
      additional_help.split("<br>").each{
        |line|
        output_usage_line("", line, nil, max_line)
      }
    end
  end
  
  def wrapped_lines(msg, offset = 0, max_line = nil)
    if max_line == nil
      max_line = detect_terminal_size()[0]-5
    end
    if offset == 0
      default_line = ""
    else
      line_format = "%-#{offset}s"
      default_line = format(line_format, " ")
    end
    
    lines = []
    words = msg.split(' ')

    force_add_word = true
    line = default_line.dup()
    while words.length() > 0
      if !force_add_word && line.length() + words[0].length() > max_line
        lines << line
        line = default_line.dup()
        force_add_word = true
      else
        if line == ""
          line = words.shift()
        else
          line += " " + words.shift()
        end
        force_add_word = false
      end
    end
    lines << line
    
    return lines
  end
end