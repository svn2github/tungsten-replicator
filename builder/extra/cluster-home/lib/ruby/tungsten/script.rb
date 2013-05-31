module TungstenScript
  NAGIOS_OK=0
  NAGIOS_WARNING=1
  NAGIOS_CRITICAL=2
  
  def run
    main()
    cleanup(0)
  end
  
  def initialize
    @require_installed_directory = true
    @option_definitions = {}
    @options = {}
    
    TU.debug("Begin #{$0} #{ARGV.join(' ')}")
    
    configure()
    
    if TU.display_help?()
      display_help()
      cleanup(0)
    end
    
    parse_options()
    
    unless TU.is_valid?()
      cleanup(1)
    end
    
    TU.debug("Options:")
    @options.each{
      |k,v|
      TU.debug("    #{k} => #{v}")
    }
    
    validate()
    
    unless TU.is_valid?()
      cleanup(1)
    end
  end
  
  def configure
  end
  
  def opt(option_key, value = nil)
    if value != nil
      @options[option_key] = value
    end
    
    return @options[option_key]
  end
  
  def add_option(option_key, definition, &parse)
    option_key = option_key.to_sym()
    if @option_definitions.has_key?(option_key)
      raise "The #{option_key} option has already been defined"
    end
    
    unless definition[:on].is_a?(Array)
      definition[:on] = [definition[:on]]
    end
    
    if parse != nil
      definition[:parse] = parse
    end
    
    if definition.has_key?(:default)
      opt(option_key, definition[:default])
    end
    
    @option_definitions[option_key] = definition
  end
  
  def parse_options
    opts = OptionParser.new()
    
    @option_definitions.each{
      |option_key,definition|
      
      args = definition[:on]
      opts.on(*args) {
        |val|
                
        if definition[:parse] != nil
          begin
            val = definition[:parse].call(val)
            
            unless val == nil
              opt(option_key, val)
            end
          rescue MessageError => me
            TU.error(me.message())
          end
        else  
          opt(option_key, val)
        end
      }
    }
    
    TU.run_option_parser(opts)
  end
  
  def parse_integer_option(val)
    val.to_i()
  end
  
  def parse_float_option(val)
    val.to_f()
  end
  
  def parse_boolean_option(val)
    if val == "true"
      true
    elsif val == "false"
      false
    else
      raise MessageError.new("Unable to parse value '#{val}'")
    end
  end
  
  def validate
    if require_installed_directory?() && TI == nil
      TU.error("Unable to run #{$0} without the '--directory' argument pointing to an active Tungsten installation")
    end
  end
  
  def display_help
    unless description() == nil
      TU.output(TU.wrapped_lines(description()))
      TU.output("")
    end
    
    TU.display_help()
    TU.write_header("Script Options", nil)
    
    @option_definitions.each{
      |option_key,definition|
      
      if definition[:help].is_a?(Array)
        help = definition[:help].shift()
        additional_help = definition[:help]
      else
        help = definition[:help]
        additional_help = []
      end
      
      TU.output_usage_line(definition[:on].join(","),
        help, definition[:default], nil, additional_help.join("\n"))
    }
  end
  
  def require_installed_directory?(v = nil)
    if (v != nil)
      @require_installed_directory = v
    end
    
    @require_installed_directory
  end
  
  def description(v = nil)
    if v != nil
      @description = v
    end
    
    @description || nil
  end
  
  def script_log_path
    nil
  end
  
  def cleanup(code = 0)
    if TU.display_help?() != true && script_log_path() != nil
      File.open(script_log_path(), "w") {
        |f|
        TU.log().rewind()
        f.puts(TU.log().read())
      }
    end
    
    TU.debug("Finish #{$0} #{ARGV.join(' ')}")
    exit(code)
  end
  
  def nagios_ok(msg)
    puts "OK: #{msg}"
    cleanup(NAGIOS_OK)
  end
  
  def nagios_warning(msg)
    puts "WARNING: #{msg}"
    cleanup(NAGIOS_WARNING)
  end
  
  def nagios_critical(msg)
    puts "CRITICAL: #{msg}"
    cleanup(NAGIOS_CRITICAL)
  end
end