require "cgi"
class TungstenBackupScript < TungstenScript
  ACTION_BACKUP = "backup"
  ACTION_RESTORE = "restore"
  ACTION_DELETE = "delete"
  
  def run
    TU.output("Options:")
    @options.each{
      |k,v|
      TU.output("    #{k} => #{v}")
    }
    super()
    
    if @options[:action] == ACTION_BACKUP
      start_thl_seqno = TI.trepctl_value(@options[:service], 'maximumStoredSeqNo')
      backup()
      end_thl_seqno = TI.trepctl_value(@options[:service], 'maximumStoredSeqNo')

      if @master_backup == true
        if @binlog_file == nil
          raise "Unable to find the binlog position information for this backup. Please try again or take the backup from a slave"
        end
        TU.cmd_result("echo \"master_eventid=#{@binlog_file}:#{@binlog_position}\" >> #{@options[:properties]}")

        if start_thl_seqno == end_thl_seqno
          TU.debug("Compare seqno #{end_thl_seqno} to #{@binlog_file}:#{@binlog_position}")
        else
          TU.debug("Search thl from #{start_thl_seqno} to #{end_thl_seqno} for #{@binlog_file}:#{@binlog_position}")
        end
      end
    elsif @options[:action] == ACTION_RESTORE
      restore()
    else
      raise "Unable to determine the appropriate action for #{self.class.name}"
    end
    
    cleanup(0)
  end
  
  def cleanup(code = 0)
    TU.output("Finish #{$0} #{ARGV.join(' ')}")
    super(code)
  end
  
  def backup
    raise "You must define the #{self.class.name}.backup method"
  end
  
  def restore
    raise "You must define the #{self.class.name}.backup method"
  end
  
  def validate
    if @options[:action] == ACTION_BACKUP
      if @master_backup == true && TI.trepctl_value(@options[:service], 'state') != "ONLINE"
        TU.error("Unable to backup a master host unless it is ONLINE. Try running `trepctl -service #{@options[:service]} online`.")
      end
    end
  end

  def initialize
    TU.output("Begin #{$0} #{ARGV.join(' ')}")
    TU.set_log_level(Logger::DEBUG)
    @options ||= {}
    @options[:action] = nil
    
    TU.remaining_arguments.map!{ |arg|
      # The backup agent sends single dashes instead of double dashes
      if arg[0,1] == "-" && arg[0,2] != "--"
        "-" + arg
      else
        arg
      end
    }
    
    opts=OptionParser.new
    opts.on("--backup")  { @options[:action] = ACTION_BACKUP }
    opts.on("--restore") { @options[:action] = ACTION_RESTORE }
    opts.on("--properties String") {|val| 
      @options[:properties] = val
    }
    opts.on("--options String")  {|val|
      CGI::parse(val).each{
        |key,value|
        sym = key.to_sym
        if value.is_a?(Array)
          @options[sym] = value.at(0).to_s
        else
          @options[sym] = value.to_s
        end
      }
    }
    TU.run_option_parser(opts)
    
    @binlog_file = nil
    @binlog_position = nil
    if TI.trepctl_value(@options[:service], 'role') == "master"
      @master_backup = true
    else
      @master_backup = false
    end
  end
end