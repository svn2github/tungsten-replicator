require "cgi"
class TungstenBackupScript
  include TungstenScript
  
  ACTION_BACKUP = "backup"
  ACTION_RESTORE = "restore"
  ACTION_DELETE = "delete"
  
  def main
    @binlog_file = nil
    @binlog_position = nil
    if TI.trepctl_value(@options[:service], 'role') == "master"
      @master_backup = true
    else
      @master_backup = false
    end
    
    if @options[:action] == ACTION_BACKUP
      start_thl_seqno = TI.trepctl_value(@options[:service], 'maximumStoredSeqNo')
      backup()
      end_thl_seqno = TI.trepctl_value(@options[:service], 'maximumStoredSeqNo')

      if @master_backup == true
        if @binlog_file == nil
          raise "Unable to find the binlog position information for this backup. Please try again or take the backup from a slave"
        end
        binlog_regex = Regexp.new("#{@binlog_file}:[0]+#{@binlog_position}")
        
        if start_thl_seqno == end_thl_seqno
          TU.debug("Compare seqno #{end_thl_seqno} to #{@binlog_file}:#{@binlog_position}")
          
          thl_result = TU.cmd_result("#{TI.thl(@options[:service])} list -headers -json -seqno #{end_thl_seqno}")
          thl_records = JSON.parse(thl_result)
          unless thl_records.instance_of?(Array)
            TU.error("Unable to read the THL record for seqno #{end_thl_seqno}")
          end
          
          thl_record = thl_records[0]
          if thl_record["eventId"] =~ binlog_regex
            TU.debug("Use #{thl_record['seqno']} and #{thl_record['eventId']}")
            TU.cmd_result("echo \"master_backup_schema=#{TI.trepctl_property(@options[:service], 'replicator.schema')}\" >> #{@options[:properties]}")
            TU.cmd_result("echo \"master_backup_seqno=#{thl_record['seqno']}\" >> #{@options[:properties]}")
            TU.cmd_result("echo \"master_backup_epoch=#{thl_record['epoch']}\" >> #{@options[:properties]}")
            TU.cmd_result("echo \"master_backup_eventid=#{thl_record['eventId']}\" >> #{@options[:properties]}")
            TU.cmd_result("echo \"master_backup_sourceid=#{thl_record['sourceId']}\" >> #{@options[:properties]}")
          end
        else
          TU.debug("Search thl from #{start_thl_seqno} to #{end_thl_seqno} for #{@binlog_file}:#{@binlog_position}")
          
          thl_result = TU.cmd_result("#{TI.thl(@options[:service])} list -headers -json -low #{start_thl_seqno} -high #{end_thl_seqno}")
          thl_records = JSON.parse(thl_result)
          unless thl_records.instance_of?(Array)
            TU.error("Unable to read the THL record for seqno #{end_thl_seqno}")
          end
          
          thl_records.each{
            |thl_record|
            if thl_record["eventId"] =~ binlog_regex
              TU.debug("Use #{thl_record['seqno']} and #{thl_record['eventId']}")
              TU.cmd_result("echo \"master_backup_seqno=#{thl_record['seqno']}\" >> #{@options[:properties]}")
              TU.cmd_result("echo \"master_backup_epoch=#{thl_record['epoch']}\" >> #{@options[:properties]}")
              TU.cmd_result("echo \"master_backup_eventid=#{thl_record['eventId']}\" >> #{@options[:properties]}")
              TU.cmd_result("echo \"master_backup_sourceid=#{thl_record['sourceId']}\" >> #{@options[:properties]}")
            end
          }
        end
      end
    elsif @options[:action] == ACTION_RESTORE
      restore()
    else
      raise "Unable to determine the appropriate action for #{self.class.name}"
    end
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

  def configure
    TU.set_log_level(Logger::DEBUG)
    
    TU.remaining_arguments.map!{ |arg|
      # The backup agent sends single dashes instead of double dashes
      if arg[0,1] == "-" && arg[0,2] != "--"
        "-" + arg
      else
        arg
      end
    }
    
    add_option(:backup, {
      :on => "--backup"
    }) {
      @options[:action] = ACTION_BACKUP
      
      nil
    }
    
    add_option(:restore, {
      :on => "--restore"
    }) {
      @options[:action] = ACTION_RESTORE
      
      nil
    }
    
    add_option(:properties, {
      :on => "--properties String"
    })
    
    add_option(:options, {
      :on => "--options String"
    }) {|val|
      CGI::parse(val).each{
        |key,value|
        sym = key.to_sym
        if value.is_a?(Array)
          @options[sym] = value.at(0).to_s
        else
          @options[sym] = value.to_s
        end
      }
      
      nil
    }
  end
end