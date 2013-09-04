class MysqlbinlogFromSeqno
  include TungstenScript
  include MySQLServiceScript
  
  def main
    begin
      begin
        TU.info("Load seqno #{@options[:seqno]}")
        cmd = "#{TI.thl(@options[:service])} list -seqno #{@options[:seqno]} -headers -json"
        thl_record_content = TU.cmd_result(cmd)
      rescue CommandError => ce
        TU.debug(ce)
        raise "There was an error running the thl command on #{@options[:host]}"
      end
    
      thl_records = JSON.parse(thl_record_content)
      unless thl_records.instance_of?(Array) && thl_records.size() == 1
        raise "Unable to read the THL record for seqno #{@options[:seqno]}"
      end
    
      thl_record = thl_records[0]
      event_info = thl_record["eventId"].split(":")
      if event_info.size() != 2
        raise "Unable to parse the THL eventId for the starting file name"
      end
      start_file = event_info[0]
      event_position_info = event_info[1].split(";")
      if event_position_info.size() > 0
        start_position = event_position_info[0]
      else
        raise "Unable to parse the THL eventId for the starting position"
      end
      
      TU.log_cmd_results?(false)
      TU.cmd_stdout("mysqlbinlog --defaults-file=#{@options[:my_cnf]} --port=#{@options[:mysqlport]} --base64-output=DECODE-ROWS --verbose -R -t -h#{thl_record['sourceId']} --start-position=#{start_position} #{start_file}") {
        |line|
        TU.output(line)
      }
      TU.log_cmd_results?(true)
    rescue => e
      raise e
    end
  end

  def configure
    super()
    description("Read mysqlbinlog events from the given THL event forward<br>
Examples:<br>
$> tungsten_mysqlbinlog.sh --seqno=35")
    
    add_option(:seqno, {
      :on => "--seqno String",
      :help => "The sequence number to use as a starting point for mysqlbinlog output"
    })
  end
  
  def validate
    super()
    
    if @options[:seqno] == nil
      TU.error("The --seqno argument is required")
    end
  end
end