class TungstenScriptMySQLDatasource < TungstenScriptDatasource
  def is_running?
    begin
      sql_result("SELECT 1")
      return true
    rescue
      return false
    end
  end
  
  def _stop_server
    begin
      pid_file = get_variable("pid_file")
      pid = TU.cmd_result("#{@ti.sudo_prefix()}cat #{pid_file}")
    rescue CommandError
      pid = ""
    end
    
    begin
      TU.cmd_result("#{@ti.sudo_prefix()}#{get_service_command()} stop")
    rescue CommandError
    end
    
    # Raise an error if we got a response to the previous command
    if is_running?() == true
      raise "Unable to properly shutdown the MySQL service"
    end
    
    # We saw issues where MySQL would not close completely. This will
    # watch the PID and make sure it does not appear
    unless pid.to_s() == ""
      begin
        TU.debug("Verify that the MySQL pid has gone away")
        Timeout.timeout(30) {
          pid_missing = false
          
          while pid_missing == false do
            begin
              TU.cmd_result("#{@ti.sudo_prefix()}ps -p #{pid}")
              sleep 5
            rescue CommandError
              pid_missing = true
            end
          end
        }
      rescue Timeout::Error
        raise "Unable to verify that MySQL has fully shutdown"
      end
    end
  end
  
  def _start_server
    begin
      TU.cmd_result("#{@ti.sudo_prefix()}#{get_service_command()} start")
    rescue CommandError
    end
    
    # Wait 30 seconds for the MySQL service to be responsive
    begin
      Timeout.timeout(30) {
        while true
          if is_running?()
            break
          else
            # Pause for a second before running again
            sleep 1
          end
        end
      }
    rescue Timeout::Error
      raise "The MySQL server has taken too long to start"
    end
  end
  
  # Read the configured value for a mysql variable
  def get_option(opt)
    begin
      cnf = @ti.setting(@ti.setting_key(REPL_SERVICES, @service, "repl_datasource_mysql_service_conf"))
      val = TU.cmd_result("my_print_defaults --config-file=#{cnf} mysqld | grep -e'^--#{opt.gsub(/[\-\_]/, "[-_]")}='")
    rescue CommandError => ce
      return nil
    end

    return val.split("\n")[0].split("=")[1]
  end
  
  # Read the current value for a mysql variable
  def get_variable(var)
    begin
      sql_result("SHOW VARIABLES LIKE '#{var}'")[0]["Value"]
    rescue => e
      TU.debug(e)
      return nil
    end
  end
  
  def get_service_command
    if @mysql_service_command == nil
      if @mysql_service_command == nil
        @mysql_service_command = @ti.setting(TI.setting_key(REPL_SERVICES, @service, "repl_datasource_boot_script"))
      end
      if @mysql_service_command == nil
        begin
          service_command=TU.cmd_result("which service")
          if TU.cmd("#{@ti.sudo_prefix()}test -x #{service_command}")
            if TU.cmd("#{@ti.sudo_prefix()}test -x /etc/init.d/mysqld")
              @mysql_service_command = "#{service_command} mysqld"
            elsif TU.cmd("#{@ti.sudo_prefix()}test -x /etc/init.d/mysql")
              @mysql_service_command = "#{service_command} mysql"
            else
              TU.error "Unable to determine the service command to start/stop mysql"
            end
          else
            if TU.cmd("#{@ti.sudo_prefix()}test -x /etc/init.d/mysqld")
              @mysql_service_command = "/etc/init.d/mysqld"
            elsif TU.cmd("#{@ti.sudo_prefix()}test -x /etc/init.d/mysql")
              @mysql_service_command = "/etc/init.d/mysql"
            else
              TU.error "Unable to determine the service command to start/stop mysql"
            end
          end
        rescue CommandError
          TU.error "Unable to determine the service command to start/stop mysql"
        end
      end
    end
    
    @mysql_service_command
  end
  
  def get_system_user
    if @mysql_user == nil
      @mysql_user = get_option("user")
      if @mysql_user.to_s() == ""
        @mysql_user = "mysql"
      end
    end
    
    @mysql_user
  end
  
  def snapshot_paths
    paths = []
    
    # The datadir may not exist in my.cnf but we need the value
    # If we don't see it in my.cnf get the value from the dbms
    val = get_option("datadir")
    if val == nil
      val = get_variable("datadir")
    end
    paths << val
    
    # These values must appear in my.cnf if they are to be used
    val = get_option("innodb_data_home_dir")
    if val != nil
      paths << val
    end
    val = get_option("innodb_log_group_home_dir")
    if val != nil
      paths << val
    end
    
    # Only return a unique set of paths
    paths.uniq()
  end
  
  def can_manage_service?
    true
  end
end