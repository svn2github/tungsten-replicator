module ClusterDiagnosticPackage
  LOG_SIZE = 10*1024*1024
  
  def get_diagnostic_file
    @zip_file
  end
  
  def parsed_options?(arguments)
    remainder = super(arguments)
    
    @zip_file = nil
    
    return remainder
  end
  
  def commit
    super()
    
    begin
      diag_dir = "#{ENV['OLDPWD']}/tungsten-diag-#{Time.now.localtime.strftime("%Y-%m-%d-%H-%M-%S")}"
      Timeout.timeout(5) {
        while File.exists?(diag_dir)
          diag_dir = "#{ENV['OLDPWD']}/tungsten-diag-#{Time.now.localtime.strftime("%Y-%m-%d-%H-%M-%S")}"
        end
      }
    rescue Timeout::Error
      error("Unable to use the #{diag_dir} directory because it already exists")
    end
    FileUtils.mkdir_p(diag_dir)
    
    get_deployment_configurations().each{
      |config|
      build_topologies(config)
      
      h_alias = config.getProperty(DEPLOYMENT_HOST)
      FileUtils.mkdir_p("#{diag_dir}/#{h_alias}")
      FileUtils.mkdir_p("#{diag_dir}/#{h_alias}/os_info")
      
      out = File.open("#{diag_dir}/#{h_alias}/manifest.json", "w")
      out.puts(@promotion_settings.getProperty([h_alias, "manifest"]))
      out.close
      
      out = File.open("#{diag_dir}/#{h_alias}/tpm.txt", "w")
      out.puts(@promotion_settings.getProperty([h_alias, "tpm_reverse"]))
      out.close

      out = File.open("#{diag_dir}/#{h_alias}/tpm_diff.txt", "w")
      out.puts(@promotion_settings.getProperty([h_alias, "tpm_diff"]))
      out.close

      begin
        scp_download("/etc/hosts", "#{diag_dir}/#{h_alias}/os_info/etc_hosts.txt", config.getProperty(HOST), config.getProperty(USERID))
      rescue
        next
      end


      begin
        log = "/etc/system-release"
        if remote_file_exists?(log, config.getProperty(HOST), config.getProperty(USERID))
          scp_download(log, "#{diag_dir}/#{h_alias}/os_info/system-release.txt", config.getProperty(HOST), config.getProperty(USERID))
        end
      rescue
        next
      end


      #Run a lsb_release -a  if it's available in the path
      begin
        lsb_path = ssh_result("which lsb_release 2>/dev/null", config.getProperty(HOST), config.getProperty(USERID))
        if lsb_path != ""
          lsb_output=ssh_result("lsb_release -a", config.getProperty(HOST), config.getProperty(USERID))
          out = File.open("#{diag_dir}/#{h_alias}/os_info/lsb_release.txt", "w")
          out.puts(lsb_output)
          out.close
        end
      rescue

      end


      if @promotion_settings.getProperty([h_alias, REPLICATOR_ENABLED]) == "true"
        if @promotion_settings.getProperty([h_alias, MANAGER_ENABLED]) == "true"
          out = File.open("#{diag_dir}/#{h_alias}/cctrl.txt", "w")
          out.puts(@promotion_settings.getProperty([h_alias, "cctrl_status"]))
          out.close
          out = File.open("#{diag_dir}/#{h_alias}/cctrl_simple.txt", "w")
          out.puts(@promotion_settings.getProperty([h_alias, "cctrl_status_simple"]))
          out.close
        end
      
        out = File.open("#{diag_dir}/#{h_alias}/trepctl.json", "w")
        out.puts(@promotion_settings.getProperty([h_alias, "replicator_json_status"]))
        out.close
        
        out = File.open("#{diag_dir}/#{h_alias}/trepctl.txt", "w")
        config.getPropertyOr([REPL_SERVICES], {}).keys().sort().each{
          |rs_alias|
          if rs_alias == DEFAULTS
            next
          end
          out.puts(@promotion_settings.getProperty([h_alias, "replicator_status_#{rs_alias}"]))
        }
        out.close
      
        out = File.open("#{diag_dir}/#{h_alias}/thl.txt", "w")
        config.getPropertyOr([REPL_SERVICES], {}).keys().sort().each{
          |rs_alias|
          if rs_alias == DEFAULTS
            next
          end
          out.puts(@promotion_settings.getProperty([h_alias, "thl_info_#{rs_alias}"]))
        }
        out.close

        out = File.open("#{diag_dir}/#{h_alias}/thl_index.txt", "w")
        config.getPropertyOr([REPL_SERVICES], {}).keys().sort().each{
            |rs_alias|
          if rs_alias == DEFAULTS
            next
          end
          out.puts(@promotion_settings.getProperty([h_alias, "thl_index_#{rs_alias}"]))
        }
        out.close
        
        begin
          scp_download("#{config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/log/trepsvc.log", "#{diag_dir}/#{h_alias}/trepsvc.log.tmp", config.getProperty(HOST), config.getProperty(USERID))
          copy_log("#{diag_dir}/#{h_alias}/trepsvc.log.tmp", "#{diag_dir}/#{h_alias}/trepsvc.log", LOG_SIZE)
          FileUtils.rm("#{diag_dir}/#{h_alias}/trepsvc.log.tmp")
        rescue CommandError => ce
          exception(ce)
        rescue MessageError => me
          exception(me)
        end
        
        begin
          log = "#{config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/log/xtrabackup.log"
          if remote_file_exists?(log, config.getProperty(HOST), config.getProperty(USERID))
            scp_download(log, "#{diag_dir}/#{h_alias}/xtrabackup.log", config.getProperty(HOST), config.getProperty(USERID))
          end
        rescue CommandError => ce
          exception(ce)
        rescue MessageError => me
          exception(me)
        end
        
        begin
          log = "#{config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/log/mysqldump.log"
          if remote_file_exists?(log, config.getProperty(HOST), config.getProperty(USERID))
            scp_download(log, "#{diag_dir}/#{h_alias}/mysqldump.log", config.getProperty(HOST), config.getProperty(USERID))
          end
        rescue CommandError => ce
          exception(ce)
        rescue MessageError => me
          exception(me)
        end
        
        begin
          log = "#{config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/log/script.log"
          if remote_file_exists?(log, config.getProperty(HOST), config.getProperty(USERID))
            scp_download(log, "#{diag_dir}/#{h_alias}/script.log", config.getProperty(HOST), config.getProperty(USERID))
          end
        rescue CommandError => ce
          exception(ce)
        rescue MessageError => me
          exception(me)
        end

        begin
          log = "/home/#{config.getProperty(USERID)}/.cctrl_history"
          if remote_file_exists?(log, config.getProperty(HOST), config.getProperty(USERID))
            scp_download(log, "#{diag_dir}/#{h_alias}/cctrl_history.txt", config.getProperty(HOST), config.getProperty(USERID))
          end
        rescue CommandError => ce
          exception(ce)
        rescue MessageError => me
          exception(me)
        end
        
        if @promotion_settings.getProperty([h_alias, MANAGER_ENABLED]) == "true"
          begin
            scp_download("#{config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-manager/log/tmsvc.log", "#{diag_dir}/#{h_alias}/tmsvc.log.tmp", config.getProperty(HOST), config.getProperty(USERID))
            copy_log("#{diag_dir}/#{h_alias}/tmsvc.log.tmp", "#{diag_dir}/#{h_alias}/tmsvc.log", LOG_SIZE)
            FileUtils.rm("#{diag_dir}/#{h_alias}/tmsvc.log.tmp")
          rescue CommandError => ce
            exception(ce)
          rescue MessageError => me
            exception(me)
          end
        end
      end
      
      if @promotion_settings.getProperty([h_alias, CONNECTOR_ENABLED]) == "true"
        begin
          scp_download("#{config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-connector/log/connector.log", "#{diag_dir}/#{h_alias}/connector.log.tmp", config.getProperty(HOST), config.getProperty(USERID))
          copy_log("#{diag_dir}/#{h_alias}/connector.log.tmp", "#{diag_dir}/#{h_alias}/connector.log", LOG_SIZE)
          FileUtils.rm("#{diag_dir}/#{h_alias}/connector.log.tmp")
        rescue CommandError => ce
          exception(ce)
        rescue MessageError => me
          exception(me)
        end
      end

      begin
        df_output=ssh_result("df -hP| grep -v Filesystem", config.getProperty(HOST), config.getProperty(USERID)).split("\n")
        out = File.open("#{diag_dir}/#{h_alias}/os_info/df.txt", "w")
        df_output.each {|partition|
          out.puts(partition)
          partition_a=partition.split(" ")
          if partition_a[4] == '100%'
           error ("Partition #{partition_a[0]} on #{config.getProperty(HOST)} is full - Check and free disk space if required")
          end
        }
        out.close
      rescue CommandError => ce
      exception(ce)
      rescue MessageError => me
      exception(me)
      end



      #Run a ifconfig if it's available in the path
      begin
        ifconfig_path = ssh_result("which ifconfig 2>/dev/null", config.getProperty(HOST), config.getProperty(USERID))
        if ifconfig_path != ""
          ifconfig_output=ssh_result("ifconfig", config.getProperty(HOST), config.getProperty(USERID))
          out = File.open("#{diag_dir}/#{h_alias}/os_info/ifconfig.txt", "w")
          out.puts(ifconfig_output)
          out.close
        end
      rescue

      end


      #Run a netstat if it's available in the path
      begin
        ifconfig_path = ssh_result("which netstat 2>/dev/null", config.getProperty(HOST), config.getProperty(USERID))
        if ifconfig_path != ""
          ifconfig_output=ssh_result("netstat -nap", config.getProperty(HOST), config.getProperty(USERID))
          out = File.open("#{diag_dir}/#{h_alias}/os_info/netstat.txt", "w")
          out.puts(ifconfig_output)
          out.close
        end
      rescue

      end

      #Run a free -m  if it's available in the path
      begin
        free_path = ssh_result("which free 2>/dev/null", config.getProperty(HOST), config.getProperty(USERID))
        if free_path != ""
          free_output=ssh_result("free -m", config.getProperty(HOST), config.getProperty(USERID))
          out = File.open("#{diag_dir}/#{h_alias}/os_info/free.txt", "w")
          out.puts(free_output)
          out.close
        end
      rescue

      end

      #Run a java -version  if it's available in the path (note output goes to stderr so it needs the redirect)
      begin
        java_path = ssh_result("which java 2>/dev/null", config.getProperty(HOST), config.getProperty(USERID))
        if java_path != ""
          java_output=ssh_result("java -version 2>&1", config.getProperty(HOST), config.getProperty(USERID))
          out = File.open("#{diag_dir}/#{h_alias}/os_info/java_info.txt", "w")
          out.puts(java_output)
          out.close
        end
      rescue

      end

      #Run a ruby -v  if it's available in the path
      begin
        ruby_path = ssh_result("which ruby 2>/dev/null", config.getProperty(HOST), config.getProperty(USERID))
        if ruby_path != ""
          ruby_output=ssh_result("ruby -v", config.getProperty(HOST), config.getProperty(USERID))
          out = File.open("#{diag_dir}/#{h_alias}/os_info/ruby_info.txt", "w")
          out.puts(ruby_output)
          out.close
        end
      rescue

      end

    }
    
    require 'zip/zip'
    require 'find'
    
    @zip_file = "#{diag_dir}.zip"
    Zip::ZipFile.open(@zip_file, Zip::ZipFile::CREATE) do |zipfile|
      Find.find(diag_dir) do |path|
        entry = path.gsub(File.dirname(diag_dir) + "/", "")
        zipfile.add(entry, path)
      end
      zipfile.close 
    end
    FileUtils.rmtree(diag_dir)
  end
  
  # Copy specified log's last n bytes.
  def copy_log(src_path, dest_path, bytes)
    if File.exist?(src_path)
      fout = File.open(dest_path, 'w')

      File.open(src_path, "r") do |f|
        if bytes < File.size(src_path)
          f.seek(-bytes, IO::SEEK_END)
        end
        while (line = f.gets)
          fout.puts line
        end
      end

      fout.close
    end
  end
end

class ClusterDiagnosticCheck < ConfigureValidationCheck
  include CommitValidationCheck
  
  def set_vars
    @title = "Collect diagnostic information"
  end
  
  def validate
    c = Configurator.instance
    current_release_directory = @config.getProperty(CURRENT_RELEASE_DIRECTORY)
    cctrl_cmd = c.get_cctrl_path(current_release_directory, @config.getProperty(MGR_RMI_PORT))
    trepctl_cmd = c.get_trepctl_path(current_release_directory, @config.getProperty(REPL_RMI_PORT))
    thl_cmd = c.get_thl_path(current_release_directory)
    tpm_cmd = c.get_tpm_path(current_release_directory)
    
    begin
      output_property("manifest", cmd_result("cat #{current_release_directory}/.manifest.json"))
      output_property("tpm_reverse", cmd_result("#{tpm_cmd} reverse --public"))
      output_property("tpm_diff", cmd_result("#{tpm_cmd} query modified-files"))
      
      ["manager", "replicator", "connector"].each {
        |svc|
        svc_path = c.get_svc_path(svc, c.get_base_path())

        if c.svc_is_running?(svc_path)
          cmd_result("#{svc_path} dump", true)
        end
      }
      
      if c.svc_is_running?(c.get_svc_path("manager", c.get_base_path()))
        cmd_result("echo 'physical;*/*/manager/ServiceManager/diag' | #{cctrl_cmd} -expert", true)
        cmd_result("echo 'physical;*/*/router/RouterManager/diag' | #{cctrl_cmd} -expert", true)
        output_property("cctrl_status", cmd_result("echo 'ls -l' | #{cctrl_cmd} -expert", true))
        output_property("cctrl_status_simple", cmd_result("echo 'ls ' | #{cctrl_cmd} -expert", true))
      end
      
      if c.svc_is_running?(c.get_svc_path("replicator", c.get_base_path()))
        output_property("replicator_json_status", cmd_result("#{trepctl_cmd} services -full -json", true))
        
        @config.getPropertyOr([REPL_SERVICES], {}).keys().sort().each{
          |rs_alias|
          if rs_alias == DEFAULTS
            next
          end
          output_property("replicator_status_#{rs_alias}", cmd_result("#{trepctl_cmd} -service #{@config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_SERVICE])} status", true))
          output_property("thl_info_#{rs_alias}", cmd_result("#{thl_cmd} -service #{@config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_SERVICE])} info", true))
          output_property("thl_index_#{rs_alias}", cmd_result("#{thl_cmd} -service #{@config.getProperty([REPL_SERVICES, rs_alias, DEPLOYMENT_SERVICE])} index", true))
        }
      end
    rescue CommandError => ce
      exception(ce)
      error(ce.message)
    end
  end
  
  def enabled?
    super() && (@config.getProperty(HOST_ENABLE_REPLICATOR) == "true")
  end
end

class OldServicesRunningCheck < ConfigureValidationCheck
  include CommitValidationCheck
  
  def set_vars
    @title = "Check for Tungsten services running outside of the current install directory"
  end
  
  def validate
    current_release_directory = @config.getProperty(CURRENT_RELEASE_DIRECTORY)
    if File.exists?(current_release_directory)
      current_release_target_dir = File.readlink(current_release_directory)
    else
      return
    end
    
    current_pid_files = cmd_result("find #{@config.getProperty(HOME_DIRECTORY)}/#{RELEASES_DIRECTORY_NAME} -name *.pid").split("\n")
    allowed_pid_files = []
    if @config.getProperty(HOST_ENABLE_REPLICATOR) == "true"
      allowed_pid_files << "#{current_release_target_dir}/tungsten-replicator/var/treplicator.pid"
    end
    if @config.getProperty(HOST_ENABLE_MANAGER) == "true"
      allowed_pid_files << "#{current_release_target_dir}/tungsten-manager/var/tmanager.pid"
    end
    if @config.getProperty(HOST_ENABLE_CONNECTOR) == "true"
      allowed_pid_files << "#{current_release_target_dir}/tungsten-connector/var/tconnector.pid"
    end
    
    extra_pid_files = current_pid_files - allowed_pid_files
    extra_pid_files.each{
      |p|
      match = p.match(/([\/a-zA-Z0-9\-\._]*)\/tungsten-([a-zA-Z]*)\//)
      if match
        error("There is an extra #{match[2]} running in #{match[1]}")
        help("shell> #{match[1]}/tungsten-#{match[2]}/bin/#{match[2]} stop; #{current_release_directory}/tungsten-#{match[2]}/bin/#{match[2]} start")
      end
    }
  end
end