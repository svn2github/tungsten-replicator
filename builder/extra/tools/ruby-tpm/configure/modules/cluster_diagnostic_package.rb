module ClusterDiagnosticPackage
  LOG_SIZE = 2*1024*1024
  
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
      h_alias = config.getProperty(DEPLOYMENT_HOST)
      FileUtils.mkdir_p("#{diag_dir}/#{h_alias}")
      
      if @promotion_settings.getProperty([h_alias, REPLICATOR_ENABLED]) == "true"
        out = File.open("#{diag_dir}/#{h_alias}/cctrl.txt", "w")
        out.puts(@promotion_settings.getProperty([h_alias, "cctrl_status"]))
        out.close
      
        out = File.open("#{diag_dir}/#{h_alias}/trepctl.txt", "w")
        out.puts(@promotion_settings.getProperty([h_alias, "replicator_status"]))
        out.close
      
        out = File.open("#{diag_dir}/#{h_alias}/thl.txt", "w")
        out.puts(@promotion_settings.getProperty([h_alias, "thl_info"]))
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
          scp_download("#{config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-manager/log/tmsvc.log", "#{diag_dir}/#{h_alias}/tmsvc.log.tmp", config.getProperty(HOST), config.getProperty(USERID))
          copy_log("#{diag_dir}/#{h_alias}/tmsvc.log.tmp", "#{diag_dir}/#{h_alias}/tmsvc.log", LOG_SIZE)
          FileUtils.rm("#{diag_dir}/#{h_alias}/tmsvc.log.tmp")
        rescue CommandError => ce
          exception(ce)
        rescue MessageError => me
          exception(me)
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
      
      df_output=ssh_result("df -hP| grep -v Filesystem", config.getProperty(HOST), config.getProperty(USERID))
      df_output.each {|partition|
        partition_a=partition.split(" ")
        if partition_a[4] == '100%'
         error ("Partition #{partition_a[0]} on #{config.getProperty(HOST)} is full - Check and free disk space if required")
        end
      }
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
    
    begin
      ["manager", "replicator", "connector"].each {
        |svc|
        svc_path = c.get_svc_path(svc, c.get_base_path())

        if c.svc_is_running?(svc_path)
          cmd_result("#{svc_path} dump", true)
        end
      }
      
      cmd_result("echo 'physical;*/*/manager/ServiceManager/diag' | #{cctrl_cmd} -expert", true)
      cmd_result("echo 'physical;*/*/router/RouterManager/diag' | #{cctrl_cmd} -expert", true)
      output_property("cctrl_status", cmd_result("echo 'ls -l' | #{cctrl_cmd} -expert", true))
      output_property("replicator_status", cmd_result("#{trepctl_cmd} status", true))
      output_property("thl_info", cmd_result("#{thl_cmd} info", true))
    rescue CommandError => ce
      exception(ce)
      error(ce.message)
    end
  end
  
  def enabled?
    super() && (@config.getProperty(HOST_ENABLE_REPLICATOR) == "true")
  end
end