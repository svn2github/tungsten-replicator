module ConfigureDeploymentStepDeployment
  def get_methods
    [
      ConfigureDeploymentMethod.new("create_release", 0, -40),
      ConfigureCommitmentMethod.new("commit_release")
    ]
  end
  module_function :get_methods
  
  def create_release
    prepare_dir = get_deployment_basedir()
    mkdir_if_absent(@config.getProperty(LOGS_DIRECTORY))
    mkdir_if_absent(@config.getProperty(RELEASES_DIRECTORY))
    
    copy_release = true
    if @config.getProperty(HOME_DIRECTORY) == Configurator.instance.get_base_path()
      copy_release = false
    end
    if "#{@config.getProperty(HOME_DIRECTORY)}/#{RELEASES_DIRECTORY_NAME}/#{Configurator.instance.get_basename()}" == Configurator.instance.get_base_path()
      copy_release = false
    end
    if File.exists?(prepare_dir)
      copy_release = false
    end

    if copy_release == true
      FileUtils.rmtree(File.dirname(prepare_dir))
      mkdir_if_absent(File.dirname(prepare_dir))
      
      if @config.getProperty(DEPLOY_CURRENT_PACKAGE) == "true"
        package_path = Configurator.instance.get_package_path()

        debug("Copy #{package_path} to #{prepare_dir}")
        FileUtils.cp_r(package_path, prepare_dir)
      else
        destination = File.dirname(prepare_dir)
        
        debug("Download and unpack #{@config.getProperty(DEPLOY_PACKAGE_URI)}")
        uri = URI::parse(@config.getProperty(DEPLOY_PACKAGE_URI))

        if uri.scheme == "http" || uri.scheme == "https"
          unless @config.getProperty(DEPLOY_PACKAGE_URI) =~ /.tar.gz/
            raise "Only files ending in .tar.gz may be fetched using #{uri.scheme.upcase}"
          end

          package_basename = File.basename(@config.getProperty(DEPLOY_PACKAGE_URI), ".tar.gz")
          unless (File.exists?("#{@config.getProperty(TEMP_DIRECTORY)}/#{package_basename}.tar.gz"))
            cmd_result("cd #{@config.getProperty(TEMP_DIRECTORY)}; wget --no-check-certificate #{@config.getProperty(DEPLOY_PACKAGE_URI)}")
          else
            debug("Using the package already downloaded to #{@config.getProperty(TEMP_DIRECTORY)}/#{package_basename}.tar.gz")
          end

          cmd_result("cd #{destination}; tar zxf #{@config.getProperty(TEMP_DIRECTORY)}/#{package_basename}.tar.gz")
        elsif uri.scheme == "file"
          rsync_cmd = ["rsync"]
      
          unless uri.port
            rsync_cmd << "-aze ssh --delete"
          else
            rsync_cmd << "-aze \"ssh --delete -p #{uri.port}\""
          end
      
          if uri.host != "localhost"
            unless uri.userinfo
              rsync_cmd << "#{uri.host}:#{uri.path}"
            else
              rsync_cmd << "#{uri.userinfo}@#{uri.host}:#{uri.path}"
            end

            rsync_cmd << @config.getProperty(TEMP_DIRECTORY)
        
            cmd_result(rsync_cmd.join(" "))
          else
            unless File.dirname(uri.path) == @config.getProperty(TEMP_DIRECTORY)
              FileUtils.cp(uri.path, @config.getProperty(TEMP_DIRECTORY))
            end
          end
        
          package_basename = File.basename(uri.path)
          if package_basename =~ /.tar.gz$/
            package_basename = File.basename(package_basename, ".tar.gz")
          
            cmd_result("cd #{destination}; tar zxf #{@config.getProperty(TEMP_DIRECTORY)}/#{package_basename}.tar.gz")
          elsif package_basename =~ /.tar$/
            package_basename = File.basename(package_basename, ".tar")
          
            cmd_result("cd #{destination}; tar xf #{@config.getProperty(TEMP_DIRECTORY)}/#{package_basename}.tar")
          elsif File.directory?("#{@config.getProperty(TEMP_DIRECTORY)}/#{package_basename}")
            FileUtils.cp_r(@config.getProperty(TEMP_DIRECTORY) + '/' + package_basename, prepare_dir)
          end
        end
      end
      
      commit_script = "#{File.dirname(prepare_dir)}/commit.sh"
      out = File.open(commit_script, "w")
      out.puts("#!/bin/sh")
      out.puts("PREPARE_DIR=#{prepare_dir}")
      out.puts("TARGET_DIR=#{@config.getProperty(TARGET_DIRECTORY)}")
      out.puts("if [ ! -d $PREPARE_DIR ]")
      out.puts("then")
      out.puts('  echo "$PREPARE_DIR is not present, it may have been promoted already."')
      out.puts("  exit 1")
      out.puts("fi")
      out.puts("mv $PREPARE_DIR $TARGET_DIR")
      out.puts("$TARGET_DIR/tools/tpm promote")
      out.close
      File.chmod(0755, commit_script)
      info "GENERATED FILE: " + commit_script
    end
    
    out = File.open(@config.getProperty(DIRECTORY_LOCK_FILE), "w")
    out.puts(@config.getProperty(HOME_DIRECTORY))
    out.close()
    File.chmod(0644, @config.getProperty(DIRECTORY_LOCK_FILE))
    
    mkdir_if_absent("#{prepare_dir}/cluster-home/conf/cluster/" + @config.getProperty(DATASERVICENAME) + "/datasource")
    mkdir_if_absent("#{prepare_dir}/cluster-home/conf/cluster/" + @config.getProperty(DATASERVICENAME) + "/service")
    mkdir_if_absent("#{prepare_dir}/cluster-home/conf/cluster/" + @config.getProperty(DATASERVICENAME) + "/extension")
    if is_replicator?()
      FileUtils.cp("#{get_deployment_basedir()}/tungsten-replicator/conf/replicator.service.properties", 
        "#{get_deployment_basedir()}/cluster-home/conf/cluster/#{@config.getProperty(DATASERVICENAME)}/service/replicator.properties")
    end
    
    Configurator.instance.write_header("Building the Tungsten home directory")
    mkdir_if_absent("#{@config.getProperty(HOME_DIRECTORY)}/share")
    
    DeploymentFiles.prompts.each{
      |p|
      if @config.getProperty(p[:local]) != nil && File.exist?(@config.getProperty(p[:local]))
        target = @config.getTemplateValue(p[:local])
        mkdir_if_absent(File.dirname(target))
    		FileUtils.cp(@config.getProperty(p[:local]), target)
    	end
    }
    
    # Create share/env.sh script.
    script = "#{@config.getProperty(HOME_DIRECTORY)}/#{CONTINUENT_ENVIRONMENT_SCRIPT}"
    debug("Generate environment at #{script}")
    transformer = Transformer.new(
		  "#{get_deployment_basedir()}/cluster-home/samples/conf/env.sh.tpl",
			script, "#")
    transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
	  transformer.transform_values(method(:transform_values))
    transformer.output
    watch_file(transformer.get_filename())
    FileUtils.chmod(0755, script)
    
    # Write the cluster-home/conf/security.properties file
    transformer = Transformer.new(
		  "#{get_deployment_basedir()}/cluster-home/samples/conf/security.properties.tpl",
			"#{get_deployment_basedir()}/cluster-home/conf/security.properties", "#")
    transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
	  transformer.transform_values(method(:transform_values))
    transformer.output
    watch_file(transformer.get_filename())
    
    if Configurator.instance.is_enterprise?()
      debug("Write INSTALLED cookbook scripts")
      transformer = Transformer.new(
        "#{get_deployment_basedir()}/cookbook/samples/INSTALLED_USER_VALUES.tpl",
        "#{get_deployment_basedir()}/cookbook/INSTALLED_USER_VALUES.sh", "#")
      transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
  	  transformer.transform_values(method(:transform_values))
	  
  	  dsid=1
  	  dsids={}
      @config.getPropertyOr(DATASERVICES, []).each_key{
        |ds_alias|
        if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
          ds_name=@config.getProperty([DATASERVICES, ds_alias, DATASERVICENAME])
          transformer << "export COMPOSITE_DS=#{ds_name}"
          dsids[ds_name] = "configure $COMPOSITE_DS"
        else
          ds_name=@config.getProperty([DATASERVICES, ds_alias, DATASERVICENAME])
          transformer << "export DS_NAME#{dsid}=#{ds_name}"
          transformer << "export MASTER#{dsid}=#{@config.getProperty([DATASERVICES, ds_alias, DATASERVICE_MASTER_MEMBER])}"
          transformer << "export CONNECTORS#{dsid}=#{@config.getProperty([DATASERVICES, ds_alias, DATASERVICE_CONNECTORS])}"
          
          dsids[ds_name] = "configure $DS_NAME#{dsid}"
          dsid = dsid+1
        end
      }
	  
      transformer.output
      watch_file(transformer.get_filename())
      FileUtils.chmod(0755, script)
    
      File.open("#{get_deployment_basedir()}/cookbook/INSTALLED.tmpl", "w") {
        |f|
        f.puts <<EOF
  ##################################
  # DO NOT MODIFY THIS FILE
  ##################################
  # Loads environment variables to fill in the cookbook
  # . cookbook/USER_VALUES.sh

EOF
        rec = ReverseEngineerCommand.new(@config)
        commands = rec.build_commands(@config)
        
        # Update the tpm configure commands to use environment variables
        commands.map!{
          |cmd|
          cmd.gsub(/configure ([a-zA-Z0-9_]+)/){
            |match|
            if dsids.has_key?($1)
              dsids[$1]
            else
              "configure #{$1}"
            end
          }
        }
        f.puts(commands.join("\n"))
      }
      watch_file("#{get_deployment_basedir()}/cookbook/INSTALLED.tmpl")
    end

    config_file = prepare_dir + '/' + Configurator::HOST_CONFIG
    debug("Write #{config_file}")

    host_config = @config.dup()
    ph = ConfigurePromptHandler.new(host_config)
    ph.prepare_saved_server_config()
    
    FileUtils.rm(Dir.glob("#{prepare_dir}/#{Configurator::DATASERVICE_CONFIG}*"))
    
    trigger_event(:deploy_config_files, host_config)
    
    host_config.store(config_file)
  end
  
  def commit_release
    prepare_dir = @config.getProperty(PREPARE_DIRECTORY)
    target_dir = @config.getProperty(TARGET_DIRECTORY)
    
    if @config.getProperty(PROFILE_SCRIPT) != ""
      profile_path = File.expand_path(@config.getProperty(PROFILE_SCRIPT), @config.getProperty(HOME_DIRECTORY))

      if File.exist?(profile_path)
        matching_lines = cmd_result("grep 'Tungsten Environment' #{profile_path} | wc -l")
        case matching_lines.to_i()
        when 2
          debug("Tungsten env.sh is already included in #{profile_path}")
        when 0
          begin
            f = File.open(profile_path, "a")
            f.puts("")
            f.puts("# Begin Tungsten Environment")
            f.puts("# Include the Tungsten variables")
            f.puts("# Anything in this section may be changed during the next operation")
            f.puts("if [ -f \"#{@config.getProperty(HOME_DIRECTORY)}/share/env.sh\" ]; then")
            f.puts("    . \"#{@config.getProperty(HOME_DIRECTORY)}/share/env.sh\"")
            f.puts("fi")
            f.puts("# End Tungsten Environment")
          ensure
            f.close()
          end
        else
          error("Unable to add the Tungsten environment to #{profile_path}.  Remove any lines from '# Begin Tungsten Environment' to '# End Tungsten Environment'.")
        end
      else
        error("Unable to add the Tungsten environment to #{profile_path} because the file does not exist.")
      end
    end
    
    current_release_directory = @config.getProperty(CURRENT_RELEASE_DIRECTORY)
    if File.exists?(current_release_directory)
      current_release_target_dir = File.readlink(current_release_directory)
    else
      current_release_target_dir = nil
    end
    
    unless target_dir == current_release_target_dir
      if File.exists?(current_release_directory)    
        if current_release_target_dir
          manager_dynamic_properties = current_release_target_dir + '/tungsten-manager/conf/dynamic.properties'
          if File.exists?(manager_dynamic_properties)
            info("Copy the previous manager dynamic properties")
            FileUtils.mv(manager_dynamic_properties, prepare_dir + '/tungsten-manager/conf')
          end
          
          Dir.glob("#{current_release_target_dir}/tungsten-replicator/conf/dynamic-*.properties") {
            |replicator_dynamic_properties|
            info("Copy the previous replicator dynamic properties - #{File.basename(replicator_dynamic_properties)}")
            FileUtils.mv(replicator_dynamic_properties, prepare_dir + '/tungsten-replicator/conf')
          }
          
          cluster_home_conf = current_release_target_dir + '/cluster-home/conf'
          if File.exists?(cluster_home_conf)
            info("Copy the previous cluster conf properties")
            
            cluster_home_conf_cluster = current_release_target_dir + '/cluster-home/conf/cluster'
            if File.exists?(cluster_home_conf_cluster)
              cmd_result("rsync -Ca --exclude=service/* --exclude=extension/* #{cluster_home_conf_cluster} #{prepare_dir}/cluster-home/conf")
            end
            
            dataservices_properties = current_release_target_dir + '/cluster-home/conf/dataservices.properties'
            if File.exists?(dataservices_properties)
              FileUtils.cp(dataservices_properties, prepare_dir + '/cluster-home/conf')
            end
            
            statemap_properties = current_release_target_dir + '/cluster-home/conf/statemap.properties'
            if File.exists?(statemap_properties) && @config.getProperty(SKIP_STATEMAP) == "false"
              FileUtils.cp(statemap_properties, prepare_dir + '/cluster-home/conf')
            end
          end
          
          connector_user_map = current_release_target_dir + '/tungsten-connector/conf/user.map'
          if File.exists?(connector_user_map) && @config.getProperty(CONN_DELETE_USER_MAP) == "false"
            info("Copy the previous connector user map")
            FileUtils.cp(connector_user_map, prepare_dir + '/tungsten-connector/conf')
          end
          
          FileUtils.touch(current_release_target_dir)
        end
      end
      
      if is_manager?()
        @config.getPropertyOr(DATASERVICES, {}).keys().each{
          |comp_ds_alias|

          if comp_ds_alias == DEFAULTS
            next
          end

          if @config.getProperty([DATASERVICES, comp_ds_alias, DATASERVICE_IS_COMPOSITE]) == "false"
            next
          end

          unless include_dataservice?(comp_ds_alias)
            next
          end

          mkdir_if_absent("#{prepare_dir}/cluster-home/conf/cluster/#{comp_ds_alias}/service")
          mkdir_if_absent("#{prepare_dir}/cluster-home/conf/cluster/#{comp_ds_alias}/datasource")
          mkdir_if_absent("#{prepare_dir}/cluster-home/conf/cluster/#{comp_ds_alias}/extension")

          @config.getProperty([DATASERVICES, comp_ds_alias, DATASERVICE_COMPOSITE_DATASOURCES]).to_s().split(",").each{
            |ds_alias|
            
            path = "#{prepare_dir}/cluster-home/conf/cluster/#{comp_ds_alias}/datasource/#{ds_alias}.properties"
            unless File.exist?(path)
              if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_RELAY_SOURCE]).to_s() != ""
                ds_role = "slave"
              else
                ds_role = "master"
              end
              
              File.open(path, "w") {
                |f|
                f.puts "
appliedLatency=-1.0
precedence=1
name=#{ds_alias}
state=OFFLINE
url=jdbc\:t-router\://#{ds_alias}/${DBNAME}
alertMessage=
isAvailable=true
role=#{ds_role}
isComposite=true
alertStatus=OK
alertTime=#{Time.now().strftime("%s000")}
dataServiceName=#{comp_ds_alias}
vendor=continuent
driver=com.continuent.tungsten.router.jdbc.TSRDriver
host=#{ds_alias}"
              }
            end
          }
        }
      end
      
      unless target_dir == prepare_dir
        debug("Move the prepared directory to #{target_dir}")
        FileUtils.mv(prepare_dir, target_dir)
      end
      
      debug("Create symlink to #{target_dir}")
      FileUtils.rm_f(current_release_directory)
      FileUtils.ln_s(target_dir, current_release_directory)
    end
    
    if File.exists?(@config.getProperty(CONFIG_DIRECTORY))
      FileUtils.rmtree(@config.getProperty(CONFIG_DIRECTORY))
    end
    if File.exists?(@config.getProperty(HOME_DIRECTORY) + "/configs")
      FileUtils.rmtree(@config.getProperty(HOME_DIRECTORY) + "/configs")
    end
    if File.exists?(@config.getProperty(HOME_DIRECTORY) + "/service-logs")
      FileUtils.rmtree(@config.getProperty(HOME_DIRECTORY) + "/service-logs")
    end
    FileUtils.touch(target_dir)
    
    FileUtils.cp(current_release_directory + '/' + Configurator::HOST_CONFIG, current_release_directory + '/.' + Configurator::HOST_CONFIG + '.orig')
    
    if is_manager?() || is_connector?()
      write_dataservices_properties()
      write_router_properties()
      write_policymgr_properties()
    end
  end
  
  def write_dataservices_properties
    dataservices_file = "#{@config.getProperty(TARGET_DIRECTORY)}/cluster-home/conf/dataservices.properties"
    dataservices = {}
    if (File.exists?(dataservices_file))
      File.open(dataservices_file, "r") {
        |f|
        f.readlines().each{
          |line|
          parts = line.split("=")
          if parts.size() == 2
            dataservices[parts[0]] = parts[1]
          end
        }
      }
    end
    
    @config.getPropertyOr(DATASERVICES, {}).each_key{
      |ds_alias|
      if ds_alias == DEFAULTS
        next
      end
      
      topology = Topology.build(ds_alias, @config)
      unless topology.use_management?()
        next
      end
      
      if @config.getProperty([DATASERVICES, ds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
        next
      end
      unless include_dataservice?(ds_alias)
        next
      end
      unless (
          @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_MEMBERS], "").split(',').include?(@config.getProperty(get_host_key(HOST))) ||
          @config.getPropertyOr([DATASERVICES, ds_alias, DATASERVICE_CONNECTORS], "").split(',').include?(@config.getProperty(get_host_key(HOST)))
        )
        next
      end

      dataservices[@config.getTemplateValue([DATASERVICES, ds_alias, DATASERVICENAME])] = @config.getTemplateValue([DATASERVICES, ds_alias, DATASERVICE_MEMBERS])
      @config.getPropertyOr(DATASERVICES, {}).each_key{
        |cds_alias|
        unless @config.getProperty([DATASERVICES, cds_alias, DATASERVICE_IS_COMPOSITE]) == "true"
          next
        end
        
        composite_members = @config.getPropertyOr([DATASERVICES, cds_alias, DATASERVICE_COMPOSITE_DATASOURCES], "").split(",")
        if composite_members.include?(ds_alias)
          composite_members.each{
            |cm_alias|
            dataservices[@config.getTemplateValue([DATASERVICES, cm_alias, DATASERVICENAME])] = @config.getTemplateValue([DATASERVICES, cm_alias, DATASERVICE_MEMBERS])
          }
        end
      }
    }

    out = File.open(dataservices_file, "w")
    dataservices.each{
      |ds_alias,managers|
      out.puts "#{ds_alias}=#{managers}"
    }
    out.close
    info "GENERATED FILE: " + dataservices_file
  end

  def write_router_properties
    transformer = Transformer.new(
          "#{@config.getProperty(TARGET_DIRECTORY)}/tungsten-connector/samples/conf/router.properties.tpl",
            "#{@config.getProperty(TARGET_DIRECTORY)}/cluster-home/conf/router.properties", "#")
      transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
    transformer.transform_values(method(:transform_values))
    
    transformer.output
    watch_file(transformer.get_filename())
  end
  
  def write_policymgr_properties
    # Write the policymgr.properties file.
    transformer = Transformer.new(
      "#{@config.getProperty(TARGET_DIRECTORY)}/tungsten-connector/samples/conf/policymgr.properties.tpl",
      "#{@config.getProperty(TARGET_DIRECTORY)}/cluster-home/conf/policymgr.properties", "# ")
    transformer.set_fixed_properties(@config.getTemplateValue(get_host_key(FIXED_PROPERTY_STRINGS)))
    transformer.transform_values(method(:transform_values))

    transformer.transform { |line|
      if line =~ /^notifierMonitorClass/ then
        "notifierMonitorClass=com.continuent.tungsten.commons.patterns.notification.adaptor.MonitorNotifierGroupCommAdaptor"
      else
        line
      end
    }
    watch_file(transformer.get_filename())
  end
end