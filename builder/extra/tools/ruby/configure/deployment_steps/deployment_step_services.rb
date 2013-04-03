module ConfigureDeploymentStepServices
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("apply_config_services", ConfigureDeployment::FINAL_STEP_WEIGHT),
    ]
  end
  module_function :get_deployment_methods
  
  # Set up files and perform other configuration for services.
  def apply_config_services
    Configurator.instance.write_header "Performing services configuration"

    config_wrapper()
    write_deployall()
    write_undeployall()
    write_startall()
    write_stopall()
    
    if @config.getProperty(SVC_INSTALL) == "true" then
      info("Installing services")
      installed = cmd_result("#{get_root_prefix()} #{get_deployment_basedir()}/cluster-home/bin/deployall")
      info(installed)
    end
    
    if @config.getProperty(SVC_REPORT) == "true" || @config.getProperty(SVC_START) == "true"
      self.trigger_event(:before_services_start)
      
      @services.each {
        |svc| 
        unless svc_is_running?("#{get_deployment_basedir()}/#{svc}")
          self.trigger_event(:before_service_start, svc)
          cmd_result(get_svc_command("#{get_deployment_basedir()}/#{svc} start"))
          self.trigger_event(:after_service_start, svc)
        else
          self.trigger_event(:before_service_stop, svc)
          cmd_result(get_svc_command("#{get_deployment_basedir()}/#{svc} stop"))
          self.trigger_event(:after_service_stop, svc)

          self.trigger_event(:before_service_start, svc)
          cmd_result(get_svc_command("#{get_deployment_basedir()}/#{svc} start"))
          self.trigger_event(:after_service_start, svc)
        end
      }
      
      self.trigger_event(:after_services_start)
    end
    
    if @config.getProperty(SVC_REPORT) == "true"
      output("Getting services list")
      
      begin
        Timeout::timeout(30) {
          while true
            begin
              services = cmd_result("#{get_deployment_basedir()}/tungsten-replicator/bin/trepctl -port #{@config.getProperty(REPL_RMI_PORT)} services")
              output(services)
              break
            rescue CommandError
            end
          end
        }
      rescue Timeout::Error
        warning("Unable to retrieve the list of services for the replicator.  Review the logs to see if there is an issue.")
      end
    end
    
    if svc_is_running?("#{get_deployment_basedir()}/tungsten-replicator/bin/replicator")
      begin
        error_lines = cmd_result("#{get_trepctl_cmd()} services | grep ERROR | wc -l")
        if error_lines.to_i() > 0
          error("At least one replication service has experienced an error")
        end
      rescue CommandError
        error("Unable to check if the replication services are working properly")
      end
    end
  end
  
  def config_wrapper
    # Patch for Ubuntu 64-bit start-up problem.
    if Configurator.instance.distro?() == OS_DISTRO_DEBIAN && Configurator.instance.arch?() == OS_ARCH_64
      wrapper_file = "#{get_deployment_basedir()}/cluster-home/bin/wrapper-linux-x86-32"
      if File.exist?(wrapper_file)
        FileUtils.rm("#{get_deployment_basedir()}/cluster-home/bin/wrapper-linux-x86-32")
      end
    end
  end
  
  def write_startall
    # Create startall script.
    script = "#{get_deployment_basedir()}/cluster-home/bin/startall"
    out = File.open(script, "w")
    out.puts "#!/bin/bash"
    out.puts "# Start all services using local service scripts"
    out.puts "THOME=`dirname $0`/../.."
    out.puts "cd $THOME"
    @services.each { |svc| out.puts get_svc_command(svc + " start") }
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    info "GENERATED FILE: " + script
  end

  def write_stopall
    # Create stopall script.
    script = "#{get_deployment_basedir()}/cluster-home/bin/stopall"
    out = File.open(script, "w")
    out.puts "#!/bin/bash"
    out.puts "# Stop all services using local service scripts"
    out.puts "THOME=`dirname $0`/../.."
    out.puts "cd $THOME"
    @services.reverse_each { |svc| out.puts get_svc_command(svc + " stop") }
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    info "GENERATED FILE: " + script
  end

  def write_deployall
    # Create deployall script.
    if Configurator.instance.can_install_services_on_os?()
      script = "#{get_deployment_basedir()}/cluster-home/bin/deployall"
      out = File.open(script, "w")
      out.puts "#!/bin/bash"
      out.puts "# Install services into /etc directories"
      out.puts "THOME=`dirname $0`/../.."
      out.puts "cd $THOME"
      @services.each { |svc|
        svcname = File.basename svc
        out.puts get_svc_command("ln -fs $PWD/" + svc + " /etc/init.d/t" + svcname)
        if Configurator.instance.distro?() == OS_DISTRO_REDHAT
          out.puts get_svc_command("/sbin/chkconfig --add t" + svcname)
        elsif Configurator.instance.distro?() == OS_DISTRO_DEBIAN
          out.puts get_svc_command("update-rc.d t" + svcname + " defaults")
        end
      }
      out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
      out.chmod(0755)
      out.close
      info "GENERATED FILE: " + script
    end
  end

  def write_undeployall
    # Create undeployall script.
    if Configurator.instance.can_install_services_on_os?()
      script = "#{get_deployment_basedir()}/cluster-home/bin/undeployall"
      out = File.open(script, "w")
      out.puts "#!/bin/bash"
      out.puts "# Remove services from /etc directories"
      out.puts "THOME=`dirname $0`/../.."
      out.puts "cd $THOME"
      @services.each { |svc|
        svcname = File.basename svc
        if Configurator.instance.distro?() == OS_DISTRO_REDHAT
          out.puts get_svc_command("/sbin/chkconfig --del t" + svcname)
          out.puts get_svc_command("rm -f /etc/init.d/t" + svcname)
        elsif Configurator.instance.distro?() == OS_DISTRO_DEBIAN
          out.puts get_svc_command("rm -f /etc/init.d/t" + svcname)
          out.puts get_svc_command("update-rc.d -f  t" + svcname + " remove")
        end
      }
      out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
      out.chmod(0755)
      out.close
      info "GENERATED FILE: " + script
    end
  end
end