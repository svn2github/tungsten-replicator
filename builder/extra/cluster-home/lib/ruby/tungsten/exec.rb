require "open4"
require "resolv"
require "ifconfig"

class TungstenUtil
  # Run the {command} and return a string of STDOUT
  def cmd_result(command, ignore_fail = false)
    errors = nil
    result = nil
    
    debug("Execute `#{command}`")
    status = Open4::popen4("export LANG=en_US; #{command}") do |pid, stdin, stdout, stderr|
      stdin.close 
      
      result = stdout.read().strip()
      errors = stderr.read().strip()
    end
    
    rc = status.exitstatus
    if errors == ""
      errors = "No Errors"
    else
      errors = "Errors: #{errors}"
    end

    debug("RC: #{rc}, Result: #{result}, #{errors}")
    if rc != 0 && ! ignore_fail
      raise CommandError.new(command, rc, result)
    end

    return result
  end
  
  # Run the {command} and run {&block} for each line of STDOUT
  def cmd_stdout(command, ignore_fail = false, &block)
    errors = nil
    result = ""
    
    debug("Execute `#{command}`")
    status = Open4::popen4("export LANG=en_US; #{command}") do |pid, stdin, stdout, stderr|
      stdin.close 
      
      while data = stdout.gets()
        result+=data
        block.call(data)
      end
      errors = stderr.read().strip()
    end
    
    rc = status.exitstatus
    if errors == ""
      errors = "No Errors"
    else
      errors = "Errors: #{errors}"
    end

    debug("RC: #{rc}, Result: #{result}, #{errors}")
    if rc != 0 && ! ignore_fail
      raise CommandError.new(command, rc, result)
    end

    return
  end
  
  # Run the {command} and run {&block} for each line of STDERR
  def cmd_stderr(command, ignore_fail = false, &block)
    errors = ""
    result = nil
    
    debug("Execute `#{command}`")
    status = Open4::popen4("export LANG=en_US; #{command}") do |pid, stdin, stdout, stderr|
      stdin.close 
      
      while data = stderr.gets()
        errors+=data
        block.call(data)
      end
      result = stdout.read().strip()
    end
    
    rc = status.exitstatus
    if errors == ""
      errors = "No Errors"
    else
      errors = "Errors: #{errors}"
    end

    debug("RC: #{rc}, Result: #{result}, #{errors}")
    if rc != 0 && ! ignore_fail
      raise CommandError.new(command, rc, result)
    end

    return
  end
  
  # Run the {command} on {host} as {user}
  # Return a string of STDOUT
  def ssh_result(command, host, user)
    if host == DEFAULTS
      debug("Unable to run '#{command}' because '#{host}' is not valid")
      raise RemoteCommandError.new(user, host, command, nil, '')
    end

    # Run the command outside of SSH if possible
    if is_localhost?(host) && 
        user == whoami()
      return cmd_result(command)
    end

    unless defined?(Net::SSH)
      begin
        require "openssl"
      rescue LoadError
        raise("Unable to find the Ruby openssl library. Try installing the openssl package for your version of Ruby (libopenssl-ruby#{RUBY_VERSION[0,3]}).")
      end
      require 'net/ssh'
    end
    
    ssh_user = get_ssh_user(user)
    if user != ssh_user
      debug("SSH user changed to #{ssh_user}")
      command = command.tr('"', '\"')
      command = "echo \"#{command}\" | sudo -u #{user} -i"
    end

    debug("Execute `#{command}` on #{host} as #{user}")
    result = ""
    rc = nil
    exit_signal=nil

    connection_error = "Net::SSH was unable to connect to #{host} as #{ssh_user}.  Check that #{host} is online, #{ssh_user} exists and your SSH private keyfile or ssh-agent settings. Try adding --net-ssh-option=port=<SSH port number> if you are using an SSH port other than 22.  Review https://docs.continuent.com/wiki/display/TEDOC/Unable+to+use+the+tpm+command+over+SSH for more help on diagnosing SSH problems."
    begin
      Net::SSH.start(host, ssh_user, get_ssh_options()) {
        |ssh|
        stdout_data = ""
        stderr_data = ""

        ssh.open_channel do |channel|
          channel.exec(". /etc/profile; #{ssh_init_profile_script()} export LANG=en_US; export LC_ALL=\"en_US.UTF-8\"; #{command}") do |ch, success|
            channel.on_data do |ch,data|
              stdout_data+=data
            end

            channel.on_extended_data do |ch,type,data|
              data = data.chomp
              log(data) unless data == ""
            end

            channel.on_request("exit-status") do |ch,data|
              rc = data.read_long
            end

            channel.on_request("exit-signal") do |ch, data|
              exit_signal = data.read_long
            end
          end
        end
        ssh.loop
        result = stdout_data.to_s.chomp
      }
    rescue Errno::ENOENT => ee
      raise MessageError.new("Net::SSH was unable to find a private key to use for SSH authenticaton. Try creating a private keyfile or setting up ssh-agent.")
    rescue OpenSSL::PKey::RSAError
      raise MessageError.new(connection_error)
    rescue Net::SSH::AuthenticationFailed
      raise MessageError.new(connection_error)
    rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH, Errno::EHOSTDOWN
      raise MessageError.new(connection_error)
    rescue Timeout::Error
      raise MessageError.new(connection_error)
    rescue NotImplementedError => nie
      raise MessageError.new(nie.message + ". Try modifying your ~/.ssh/config file to define values for Cipher and Ciphers that do not include this algorithm.  The supported encryption algorithms are #{Net::SSH::Transport::CipherFactory::SSH_TO_OSSL.keys().delete_if{|e| e == "none"}.join(", ")}.")
    end

    if rc != 0
      raise RemoteCommandError.new(user, host, command, rc, result)
    else
      debug("RC: #{rc}, Result: #{result}")
    end

    return result
  end
  
  def get_ssh_options
    @ssh_options
  end
  
  def get_ssh_user(user = nil)
    ssh_options = get_ssh_options
    if ssh_options.has_key?(:user) && ssh_options[:user].to_s != ""
      ssh_options[:user]
    else
      user
    end
  end
  
  def get_ssh_port(port = 22)
    ssh_options = get_ssh_options
    if ssh_options.has_key?(:port) && ssh_options[:port].to_s != ""
      ssh_options[:port]
    else
      port
    end
  end
  
  def ssh_init_profile_script
    if @ssh_init_profile_script == nil
      init_profile_script_parts = []
      [
        "$HOME/.bash_profile",
        "$HOME/.bash_login",
        "$HOME/.profile"
      ].each{
        |filename|

        if init_profile_script_parts.size() == 0
          init_profile_script_parts << "if"
        else
          init_profile_script_parts << "elif"
        end

        init_profile_script_parts << "[ -f #{filename} ]; then . #{filename};"
      }
      init_profile_script_parts << "fi;"
      @ssh_init_profile_script = init_profile_script_parts.join(" ")
    end
  
    return @ssh_init_profile_script
  end

  def hostname
    `hostname 2>/dev/null`.chomp
  end
  
  def is_localhost?(hostname)
    if hostname == DEFAULTS
      return false
    end
    
    @_is_localhost_cache ||= {}
    unless @_is_localhost_cache.has_key?(hostname)
      @_is_localhost_cache[hostname] = _is_localhost?(hostname)
    end
    
    return @_is_localhost_cache[hostname]
  end
  
  def _is_localhost?(hostname)
    if hostname == hostname()
      return true
    end

    ip_addresses = get_ip_addresses(hostname)
    if ip_addresses == false
      return false
    end

    debug("Search ifconfig for #{ip_addresses.join(', ')}")
    ifconfig = IfconfigWrapper.new().parse()
    ifconfig.each{
      |iface|

      begin
        # Do a string comparison so that we only match the address portion
        iface.addresses().each{
          |a|
          if ip_addresses.include?(a.to_s())
            return true
          end
        }
      rescue ArgumentError
      end
    }

    false
  end
  
  def get_ip_addresses(hostname)
    begin
      if hostname == DEFAULTS
        return false
      end
      
      ip_addresses = Timeout.timeout(5) {
        Resolv.getaddresses(hostname)
      }
      
      if ip_addresses.length == 0
        begin
          ping_result = cmd_result("ping -c1 #{hostname} 2>/dev/null | grep PING")
          matches = ping_result.match("[0-9]+.[0-9]+.[0-9]+.[0-9]+")
          if matches && matches.size() > 0
            return [matches[0]]
          end
        rescue CommandError
        end
        
        warning "Unable to determine the IP addresses for '#{hostname}'"
        return false
      end
      
      return ip_addresses
    rescue Timeout::Error
      warning "Unable to lookup #{hostname} because of a DNS timeout"
      return false
    rescue => e
      warning "Unable to determine the IP addresses for '#{hostname}'"
      return false
    end
  end
  
  def is_real_hostname?(hostname)
    begin
      ip_addresses = Timeout.timeout(5) {
        Resolv.getaddresses(hostname)
      }

      if ip_addresses.length == 0
        begin
          ping_result = cmd_result("ping -c1 #{hostname} 2>/dev/null | grep PING")
          matches = ping_result.match("[0-9]+.[0-9]+.[0-9]+.[0-9]+")
          if matches && matches.size() > 0
            ip_addresses = [matches[0]]
          end
        rescue CommandError
        end
      end
    rescue Timeout::Error
    rescue
    end
    
    if ip_addresses.size() == 0
      return false
    else
      return true
    end
  end
  
  # Find out the full executable path or return nil
  # if this is not executable. 
  def which(cmd)
    if ! cmd
      nil
    else 
      path = cmd_result("which #{cmd} 2>/dev/null", true)
      path.chomp!
      if File.executable?(path)
        path
      else
        nil
      end
    end
  end
end