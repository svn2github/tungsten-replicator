class InteractiveCommand
  include ConfigureCommand
  include ClusterCommandModule
  
  def initialize(config)
    super(config)
    interactive?(true)
  end
  
  def load_prompts
    prompt_handler = ConfigurePromptHandler.new(@config)
    
    debug("Collect prompt responses")
    # Collect responses to the configuration prompts
    begin
      @config.setProperty([SYSTEM], nil)
      prompt_handler.run()

      value = ""
      while value.to_s == ""
        puts "
Tungsten has all values needed to configure itself properly.  
Do you want to continue with the configuration (Y) or quit (Q)?"
        value = STDIN.gets
        value.strip!

        case value.to_s().downcase()
          when "y"
            next
          when "yes"
            next 
          when "no"
            raise ConfigureSaveConfigAndExit
          when "n"
            raise ConfigureSaveConfigAndExit
          when "q"
            raise ConfigureSaveConfigAndExit
          when "quit"
            raise ConfigureSaveConfigAndExit
          else
            value = nil
        end
      end
    rescue ConfigureSaveConfigAndExit => csce 
      write "Saving configuration values and exiting"
      save_config_file()
      raise IgnoreError
    end
    
    is_valid?()
  end
  
  def allow_interactive?
    true
  end
  
  def allow_check_current_version?
    true
  end

  def output_completion_text
    output_cluster_completion_text()

    super()
  end
  
  def self.get_command_name
    'interactive'
  end
  
  def self.display_command
    false
  end
end