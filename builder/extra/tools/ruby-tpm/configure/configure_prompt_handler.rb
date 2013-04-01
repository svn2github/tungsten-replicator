# This class will prompt the user for each of the ConfigurePrompts that are
# registered and validate that they have valid values
class ConfigurePromptHandler
  include ConfigureMessages

  def initialize(config)
    super()
    @errors = []
    @prompts = []
    @key_index = {}
    @config = config
    initialize_prompts()
  end
  
  def initialize_prompts
    @prompts = []
    @key_index = {}
    Configurator.instance.command.get_prompts().each{
      |prompt_obj| 
      register_prompt(prompt_obj)
    }
    @prompts = @prompts.sort{|a,b| a.get_weight <=> b.get_weight}
  end
  
  def register_prompts(prompt_objs)
    prompt_objs.each{|prompt_obj| register_prompt(prompt_obj)}
  end
  
  # Validate the prompt object and add it to the queue
  def register_prompt(prompt_obj)    
    unless prompt_obj.is_a?(ConfigurePromptInterface)
      raise "Attempt to register invalid prompt #{prompt_obj.class} failed " +
        "because it does not extend ConfigurePromptInterface"
    end
    
    prompt_obj.set_config(@config)
    unless prompt_obj.is_initialized?()
      raise "#{prompt_obj.class().name()} cannot be used because it has not been properly initialized"
    end
    
    @prompts.push(prompt_obj)
    @key_index[prompt_obj.name] = prompt_obj
  end
  
  # Loop over each ConfigurePrompt object and collect the response
  def run()
    i = 0
    previous_prompts = []
    
    display_help()
    
    # Go through each prompt in the system and collect a value for it
    while i < @prompts.length
      begin
        Configurator.instance.debug("Start prompt #{@prompts[i].class().name()}:#{@prompts[i].get_name()}")
        @prompts[i].run()
        Configurator.instance.debug("Finish prompt #{@prompts[i].class().name()}:#{@prompts[i].get_name()}")
        if @prompts[i].allow_previous?()
          previous_prompts.push(i)
        end
        i += 1
      rescue ConfigureSaveConfigAndExit => csce
        # Pass the request to save and exit up the chain untouched
        raise csce
      rescue ConfigureAcceptAllDefaults
        force_default_prompt = false
        
        fix_default_value = lambda do ||
          begin
            # Prompt the user because the default value is invalid
            @prompts[i].run
            if @prompts[i].allow_previous?
              previous_prompts.push(i)
            end
            i += 1
            force_default_prompt = false
          rescue ConfigureSaveConfigAndExit => csce
            # Pass the request to save and exit up the chain untouched
            raise csce
          rescue ConfigureAcceptAllDefaults
            # We are already trying to do this
            puts "The current prompt does not have a valid default, please provide a value"
            redo
          rescue ConfigurePreviousPrompt
            previous_prompt = previous_prompts.pop()
            if previous_prompt == nil
              puts "Unable to move to the previous prompt"
            else
              i = previous_prompt
              force_default_prompt = true
            end
          end
        end
        
        # Store the default value for this and all remaining prompts
        Configurator.instance.write "Accepting the default value for all remaining prompts"
        while i < @prompts.length do
          unless @prompts[i].enabled_for_config?
            # Clear the config value because the prompt is disabled
            @prompts[i].save_disabled_value()
          else
            @prompts[i].save_current_value()
          end
          
          unless @prompts[i].enabled?
            i += 1
          else
            begin
              # Trigger the prompt to be run because the user requested it
              if force_default_prompt
                raise PropertyValidatorException
              end
              
              # Ensure that the value is valid 
              @prompts[i].validate()
              unless @prompts[i].is_valid?
                raise ConfigurePromptError.new(self, "")
              end
              
              i += 1
            rescue ConfigurePromptError => e
              fix_default_value.call()
            rescue ConfigurePromptErrorSet => s
              fix_default_value.call()
            end
          end
        end
      rescue ConfigurePreviousPrompt
        previous_prompt = previous_prompts.pop()
        if previous_prompt == nil
          puts "Unable to move to the previous prompt"
        else
          i = previous_prompt
        end
      end
    end
  end
    
  def validate()
    reset_errors()
    prompt_keys = []

    # Test each ConfigurePrompt to ensure the config value passes the validation rule
    @prompts.each{
      |prompt|
      begin
        prompt_keys = prompt_keys + prompt.get_keys()
        prompt.validate()
        @errors = @errors + prompt.errors
      rescue ConfigurePromptError => cpe
        @errors << cpe
      rescue ConfigurePromptErrorSet => s
        @errors = @errors + s.errors
      rescue => e
        begin
          val = prompt.get_value()
        rescue
          val = ""
        end
        
        Configurator.instance.debug(e.message + "\n" + e.backtrace.join("\n"), get_message_hostname())
        @errors << ConfigurePromptError.new(prompt, e.message, val)
      end
    }
    
    # Register prompts that already have an error to avoid duplicates
    @errors.each{
      |e|
      prompt_keys <<  e.prompt.get_name
    }
    
    # Ensure that there are not any extra values in the config object
    find_extra_keys = lambda do |hash, current_path|
      hash.each{
        |key,value|
        if key == SYSTEM
          next
        end
        if key == REMOTE
          next
        end
        
        if current_path
          path = "#{current_path}.#{key}"
        else
          path = key
        end
        
        if value.is_a?(Hash)
          find_extra_keys.call(value, path)
        else
          unless prompt_keys.include?(path)
            @errors.push(ConfigurePromptError.new(
              ConfigurePrompt.new(path, "Unknown configuration key"),
              "This is an unknown configuration key",
              @config.getProperty(path.split("."))
            ))
          end
        end
      }
    end
    find_extra_keys.call(@config.props, false)
    
    is_valid?()
  end
  
  def display_help
    filename = File.dirname(__FILE__) + "/interface_text/configure_prompt_handler_run"
    Configurator.instance.write_from_file(filename)
  end
  
  def each_prompt(&block)
    @prompts.each{
      |x|
      block.call(x)
    }
    self
  end
  
  def output_config_file_usage
    @prompts.each{
      |prompt|
      prompt.output_config_file_usage()
    }
  end
  
  def output_template_file_usage
    @prompts.each{
      |prompt|
      if prompt.enabled_for_template_file?()
        prompt.output_template_file_usage()
      end
    }
  end
  
  def output_update_components
    @prompts.each{
      |prompt|
       prompt.output_update_components()
    }
  end
  
  def find_prompt_by_name(name)
    each_prompt{
      |prompt|
      
      begin
        return prompt.find_prompt_by_name(name)
      rescue IgnoreError
        #Do Nothing
      end  
    }
    
    nil
  end
  
  def find_prompt(attrs)
    prompt = @key_index[attrs[0]]
    if prompt != nil
      begin
        return prompt.find_prompt(attrs)
      rescue IgnoreError
        #Do Nothing
      end
    end
    
    nil
  end
  
  def get_property(attrs, allow_disabled = false)
    prompt = @key_index[attrs[0]]
    if prompt != nil
      begin
        return prompt.get_property(attrs, allow_disabled)
      rescue IgnoreError
        #Do Nothing
      end
    end
    
    nil
  end
  
  def find_template_value(attrs, transform_values_method)
    prompt = @key_index[attrs[0]]
    if prompt != nil
      begin
        return prompt.find_template_value(attrs, transform_values_method)
      rescue IgnoreError
        #Do Nothing
      end
    end
    
    nil
  end
  
  def update_deprecated_keys()
    @prompts.each{
      |prompt|
      
      prompt.update_deprecated_keys()
    }
  end
  
  def save_system_defaults
    @config.setProperty([SYSTEM], nil)
    @prompts.each{
      |prompt|
      
      begin
        prompt.save_system_default()
      rescue ConfigurePromptError => cpe
        @errors << cpe
      rescue ConfigurePromptErrorSet => s
        @errors = @errors + s.errors
      end
    }
  end
  
  def prepare_saved_config
    @prompts.each{
      |prompt|
      
      prompt.prepare_saved_config_value()
    }
    
    # Clear this for config files that aren't the actual 
    # $CONTINUENT_ROOT/conf/tungsten.cfg file
    @config.setNestedProperty(nil, [SYSTEM])
  end
  
  def prepare_saved_server_config
    @prompts.each{
      |prompt|
      
      prompt.prepare_saved_config_value(true)
    }
  end
  
  def get_updated_keys(old_cfg)
    reset_errors()
    r = []
    
    @prompts.each{
      |prompt|
      
      begin
        r = r + prompt.get_updated_keys(old_cfg)
      rescue ConfigurePromptError => cpe
        @errors << cpe
      rescue ConfigurePromptErrorSet => s
        @errors = @errors + s.errors
      rescue IgnoreError
      end
    }
 
    r
  end
end

class ConfigurePromptError < StandardError
  attr_reader :prompt, :message, :current_value
  
  def initialize(prompt, message, current_value = nil)
    @prompt = prompt
    @message = message
    @current_value = current_value
  end
  
  def is_fatal?
    true
  end
  
  def clean
  end
  
  def output
    Configurator.instance.write_divider(Logger::ERROR)
    Configurator.instance.error @prompt.get_display_prompt()
    Configurator.instance.error "> Message: #{@message}"
    
    arg = @prompt.get_command_line_argument
    if Configurator.instance.is_interactive?
      Configurator.instance.error "> Prompt: #{@prompt.get_display_prompt()}"
    else
      if @prompt.enabled_for_command_line?
        arg = @prompt.get_command_line_argument
        Configurator.instance.error "> Argument: --#{arg}"
      end
    end
    
    if @current_value.to_s() != ""
      Configurator.instance.error "> Current Value: #{@current_value}"
    end
    
    Configurator.instance.error "> Prompt Class: #{@prompt.class.name}"
  end
  
  def get_message
    @message
  end
end

class ConfigurePromptErrorSet < StandardError
  attr_reader :errors
  
  def initialize(errors = [])
    @errors = errors
  end
end