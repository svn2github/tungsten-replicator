#!/usr/bin/env ruby
#
# This script outputs information about the Tungsten environment
#

require "#{File.dirname(__FILE__)}/../../lib/ruby/tungsten"

class TungstenEnvironment
  include TungstenScript
  
  def main
    TU.info("Latency is #{opt(:latency)}")
    
    if opt(:test)
      TU.warning("Test mode is enabled")
    end
  end
  
  def configure
    require_installed_directory?(false)
    
    add_option(:test, {
      :on => "--test",
      :help => "A test option",
      :default => false
    })
    
    add_option(:boolean, {
      :on => "--boolean String",
      :parse => method(:parse_boolean_option),
      :help => "This will be parsed to be either true or false",
      :default => false
    })
    
    add_option(:integer, {
      :on => "--integer String",
      :parse => method(:parse_integer_option),
      :help => "This will be parsed as an integer",
      :default => 10
    })
    
    add_option(:latency, {
      :on => "--latency String",
      :help => "The maximum allowed latency",
      :default => 60
    }) {|val|
      if val == "false"
        false
      else
        val.to_i()
      end
    }
  end
  
  def validate
    super()
    
    # Optional logic to test the options
  end
  
  self.new().run()
end