#!/usr/bin/env ruby

require "#{File.dirname(__FILE__)}/../../cluster-home/lib/ruby/tungsten"
require "#{File.dirname(__FILE__)}/../lib/ruby/mysqldump"

TU.set_log_level(Logger::DEBUG)
TungstenMySQLdumpScript.new().run()