libdir = File.dirname(__FILE__)
$LOAD_PATH.unshift(libdir) unless $LOAD_PATH.include?(libdir)

require "date"
require "fileutils"
require 'json'
require 'logger'
require "optparse"
require "pathname"
require 'pp'
require "singleton"
require "tempfile"
require 'timeout'
require "tungsten/common"
require "tungsten/exec"
require "tungsten/install"
require "tungsten/properties"
require "tungsten/script"
require "tungsten/topology"
require "tungsten/util"

# Setup default instances of the TungstenUtil and TungstenInstall classes
# Access these constants anywhere in the code
# TU.info(msg)
TU = TungstenUtil.instance()

# Intialize a default TungstenInstall object that uses TU.get_base_path()
# as the default. If --home-directory is found, that path is used instead.
install_base_path = TU.get_base_path()
opts = OptionParser.new
opts.on("--directory String") {|val| install_base_path = "#{val}/tungsten"}
TU.run_option_parser(opts)

begin
  TI = TungstenInstall.get(install_base_path)
rescue => e
  TU.exception(e)
  exit(1)
end