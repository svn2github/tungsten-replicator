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
TI = TungstenInstall.get(TU.get_base_path())