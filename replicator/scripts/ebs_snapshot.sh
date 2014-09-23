#!/usr/bin/env ruby

require "#{File.dirname(__FILE__)}/../../cluster-home/lib/ruby/tungsten"
require "#{File.dirname(__FILE__)}/../lib/ruby/ebs_snapshot_backup"

TU.set_log_level(Logger::DEBUG)
EBSSnapshotBackup.new().run()