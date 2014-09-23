#!/usr/bin/env ruby

require "#{File.dirname(__FILE__)}/../../cluster-home/lib/ruby/tungsten"
require "#{File.dirname(__FILE__)}/../lib/ruby/file_copy_snapshot_backup"

TU.set_log_level(Logger::DEBUG)
FileCopySnapshotBackup.new().run()