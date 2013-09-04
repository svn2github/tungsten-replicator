#!/usr/bin/env ruby

require "#{File.dirname(__FILE__)}/../../cluster-home/lib/ruby/tungsten"
require "#{File.dirname(__FILE__)}/../lib/ruby/mysqlbinlog_from_seqno"

MysqlbinlogFromSeqno.new().run()