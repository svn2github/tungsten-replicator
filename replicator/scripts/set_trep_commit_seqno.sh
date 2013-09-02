#!/usr/bin/env ruby
#
# Make sure the trep_commit_seqno table exists and has the correct values
#

require "#{File.dirname(__FILE__)}/../../cluster-home/lib/ruby/tungsten"
require "#{File.dirname(__FILE__)}/../lib/ruby/set_trep_commit_seqno"

ReplicatorSetTrepCommitSeqno.new().run()