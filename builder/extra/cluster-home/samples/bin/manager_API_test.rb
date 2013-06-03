#!/usr/bin/env ruby

require "./cluster-home/lib/ruby/tungsten"
require 'pp'

class TungstenEnvironment

  include TungstenScript
  include TungstenAPI

  def main
      cctrl = TungstenDataserviceManager.new()
      cctrl.list(:hash)
      puts ''
      json_obj = cctrl.get('r1:8090','chicago', 'policy')
      pp json_obj["message"]
      begin
        APICall.set_return_on_call_fail(:hash)
        json_obj = cctrl.post('r1:8090','chicago/qa.r1.continuent.com', 'promote')
        pp json_obj["message"]
      rescue Exception => e
        puts e
      end
  end

  self.new().run()
end

