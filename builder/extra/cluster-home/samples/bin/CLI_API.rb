#!/usr/bin/env ruby

require "./cluster-home/lib/ruby/tungsten"
require 'pp'

class TungstenEnvironment

    include TungstenScript
    include TungstenAPI

    #
    # Initializes the cctrl object
    # and creates a command line option for every API call available
    #
    def configure
        super()
    
        @cctrl = TungstenDataserviceManager.new()
        apis = @cctrl.to_hash
        # 
        # loops through the API calls and creates an option for each one
        #
        apis.each { |name,api| 
            add_option(name.to_sym, {
                :on => "--#{name}",
                :help => api['help'],
                :default => false
                })
        }
        #
        # Add an option to define to which datasource we want to apply the call
        #
        add_option(:target, {
            :on => "--target String",
            :help => "defines the datasource to which we apply the api call ",
            :default => nil
        })

        #
        # Add an option to define the data service
        #
        add_option(:service, {
            :on => "--service String",
            :help => "defines the service name for the API call",
            :default => nil
        })

        #
        # Add an option to define the manager API server
        #
        add_option(:api_server, {
            :on => "--api-server String",
            :help => "defines the server to ask for the API",
            :default => 'localhost:8090'
        })

        #
        # We can instead list all calls
        #
        add_option(:list, {
            :on => "--list",
            :help => "Shows the api list",
            :default => false
        })
    end

    #
    # get a return JSON object and prints selected fields from the structure
    #
    def display_cluster (call_name, hash)
        root = hash["outputPayload"]
        unless hash["outputPayload"]
            raise "the response from #{call_name} does not contain 'outputPayload'}"
        end
        %w(replicators dataSources).each { |item|
            unless root[item]
                raise "the response from #{call_name} does not contain 'outputPayload/#{item}'}"
            end
        }
        status_tree = {}
        member_count=0
        root['dataSources'].each { |ds_name, ds|
            status_tree[ds_name] = {}
            unless root['replicators'][ds_name]
                raise "datasource #{ds_name} has no replicator in call to #{call_name}"
            end
            status_tree[ds_name]['state']    = root['dataSources'][ds_name]['state']
            status_tree[ds_name]['role']     = root['dataSources'][ds_name]['role']
            if status_tree[ds_name]['role'] == 'master'
                status_tree[ds_name]['role'] = "*master"
            end
            status_tree[ds_name]['progress'] = root['replicators'][ds_name]['appliedLastSeqno']
            status_tree[ds_name]['latency']  = root['dataSources'][ds_name]['appliedLatency']
        }
        policy = root['policyManagerMode']
        coordinator = root['coordinator']
        service_name = root['name']
        puts service_name
        puts "coordinator: #{coordinator} - policy: #{policy}"
        status_tree.sort.each { |ds_name, ds|
            puts sprintf "\t%-30s (%-8s:%-7s) - progress: %6d [%5.3f]", 
                ds_name,
                ds['role'],
                ds['state'],
                ds['progress'],
                ds['latency']
        }
    end

    #
    # Main app
    #
    def main
        # sets the defaults
        service = opt(:service)
        service ||= 'chicago'
        api_server = opt(:api_server)
        api_server ||= 'localhost:8090'

        if opt(:list)
            # displays the list of available API calls
            @cctrl.list()
        else
            # 
            # runs the requested API calls
            #
            apis = @cctrl.to_hash
            target = opt(:target)
            apis.each { |name,api|
                command = name
                if opt(command.to_sym) # if the option corresponding to the API name was selected
                    if target
                        service += "/#{target}"
                    end
                    cluster_hash = @cctrl.call_default(api_server,service, command)
                    if cluster_hash['httpStatus'] != 200
                        puts "*** #{cluster_hash['message']} (#{cluster_hash['httpStatus']})"
                    else
                        if api["type"] == :get
                            display_cluster(command, cluster_hash)
                        else
                            puts "OK: #{cluster_hash['message']} (#{cluster_hash['httpStatus']})"
                        end
                    end
                end
          }     
      end

  end

  self.new().run()
end

