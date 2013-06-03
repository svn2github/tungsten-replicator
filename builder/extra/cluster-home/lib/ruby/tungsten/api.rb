module TungstenAPI

require 'uri'
require 'net/http'



#      SAMPLE USAGE:
#      api_server='localhost:8090'
#      service=chicago
#
#      cctrl = TungstenServiceManager.new()
#      cctrl.list(:text)
#      puts ''
#      json_obj = cctrl.get(api_server, service, 'policy')
#      pp json_obj["message"]
#
#      APICall.set_return_on_call_fail(:hash)
#      json_obj = cctrl.post(api_server, "#{service}/host1", 'promote')
#      pp json_obj["message"]  ## Failure message will come here
# 
#      begin
#        APICall.set_return_on_call_fail(:raise)
#        json_obj = cctrl.post(api_server,'#{service}/host1', 'promote')
#        pp json_obj["message"]
#      rescue Exception => e
#        puts e   # failure message will coem here
#      end
 

#
# This class defines an API call, as needed by the Tungsten Manager API.
# Up to a ppint, the class is fairly generic, as it defines how to create a URI 
# with three to four segments.
#
# An APICall instance can run a 'get' or  a 'post' call.
#
# Class methods:
# * set_api_root : defines which string is the root of the URI (default: 'manager')
# * set_return_on_call_fail: defines how we fail when a call does not succeed. 
#   You can set either :hash (default) or :raise. When :hash is defined, get and post
#   always return a hash containing 'message' and 'httpStatus'. If :raise is chosen
#   then all call failures will raise an exception
# * header : used to display the list of API calls
# * dashes : used to draw dashes below the header. (internally used by  TungstenClusterManager::list)
#
# Public instance methods:
#  * initialize (name, prefix, command, help, return_structure = :hash, type = :get)
#  * description (display_mode ={:text|:hash:json} ) shows the API structure
#  * make_uri (api_server, service) : creates a well formed URI, ready to submit
#  * get (api_server, service) : returns the result of a 'get' call
#  * post (api_server, service) : returns the result of a 'post' operation
#
class APICall

    attr_accessor :name, :prefix, :command, :return_structure, :type
    #
    # By default, a call using this class will return a hash
    # You can change this behavior by setting the fail type to :raise instead
    #
    @@return_on_call_fail=:hash

    @@api_root = 'manager'

    # 
    # This template is used to display a quick help about the API call
    #
    @@template = "%-15s %-4s %-10s %-10s %s"
    #
    # name
    # type 
    # prefix
    # command
    # help

    # 
    # Initialize the object. 
    # * name : how we call the API, even informally. This name is not used operationally
    # * prefix: the portion of the call that needs to be inserted before the service name
    # * command: what the api call responds to. For some calls, this part can be empty
    # * type: either :get or :post
    # * return_structure: so far, only :hash is supported
    # * help: a brief description of what the API does
    #
    def initialize(name, prefix, command, help, return_structure = :hash, type = :get)
        @name = name    
        @prefix = prefix
        @command = command
        @type = type  # type can be :get, :post, :cmd
        @returns = return_structure
        @help = help
        # TODO : add expected structure
    end

    #
    # Creates a well formed URI, ready to be used
    #
    def make_uri(api_server, service)
        if (service)
            return "http://#{api_server}/#{@@api_root}/#{@prefix}/#{service}/#{@command}" 
        else
            return "http://#{api_server}/#{@@api_root}/#{@command}" 
        end
    end

    #
    # Class method. Defines how we behave in case of call failure
    # By default, we ALWAYS return a :hash. We can also :raise
    # an exception
    #
    def self.set_return_on_call_fail(return_on_call_fail)
        @@return_on_call_fail = return_on_call_fail
    end

    #
    # Defines the default API root in the URI.
    # Currently it is 'manager'
    #
    def self.set_api_root(api_root)
        @@api_root = api_root
    end

    # 
    # returns a header for the API call fields
    #
    def self.header ()
        return sprintf @@template , 'name', 'type', 'prefix', 'command' , 'help'
    end

    #
    # returns a set of dashes to ptint below the header
    #
    def self.dashes ()
        return sprintf @@template , '----', '----', '------', '-------' , '----'
    end

    def to_s
        return sprintf @@template , @name, @type, @prefix, @command , @help     
    end

    def to_hash
        { 
            :name.to_s => @name, 
            :type.to_s => @type, 
            :prefix.to_s => @prefix, 
            :command.to_s => @command, 
            :help.to_s => @help 
        }
    end

    #
    # Returns a description of the API call, according to the display_mode:
    # * :text (default) is a single line of text according to @@template
    # * :hash is a Ruby hash of the API call contents
    # * :json is a JSON representation of the above hash
    #
    def description (display_mode = :text)
        if display_mode == :text
            return self.to_s
        end
        if display_mode == :json
            return JSON.generate(self.to_hash)
        elsif display_mode == :hash
            return self.to_hash
        else
            raise SyntaxError, "No suitable display mode selected"
        end
    end

    #
    # Used internally by the calls to get and post to determine if the response was successful
    #
    def evaluate_response (api_server, response)

        if response.body
            hash_from_json = JSON.parse(response.body)
        end

        if hash_from_json && hash_from_json["returnMessage"] && hash_from_json["returnCode"] && hash_from_json["returnCode"] != '200'
            return_object = {
                "httpStatus"    => hash_from_json["returnCode"],
                "message"       => hash_from_json["returnMessage"]
            } 
            if @@return_on_call_fail == :raise
                raise RuntimeError, "There was an error (#{hash_from_json["returnCode"]}) : #{hash_from_json["returnMessage"]}"
            end
            return return_object
        end
 
        if response.code != '200'
            if @@return_on_call_fail == :raise
                raise RuntimeError, "The request returned code #{response.code}"
            else
                return_object = {
                    "httpStatus"    => response.code,
                    "message"       => "unidentified error with code #{response.code}"
                } 
                return return_object
            end
        end
        return hash_from_json
    end

    #
    # Get a JSON object from the output of a command, such as 'trepctl status -json'
    #
    def get_from_file(path, command)
        # TODO get a JSON object from a command output, such as 'trepctl status -json' 
        # TODO: determine if we can do this within this class or if it's better to subclass
    end

    #
    # Runs a 'get' call, using a given api_server and service name
    #
    def get(api_server, service)
        api_uri = URI(self.make_uri(api_server,service))
        puts api_uri
        response = Net::HTTP.get_response(api_uri)
        return evaluate_response(api_server,response)
    end

    #
    # Runs a 'post' call, using a given api_server and service name
    #
    def post(api_server, service, post_params = {})
        api_uri = URI(self.make_uri(api_server,service))
        puts api_uri
        response = Net::HTTP.post_form(api_uri, post_params)
        return evaluate_response(api_server,response)
    end

end

#
# Container for API calls.
# It has the definition of the api calls supported through this architecture, and methods to call them easily.
# 
# Public instance methods:
#  * list (display_mode)
#    will show all the API registered with this service
#  * get(api_server,service,name) will return the result of a get call
#  * post(api_server,service,name) will return the result of a post operation
#

class TungstenServiceManager

    #
    # Registers all the known API calls for Tungsten data service
    #
    def initialize
        @@api_calls = {}
        add_api_call( APICall.new('status', 'status', '', 'Show cluster status', :hash, :get) )      
        add_api_call( APICall.new('promote', 'control', 'promote', 'promotes a slave to master', :hash, :post) )      
        add_api_call( APICall.new('policy',  'policy', '', 'show current policy',:hash, :get) )      
        add_api_call( APICall.new('setmaintenance', 'policy', 'maintenance', 'set policy as maintenance',:hash, :post) )      
        add_api_call( APICall.new('setautomatic', 'policy', 'automatic', 'set policy as automatic',:hash, :post) )      
        add_api_call( APICall.new('setmanual', 'policy', 'manual', 'set policy as manual',:hash, :post) )      
        add_api_call( APICall.new('shun', 'control', 'shun', 'shuns a data source',:hash, :post) )      
        add_api_call( APICall.new('welcome', 'control', 'welcome', 'welcomes back a data source',:hash, :post) )      
        add_api_call( APICall.new('backup', 'control', 'backup', 'performs a datasource backup',:hash, :post) )      
        add_api_call( APICall.new('restore', 'control', 'restore', 'Performs a datasource restore',:hash, :post) )      
        add_api_call( APICall.new('online', 'control', 'online', 'puts a datasource online',:hash, :post) )      
        add_api_call( APICall.new('offline', 'control', 'offline', 'Puts a datasource offline',:hash, :post) )      
        add_api_call( APICall.new('fail', 'control', 'fail', 'fails a datasource',:hash, :post) )      
        add_api_call( APICall.new('recover', 'control', 'recover', 'recover a failed datasource',:hash, :post) )      
        add_api_call( APICall.new('heartbeat', 'control', 'heartbeat', 'Issues a heartbeat on the master',:hash, :post) )      
    end

    #
    # Registers a given API call into the service
    #
    def add_api_call (api_call)
        @@api_calls[api_call.name()] = api_call
    end

    #
    # Display the list of registered API calls
    # using a given display_mode: 
    # * :text (default)
    # * :hash : good for further usage of the API call within the same application
    # * :json : good to export to other applications
    #
    def list (display_mode=:text)
        if display_mode == :text
            puts APICall.header()
            puts APICall.dashes()
            @@api_calls.sort.each do |name,api|
                puts api
            end
        else
            display_api_calls = {}
            @@api_calls.each do |name,api|
                display_api_calls[name] = api.to_hash
            end
            if display_mode == :hash
                require 'pp'
                pp display_api_calls
            elsif display_mode == :json
                puts JSON.generate(display_api_calls)
            else
                raise SyntaxError,  "no suitable display method selected"
            end
        end
    end

    #
    # Runs a 'get' call with a given API 
    #
    def get (api_server, service, name )
        return call(api_server,service,name,:get)
    end

    #
    # Runs a 'post' call with a given API 
    #
    def post (api_server, service, name )
        return call(api_server,service,name,:post)
    end

    private

    def call (api_server, service, name , type)
        api = @@api_calls[name]
        unless api
            raise SyntaxError, "api call #{name} not found"
        end
        if type == :get
            return api.get(api_server,service)
        else
            return api.post(api_server,service)
        end
    end
end

end

