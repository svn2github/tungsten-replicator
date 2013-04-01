module ConfigureDeploymentStepDataService
  def get_methods
    [
      ConfigureCommitmentMethod.new("create_composite_dataservice", 5, 0)
    ]
  end
  module_function :get_methods
  
  def create_composite_dataservice
    if manager_is_running?()
      
      
      @config.getPropertyOr(DATASERVICES, {}).keys().each{
        |comp_ds_alias|
        
        if comp_ds_alias == DEFAULTS
          next
        end
        
        if @config.getProperty([DATASERVICES, comp_ds_alias, DATASERVICE_IS_COMPOSITE]) == "false"
          next
        end
        
        unless include_dataservice?(comp_ds_alias)
          next
        end
        
        cmd_result("echo \"create composite dataservice #{comp_ds_alias}\" | #{get_cctrl_cmd} -multi")
        
        @config.getProperty([DATASERVICES, comp_ds_alias, DATASERVICE_COMPOSITE_DATASOURCES]).to_s().split(",").each{
          |ds_alias|
                    
          cmd_result("echo \"create composite datasource #{ds_alias}\" | #{get_cctrl_cmd} -multi -service #{comp_ds_alias}")
        }
      }
    end
  end
end