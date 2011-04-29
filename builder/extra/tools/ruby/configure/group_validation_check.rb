class GroupValidationCheck
  include ValidationCheckInterface
  attr_accessor :name, :singular, :plural
  
  def initialize(name, singular, plural)
    @group_checks = []
    @name = name.to_s()
    @config = nil
    @weight = 0
    @singular = singular.to_s().downcase()
    @plural = plural.to_s().downcase()
    
    super()
  end
  
  # The config object must be set down on each of the checks so that they
  # have direct access
  def set_config(config)
    @config = config
    each_check{
      |check|
      check.set_config(config)
    }
  end
  
  def is_initialized?
    (super() && get_checks().size() > 0)
  end
  
  def validate
    each_member_check{
      |member, check|
      
      check.set_member(member)
      check.run()
      @errors = @errors + check.errors
    }
  end
  
  # Add a single check to this group
  def add_check(new_check)
    unless new_check.is_a?(GroupValidationCheckMember)
      raise "Unable to add #{new_check.class().name()}:#{new_check.get_name()} because it does not extend GroupValidationCheckMember"
    end
    
    unless new_check.is_a?(ConfigureValidationCheck)
      raise "Unable to add #{new_check.class().name()}:#{new_check.get_name()} because it does not extend ConfigureValidationCheck"
    end
    
    new_check.set_group(self)
    @group_checks << new_check
  end
  
  # Add a list of checks to this group
  # self.add_checks(check1, check2, check3)
  def add_checks(*new_checks)
    new_checks_count = new_checks.size
    for i in 0..(new_checks_count-1)
      add_check(new_checks[i])
    end
  end
  
  # Get the list of checks in this group
  def get_checks
    @group_checks || []
  end
  
  # Get the list of members excluding the defaults entry
  def get_members
    (@config.getPropertyOr(@name, {}).keys() - [DEFAULTS])
  end
  
  # Loop over each member to execute &block
  # This will exclude the defaults entry in the group
  def each_member(&block)
    get_members().each{
      |member|
      
      block.call(member)
    }
  end
  
  # Loop over each check to execute &block
  def each_check(&block)
    get_checks().each{
      |check|
      
      block.call(check)
    }
  end
  
  # Loop over each member-check combination to execute &block
  # This will exclude the defaults entry in the group
  def each_member_check(&block)
    each_member{
      |member|
      each_check{
        |check|
        check.set_member(member)
        block.call(member, check)
      }
    }
  end
end

module GroupValidationCheckMember
  # Assign the parent group to this prompt
  def set_group(val)
    @parent_group = val
  end
  
  # Assign the current member for this prompt
  def set_member(member_name)
    @member_name = member_name
  end
  
  # Reset the member assignment for this prompt
  def clear_member
    @member_name = nil
  end
  
  # Return the current member or the defaults member if none is set
  def get_member
    if @member_name
      @member_name
    else
      DEFAULTS
    end
  end
  
  # Get an array prepared for the Properties.*Property calls
  def get_member_key(name)
    [@parent_group.name, get_member(), name]
  end
end