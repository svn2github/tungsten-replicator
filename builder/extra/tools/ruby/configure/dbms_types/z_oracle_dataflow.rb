class OracleDatabasePlatform
  def get_extractor_template
    super()
  end

  def get_default_table_engine
    case @config.getProperty(REPL_ROLE)
    when REPL_ROLE_S
      ""
    else
      "CDC"
    end
  end

  def get_allowed_table_engines
    ["CDC", "CDCSYNC"]
  end
end