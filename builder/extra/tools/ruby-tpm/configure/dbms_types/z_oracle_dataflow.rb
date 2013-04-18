class OracleDatabasePlatform
  def get_extractor_template
    if @config.getPropertyOr([DATASOURCES, @ds_alias, REPL_ORACLE_SCAN], "") == ""
      "tungsten-replicator/samples/conf/extractors/#{get_uri_scheme()}.tpl"
    else
      "tungsten-replicator/samples/conf/extractors/oracle-scan.tpl"
    end
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