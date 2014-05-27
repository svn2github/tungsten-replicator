class WatchFiles
  def self.show_differences(basedir)
    unless Configurator.instance.is_locked?()
      raise "Unable to show modified files because this is not the installed directory. If this is the staging directory, try running tpm from an installed Tungsten directory."
    end
    
    filename = basedir + "/.watchfiles"
    if File.exist?(filename)
      File.open(filename, 'r') do |file|
        file.read.each_line do |line|
          line.strip!

          unless line[0,1] == "/"
            line = "#{basedir}/#{line}"
          end
          current_file = line
          original_file = File.dirname(line) + "/." + File.basename(line) + ".orig"
          
          unless File.exist?(original_file)
            next
          end
          
          Configurator.instance.info("Compare #{current_file} to #{original_file}")
          begin
            file_differences = cmd_result("diff -u #{original_file} #{current_file}")
          rescue CommandError => ce
            if ce.rc == 1
              puts ce.result
            else
              raise ce
            end
          end
        end
      end
    else
      error("Unable to find #{filename}")
    end
  end
  
  def self.get_modified_files(basedir)
    filename = basedir + "/.watchfiles"
    if File.exist?(filename)
      modified_files = []
      File.open(filename, 'r') do |file|
        file.read.each_line do |line|
          line.strip!

          unless line[0,1] == "/"
            line = "#{basedir}/#{line}"
          end
          current_file = line
          original_file = File.dirname(line) + "/." + File.basename(line) + ".orig"
          
          unless File.exist?(original_file)
            next
          end
          
          Configurator.instance.debug("Compare #{current_file} to #{original_file}")
          begin
            file_differences = cmd_result("diff -u #{original_file} #{current_file}")
            # No differences
          rescue CommandError => ce
            if ce.rc == 1
              modified_files << line
            else
              raise ce
            end
          end
        end
      end
      
      return modified_files
    else
      return nil
    end
  end
end