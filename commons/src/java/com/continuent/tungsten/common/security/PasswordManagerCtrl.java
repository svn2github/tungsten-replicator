/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2013 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 *
 * Initial developer(s): Ludovic Launer
 * Contributor(s): 
 */

package com.continuent.tungsten.common.security;

import java.text.MessageFormat;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.security.PasswordManager.ClientApplicationType;

/**
 * Application to manage passwords and users
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class PasswordManagerCtrl
{
    
    private static  Logger                                  logger  = Logger.getLogger(PasswordManagerCtrl.class);
    
    private         Options                                 options                 = new Options();
    private static  PasswordManagerCtrl                     pwd;
    private static  PasswordManager.ClientApplicationType   clientApplicationType   = null;
    public          PasswordManager                         passwordManager         = null;
    
    // -- Define constants for command line arguments ---
    private static final String _HELP = "help";
    private static final String CREATE = "create";
    private static final String _CREATE = "c";
    private static final String DELETE = "delete";
    private static final String _DELETE = "d";
    private static final String FILE = "file";
    private static final String _FILE = "f";
    private static final String TARGET_APPLICATION = "target";
    private static final String _TARGET_APPLICATION = "t";
    
    static Option create;

    /**
     * Setup command line options
     * 
     */
    @SuppressWarnings("static-access")
    private void setupCommandLine()
    {
        // --- Options on the command line ---
        Option help = new Option(_HELP, "print this message");
        Option file = OptionBuilder.withLongOpt(FILE).withArgName("filename").hasArgs().withDescription(SecurityConf.SECURITY_PROPERTIES_FILE_NAME + " file location").create(_FILE);
    
        // Mutually excluding options
        OptionGroup optionGroup = new OptionGroup( );
         create = OptionBuilder.withLongOpt(CREATE).hasArgs(2).withArgName("username> <password").withDescription("Creates or Updates a user").create(_CREATE);
        Option delete = OptionBuilder.withLongOpt(DELETE).withArgName("username").hasArgs().withDescription("Deletes a user").create(_DELETE);
        
        
        
        optionGroup.addOption(create);
        optionGroup.addOption(delete);
        optionGroup.setRequired(true);      // At least 1 command required
        
        Option targetApplication = OptionBuilder.withLongOpt(TARGET_APPLICATION).withArgName("target").hasArgs().withDescription("Target application: " + getListOfClientApplicationType()).create(_TARGET_APPLICATION);
        
        // --- Add options to the list ---
        this.options.addOptionGroup(optionGroup);
        this.options.addOption(help);
        this.options.addOption(file);
        
        this.options.addOption(targetApplication);
    }

    /**
     * Creates a new <code>PasswordManager</code> object
     * 
     */
    public PasswordManagerCtrl()
    {
        this.setupCommandLine();
    }

    /**
     * Password Manager entry point
     * 
     * @param argv
     * @throws Exception
     */
    public static void main(String argv[]) throws Exception
    {
        pwd = new PasswordManagerCtrl();
        
        // --- Options ---
        ClientApplicationType   clientApplicationType           = null;
        String                  securityPropertiesFileLocation  = null;
        String                  username                        = null;
        String                  password                        = null;

        try {
            CommandLineParser parser = new GnuParser();
            // parse the command line arguments
            CommandLine line = parser.parse( pwd.options, argv );

            // --- Handle options ---
            if (line.hasOption(_HELP))
            {
                DisplayHelpAndExit();
            }
            
            // --- Optional arguments : Get options ---
            if (line.hasOption(_TARGET_APPLICATION))                        // Target Application
            {  
                String target = line.getOptionValue(TARGET_APPLICATION);
                clientApplicationType = PasswordManagerCtrl.getClientApplicationType(target);
            }
            if (line.hasOption(_FILE))                                      // security.properties file location
            {  
                securityPropertiesFileLocation = line.getOptionValue(_FILE);
            }
            if (line.hasOption(_CREATE))                                    // Make sure username + password are provided
            {   
                String[] createArgs = line.getOptionValues(_CREATE);
                if (createArgs.length<2)
                    throw new MissingArgumentException(create);
                
                username = createArgs[0];
                password = createArgs[1];
            }
            
            
            try
            {
                pwd.passwordManager = new PasswordManager(securityPropertiesFileLocation, clientApplicationType);
            }
            catch (ConfigurationException ce)
            {
                logger.error(MessageFormat.format("Could not retrieve configuration information: {0}\n Try to specify a security.properties file location or have the cluster.home variable set.", ce.getMessage()));
                System.exit(0);
            }
            

            // --- Perform commands ---
            
            // ######### Create ##########
            if (line.hasOption(_CREATE))
            {
                try
                {
                    pwd.passwordManager.setPasswordForUser(username, password);
                    System.out.println(MessageFormat.format("User created successfuly: {0}", username));
                }
                catch (Exception e)
                {
                    logger.error(MessageFormat.format("Error while creating user: {0}", e.getMessage()));
                }
            }
            
            // ########## DELETE ##########
            else if (line.hasOption(_DELETE))
            {
                username = line.getOptionValue(_DELETE);
                
                try
                {
                    pwd.passwordManager.deleteUser(username);
                    System.out.println(MessageFormat.format("User deleted successfuly: {0}", username));
                }
                catch (Exception e)
                {
                    logger.error(MessageFormat.format("Error while deleting user: {0}", e.getMessage()));
                }
            }
           
           
        }
        catch( ParseException exp ) {
            System.out.println(exp.getMessage());
            
            DisplayHelpAndExit();
        }
    }
    
    
    /**
     * Get the list of ClientApplicationType as a string
     * 
     * @return String containing all possible lower-cased application types
     */
    private static String getListOfClientApplicationType()
    {
        String listApplicationType = "";
        
        
       for (ClientApplicationType appType: ClientApplicationType.values())
       {
           listApplicationType += ( (listApplicationType.isEmpty()?"":" | ") + appType.toString().toLowerCase() );
       }
       listApplicationType.trim();
        
        return listApplicationType;
    }
    
    /**
     * Get the application type argument.
     * Populates the <code>clientApplicationType</code> property
     * 
     * @param commandLineTargetApplicationType the argument provided on the command line
     * @return a ClientApplicationType
     * @throws UnrecognizedOptionException if the cast could not be made
     */
    private static ClientApplicationType getClientApplicationType(String commandLineTargetApplicationType) throws UnrecognizedOptionException
    {
        try
        {
            clientApplicationType = ClientApplicationType.fromString(commandLineTargetApplicationType);
        }
        catch (IllegalArgumentException iae)
        {
            throw new UnrecognizedOptionException(MessageFormat.format("The target application type does not exist: {0}", commandLineTargetApplicationType));
        }
        
        return clientApplicationType;
    }
    
    /**
     * Display the program help and exits
     * 
     */
    private static void DisplayHelpAndExit()
    {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("PasswordManager", pwd.options);
        System.exit(0);
    }
    
    
    
    
    
    
    
}
