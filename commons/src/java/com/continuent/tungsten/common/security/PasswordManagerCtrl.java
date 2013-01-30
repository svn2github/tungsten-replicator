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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * Application to manage passwords and users
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class PasswordManagerCtrl
{
    private static  Logger                  logger  = Logger.getLogger(PasswordManagerCtrl.class);
    
    private         Options                 options = new Options();
    private static  PasswordManagerCtrl     pwd;
    
    // -- Define constants for command line arguments ---
    private static final String _HELP = "help";
    private static final String CREATE = "create";
    private static final String _CREATE = "c";
    private static final String DELETE = "delete";
    private static final String _DELETE = "d";
    private static final String UPDATE = "update";
    private static final String _UPDATE = "u";
    private static final String FILE = "file";
    private static final String _FILE = "f";

    /**
     * Setup command line options
     * 
     */
    @SuppressWarnings("static-access")
    private void setupCommandLine()
    {
        // --- Options on the command line ---
        Option help = new Option(_HELP, "print this message");
        Option file = OptionBuilder.withLongOpt(FILE).withArgName("filename").hasArgs().withDescription("Password file location").create(_FILE);
    
        // Mutually excluding options
        OptionGroup optionGroup = new OptionGroup( );
        Option create = OptionBuilder.withLongOpt(CREATE).withArgName("username").hasArgs().withDescription("Creates a user").create(_CREATE);
        Option delete = OptionBuilder.withLongOpt(DELETE).withArgName("username").hasArgs().withDescription("Deletes a user").create(_DELETE);
        Option update = OptionBuilder.withLongOpt(UPDATE).withArgName("username").hasArgs().withDescription("Updates a user").create(_UPDATE);
        
        optionGroup.addOption(create);
        optionGroup.addOption(delete);
        optionGroup.addOption(update);
        
        // --- Add options to the list ---
        this.options.addOptionGroup(optionGroup);
        this.options.addOption(help);
        this.options.addOption(file);
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

        try {
            CommandLineParser parser = new GnuParser();
            // parse the command line arguments
            CommandLine line = parser.parse( pwd.options, argv );

            // --- Handle options ---
            if (line.hasOption(_HELP) || line.getArgList().size()==0)
            {
                DisplayHelpAndExit();
            }
            else if (line.hasOption(_CREATE))
            {
                
            }
            else if (line.hasOption(_DELETE))
            {
                
            }
            else if (line.hasOption(_UPDATE))
            {
                
            }
           
            
           
        }
        catch( ParseException exp ) {
            System.out.println(exp.getMessage());
            
            DisplayHelpAndExit();
        }
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
