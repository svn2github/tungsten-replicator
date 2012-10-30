
package com.continuent.tungsten.common.utils;

import java.net.ServerSocket;

public class BindPort
{

    public static void main(String[] args)
    {

        if (args.length < 1)
        {
            System.err.println("usage: bindPort <port>");
            System.exit(1);
        }

        int iterations = 0;
        while (true)
        {
            int port = Integer.parseInt(args[0]);

            try
            {
                ServerSocket s = new ServerSocket(port);
                System.out.println("listening on port " + port);
                sleep(Long.MAX_VALUE);

            }
            catch (Exception ex)
            {
                System.out.print("*");

                if (++iterations % 40 == 0)
                    System.out.println("");

                sleep(1000);

            }
        }

    }

    public static void sleep(long milliseconds)
    {
        try
        {
            Thread.sleep(milliseconds);
        }
        catch (Exception ignored)
        {
        }

    }
}