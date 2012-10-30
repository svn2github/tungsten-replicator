
package com.continuent.tungsten.common.utils;

import java.io.*;
import java.net.*;

class TCPServer
{
    public static void main(String argv[])
    {

        if (argv.length != 1)
        {
            System.out.println("usage: TCPServer <port>");
            System.exit(1);
        }

        try
        {
            int port = Integer.parseInt(argv[0]);

            String clientSentence;
            String capitalizedSentence;
            ServerSocket welcomeSocket = new ServerSocket(port);

            while (true)
            {
                Socket connectionSocket = welcomeSocket.accept();
                BufferedReader inFromClient = new BufferedReader(
                        new InputStreamReader(connectionSocket.getInputStream()));
                DataOutputStream outToClient = new DataOutputStream(
                        connectionSocket.getOutputStream());
                clientSentence = inFromClient.readLine();
                System.out.println("Received: " + clientSentence);
                capitalizedSentence = clientSentence.toUpperCase() + '\n';
                outToClient.writeBytes(capitalizedSentence);
            }
        }
        catch (Exception e)
        {
            System.out.println(e);
            System.exit(1);
        }
    }
}
