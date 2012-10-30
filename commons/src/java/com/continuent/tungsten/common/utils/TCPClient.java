
package com.continuent.tungsten.common.utils;

import java.io.*;
import java.net.*;

class TCPClient
{
    public static void main(String argv[])
    {

        if (argv.length != 2)
        {
            System.out.println("usage: TCPClient <host> <port>");
            System.exit(1);
        }

        try
        {
            String host = argv[0];
            int port = Integer.parseInt(argv[1]);

            String sentence;
            String modifiedSentence;
            BufferedReader inFromUser = new BufferedReader(
                    new InputStreamReader(System.in));
            Socket clientSocket = new Socket(host, port);
            DataOutputStream outToServer = new DataOutputStream(
                    clientSocket.getOutputStream());
            BufferedReader inFromServer = new BufferedReader(
                    new InputStreamReader(clientSocket.getInputStream()));
            sentence = inFromUser.readLine();
            outToServer.writeBytes(sentence + '\n');
            modifiedSentence = inFromServer.readLine();
            System.out.println("FROM SERVER: " + modifiedSentence);
            clientSocket.close();
        }
        catch (Exception e)
        {
            System.out.println(e);
            System.exit(1);
        }
    }
}