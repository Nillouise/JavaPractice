/**
 * Created by win7x64 on 2016/12/18.
 */
import java.io.*;
import java.net.*;
import java.util.Scanner;

public class TCPServer {
    public static void listen()throws Exception
    {
        String clientSentence;
        String capitalizedSentence;
        ServerSocket welcomeSocket = new ServerSocket(6789);
        while (true)
        {
            Socket connectionSocket = welcomeSocket.accept();
            BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
            clientSentence =inFromClient.readLine();
            capitalizedSentence = clientSentence.toUpperCase() +"\n";
            System.out.println(capitalizedSentence);
//            outToClient.writeBytes(capitalizedSentence);


        }
    }



}
