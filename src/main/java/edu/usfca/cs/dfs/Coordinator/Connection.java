package edu.usfca.cs.dfs.Coordinator;

import edu.usfca.cs.dfs.StorageMessages;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

public class Connection {
    private Socket connectionSocket;
//    private int timeout = 1000;

    public void sendSomthing(String hostport, StorageMessages.DataPacket message) {
        try {
            String[] address = hostport.split(":");
            InetAddress ip = InetAddress.getByName(address[0]);
            int port = Integer.parseInt(address[1]);
            connectionSocket = new Socket(ip, port);
            OutputStream outstream = connectionSocket.getOutputStream();
            message.writeDelimitedTo(outstream);
            connectionSocket.close();
        } catch (Exception e) {
            System.out.println("Send something error");
        }


    }
}
