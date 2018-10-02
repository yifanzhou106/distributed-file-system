package edu.usfca.cs.dfs.StorageNode;

import edu.usfca.cs.dfs.StorageMessages;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Connection {
    private Socket connectionSocket;
//    private int timeout = 1000;

    /**
     * Send something without reply
     *
     * @param hostport
     * @param message
     */
    public void sendSomthing(String hostport, StorageMessages.DataPacket message) {
        try {
            String[] address = hostport.split(":");
            InetAddress ip = InetAddress.getByName(address[0]);
            int port = Integer.parseInt(address[1]);
            connectionSocket = new Socket(ip, port);
//            connectionSocket.setSoTimeout(timeout);
            OutputStream outstream = connectionSocket.getOutputStream();
            message.writeDelimitedTo(outstream);
            connectionSocket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Send something with reply
     *
     * @param hostport
     * @param message
     * @return
     */
    public StorageMessages.DataPacket sendRequest(String hostport, StorageMessages.DataPacket message) {
        try {
            /**
             * Send self info and receive nodes and file detail info
             */
            String[] address = hostport.split(":");
            InetAddress ip = InetAddress.getByName(address[0]);
            int port = Integer.parseInt(address[1]);
            connectionSocket = new Socket(ip, port);
            connectionSocket.setSoTimeout(1000);
            OutputStream outstream = connectionSocket.getOutputStream();
            message.writeDelimitedTo(outstream);
            /**
             * Read list and begin to communicate with provided nodes
             */
            InputStream instream = connectionSocket.getInputStream();
            StorageMessages.DataPacket nodeListMessage = StorageMessages.DataPacket.getDefaultInstance();
            nodeListMessage = nodeListMessage.parseDelimitedFrom(instream);
            return nodeListMessage;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public String nameToSha1(String input) throws NoSuchAlgorithmException {
        MessageDigest mDigest = MessageDigest.getInstance("SHA1");
        byte[] result = mDigest.digest(input.getBytes());
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < result.length; i++) {
            sb.append(Integer.toString((result[i] & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }
}
