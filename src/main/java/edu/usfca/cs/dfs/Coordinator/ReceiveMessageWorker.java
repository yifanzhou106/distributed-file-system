package edu.usfca.cs.dfs.Coordinator;

import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.ExecutorService;

import static edu.usfca.cs.dfs.Coordinator.Coordinator.*;

public class ReceiveMessageWorker extends Connection implements Runnable {
    private ExecutorService threads;
    private NodeMap nm;
    private HeartBeatManager hbm;

    public ReceiveMessageWorker(ExecutorService threads, NodeMap nm, HeartBeatManager hbm) {
        this.threads = threads;
        this.nm = nm;
        this.hbm = hbm;
    }
    @Override
    public void run() {
        try {
            ServerSocket welcomingSocket = new ServerSocket(PORT);
            while (!isShutdown) {
                Socket connectionSocket = welcomingSocket.accept();
                InputStream instream = connectionSocket.getInputStream();
                StorageMessages.DataPacket heartBeatMessage = StorageMessages.DataPacket.getDefaultInstance();
                heartBeatMessage = heartBeatMessage.parseDelimitedFrom(instream);
                if (heartBeatMessage.getType() == StorageMessages.DataPacket.packetType.HEARTBEAT)
                {
                    /**
                     * Check heartbeat in list
                     */
                    String host = heartBeatMessage.getHost();
                    int port = heartBeatMessage.getPort();
                    String hostport = host+":"+port;
                    System.out.println("hosthort = "+ hostport);
                    if (!nm.checkExist(hostport))
                    {
                        nm.addNode(hostport);
                        hbm.updateTimestamp(hostport);
                        /**
                         * Send replication of node map to first datanode,then pass around finally receive a Ack from the last datanode
                         */
//                        sendSomthing(hostport, nm.getNodeList());
                    }
                    else {
                        /**
                         * Get into heartbeat manager, update node's usage, num requests from client, and timestamp
                         */
                        System.out.println("Already have this node in list");
                    }

                }


            }
            if (isShutdown) {
                welcomingSocket.close();
            }
        } catch (IOException e) {
            System.out.println(e);

        }

    }
}
