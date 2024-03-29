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
                OutputStream outstream = connectionSocket.getOutputStream();
                StorageMessages.DataPacket heartBeatMessage = StorageMessages.DataPacket.getDefaultInstance();
                heartBeatMessage = heartBeatMessage.parseDelimitedFrom(instream);
                if (heartBeatMessage.getType() == StorageMessages.DataPacket.packetType.HEARTBEAT) {
                    /**
                     * Check heartbeat in list
                     */
                    String host = heartBeatMessage.getHost();
                    int port = heartBeatMessage.getPort();
                    String hostport = host + ":" + port;
                    System.out.println("hosthort = " + hostport);
                    hbm.updateDataInfo(hostport, heartBeatMessage.getUsage(), heartBeatMessage.getRequestNum());

                    if  (nm.compareNodeListLength(heartBeatMessage)) {
//                    System.out.println("Receive heartBeatMessage list********************");
//                    System.out.println(heartBeatMessage);
//                    System.out.println("Receive heartBeatMessage list********************");

                    System.out.println("Seems I fails, Update my node map");
                    nm.updateNodeMap(heartBeatMessage);
                }
                    if (!nm.checkExist(hostport)) {
                            nm.addNode(hostport);
                            /**
                             * Send replication of node map to All datanodes
                             */
                            nm.BcastAllNode();
                            /**
                             * Begin re-balance process,
                             */
                            System.out.println("begin re-balance");
                            StorageMessages.DataPacket reBalance = StorageMessages.DataPacket.newBuilder().setType(StorageMessages.DataPacket.packetType.REBALANCE).setBeginRebalanceNode(hostport).setIsBroken(false).build();
                            sendSomthing(hostport, reBalance);
                            connectionSocket.close();

                    } else {
                        /**
                         * Get into heartbeat manager, update node's usage, num requests from client, and timestamp
                         */
                        System.out.println("Already have this node in list");
                        System.out.println("Update TimeStamp to " + hbm.getTimeStamp(hostport) + " Usage: " + hbm.getUsage(hostport) + " Num of Request: " + hbm.getNumRequest(hostport));
                    }

                } else if (heartBeatMessage.getType() == StorageMessages.DataPacket.packetType.CHECK_NODE_INFO) {
                    hbm.getNodeInfoPacket().writeDelimitedTo(outstream);
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
