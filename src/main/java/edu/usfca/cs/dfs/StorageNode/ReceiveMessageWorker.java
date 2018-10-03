package edu.usfca.cs.dfs.StorageNode;

import edu.usfca.cs.dfs.CheckSum;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static edu.usfca.cs.dfs.StorageMessages.DataPacket.packetType.*;
import static edu.usfca.cs.dfs.StorageNode.StorageNode.*;

public class ReceiveMessageWorker extends Connection implements Runnable {
    private ExecutorService threads;
    private NodeMap nm;
    private FileMap fm;

    public ReceiveMessageWorker(ExecutorService threads, FileMap fm, NodeMap nm) {
        this.threads = threads;
        this.nm = nm;
        this.fm = fm;
    }

    @Override
    public void run() {
        try {
            ServerSocket welcomingSocket = new ServerSocket(PORT);
            while (!isShutdown) {
                Socket connectionSocket = welcomingSocket.accept();
                InputStream instream = connectionSocket.getInputStream();
                OutputStream outstream = connectionSocket.getOutputStream();

                StorageMessages.DataPacket requestMessage = StorageMessages.DataPacket.getDefaultInstance();
                requestMessage = requestMessage.parseDelimitedFrom(instream);

                if (requestMessage.getType() == StorageMessages.DataPacket.packetType.REQUEST) {
                    /**
                     * Type REQUEST
                     */
                    ifRequest(connectionSocket, requestMessage);
                } else if (requestMessage.getType() == StorageMessages.DataPacket.packetType.DOWNLOAD) {
                    /**
                     * Type DOWNLOAD
                     */
                    ifDownload(connectionSocket, requestMessage);
                } else if (requestMessage.getType() == StorageMessages.DataPacket.packetType.NODELIST) {
                    /**
                     * Update nodelist
                     */
                    nm.updateNodeMap(requestMessage);

                } else if (requestMessage.getType() == StorageMessages.DataPacket.packetType.DATA) {
                    /**
                     * Type DATA
                     */
                    ifData(connectionSocket, requestMessage);
                } else if (requestMessage.getType() == StorageMessages.DataPacket.packetType.REBALANCE) {
                    /**
                     * Re-BALANCE Process
                     */
                    ifRebalance(connectionSocket, requestMessage);
                } else if (requestMessage.getType() == StorageMessages.DataPacket.packetType.REBALANCE_ACK) {
                    /**
                     * Re-BALANCE ACK Process
                     */
                    ifRebalanceACK(connectionSocket, requestMessage);
                } else if (requestMessage.getType() == StorageMessages.DataPacket.packetType.DELETE_BY_NODE) {
                    /**
                     * Delete file based on node
                     */
                    ifDeleteByNode(connectionSocket, requestMessage);
                } else if (requestMessage.getType() == StorageMessages.DataPacket.packetType.FILE_META) {
                    /**
                     * Update file Metadata map
                     */
                    nm.updateFileMeta(requestMessage);
                } else if (requestMessage.getType() == StorageMessages.DataPacket.packetType.UPDATE_REPLICATION) {
                    /**
                     * Update replication map
                     */
                    System.out.println("Updating Replication Map");
//                    System.out.println(requestMessage);
                    fm.storeRebalanceFile(requestMessage);
                } else if (requestMessage.getType() == StorageMessages.DataPacket.packetType.FIX_FILE_CORRUPTION) {
                    /**
                     * Fix File Corruption
                     */
                    System.out.println("Fixing File Corruption");
                    ifFixFileCorruption(connectionSocket, requestMessage);

                }

            }
            if (isShutdown) {
                welcomingSocket.close();
            }
        } catch (IOException e) {
            System.out.println(e);

        } catch (NoSuchAlgorithmException e) {
            System.out.println(e);

        }

    }

    public void ifRequest(Socket connectionSocket, StorageMessages.DataPacket requestMessage) throws IOException, NoSuchAlgorithmException {
        OutputStream outstream = connectionSocket.getOutputStream();
        NumRequest++;
        int numChunks;
        /**
         * Check request node info in list
         */
        InetAddress host = connectionSocket.getInetAddress();
        int port = connectionSocket.getPort();
        String hostport = host + ":" + port;
        String filename = requestMessage.getFileName();
        String hashedName = nameToSha1(filename);

        if (!requestMessage.getIsDownload()) {
            /**
             * Get data from protocol buffer
             */

            if (!fm.isFileExist(filename))
                numChunks = requestMessage.getNumChunk();
            else numChunks = fm.getPieceNum(filename);
            System.out.println("Filename is " + filename);
            System.out.println("\nnumChunks is " + numChunks);

            /**
             * Reply a hashed node address
             */
            System.out.println("\nhashedName is " + hashedName);
            System.out.println(nm.pickNodeList(hashedName, numChunks));
            nm.pickNodeList(hashedName, numChunks).writeDelimitedTo(outstream);
            nm.BcastAllFileMeta();
            connectionSocket.close();

        } else {
            System.out.println("Download Request");
            System.out.println(hashedName);
            System.out.println(nm.getSingleFileMeta(hashedName));
            nm.getSingleFileMeta(hashedName).writeDelimitedTo(outstream);
            connectionSocket.close();

        }

    }

    public void ifDownload(Socket connectionSocket, StorageMessages.DataPacket requestMessage) throws IOException {
        /**
         * Retrieval and Send file chunks
         */
        CheckSum stringSum = new CheckSum();
        OutputStream outstream = connectionSocket.getOutputStream();

        String filename = requestMessage.getFileName();
        int chunkID = requestMessage.getChunkId();
        StorageMessages.DataPacket dataPacket = fm.getPiece(filename, chunkID);
        String hashedPieceSum = stringSum.hashToHexString(stringSum.hash(dataPacket.getData().toByteArray()));
        System.out.println("Download File: " + filename + " Chunk: " + chunkID);
        /**
         * Check if the file chunk is fine
         */
        if (hashedPieceSum.equals(dataPacket.getHashedPieceSum())) {
            dataPacket.writeDelimitedTo(outstream);
        } else {
            String preNode = nm.getPreNode(HOSTPORT);
            System.out.println("This chunk is broken");
            /**
             * Ask pre node for this replication and update this chunk
             */
            StorageMessages.DataPacket fileCorrpution = StorageMessages.DataPacket.newBuilder().setType(FIX_FILE_CORRUPTION).setHostport(HOSTPORT).setFileName(filename).setChunkId(chunkID).build();
            StorageMessages.DataPacket goodFileChunk = sendRequest(preNode,fileCorrpution);
            fm.addFile(filename,chunkID,goodFileChunk);
            goodFileChunk.writeDelimitedTo(outstream);
        }
        connectionSocket.close();
    }

    public void ifData(Socket connectionSocket, StorageMessages.DataPacket requestMessage) throws IOException {
        /**
         * Store Data
         */
        Boolean isReplic = requestMessage.getIsReplic();
        String filename = requestMessage.getFileName();
        int chunkId = requestMessage.getChunkId();
        if (!isReplic) {
            fm.addFile(filename, chunkId, requestMessage);
            System.out.println("*************************");
            System.out.println("Received file: " + filename + " ChunkId: " + chunkId);
            nm.replicateChunkToNodes(fm.rebuildReplicChunk(requestMessage));

        } else {
            System.out.println("*************************");
            System.out.println("Received replication chunk: " + filename + " ChunkId: " + chunkId);

            String hostport = requestMessage.getHost() + ":" + requestMessage.getPort();
            fm.addReplic(hostport, filename, requestMessage);

        }
        connectionSocket.close();
    }

    public void ifDeleteByNode(Socket connectionSocket, StorageMessages.DataPacket requestMessage) throws IOException {
        String replicNode = requestMessage.getDeleteNodeFile();
        System.out.println("delete replication node " + replicNode);

        fm.removeReplicationByNode(replicNode);
        System.out.println("Delete success");
    }

    public void ifRebalance(Socket connectionSocket, StorageMessages.DataPacket requestMessage) throws IOException {
        String nextNode = nm.getNextNode(HOSTPORT);
        String nextnextNode = nm.getNextNode(nextNode);
        String preNode = nm.getPreNode(HOSTPORT);
        String prepreNode = nm.getPreNode(preNode);
        if (!requestMessage.getIsBroken()) {
            System.out.println("New node Re_balance, I'm the leader " + HOSTPORT);

            /**
             * This is a begin node.
             * ask next node for local date as replication
             */
            StorageMessages.DataPacket request = StorageMessages.DataPacket.newBuilder().setType(REBALANCE_ACK).setIsBroken(false).setBeginRebalanceNode(HOSTPORT).build();

            StorageMessages.DataPacket replicData;
            System.out.println("next node: " + nextNode);
            if (!nextNode.equals(HOSTPORT)) {
                System.out.println("1. ask next node for local date as replication");
                replicData = sendRequest(nextNode, request);
                System.out.println(replicData);
                if (replicData != null)
                    fm.storeRebalanceFile(replicData);
                System.out.println("Completed");
            }
            /**
             * ask next next node's local date as replication
             */
            System.out.println("next next node: " + nextnextNode);

            if (!nextnextNode.equals(HOSTPORT)) {
                System.out.println("2. ask next next node's local date as replication");
                replicData = sendRequest(nextnextNode, request);
                System.out.println(replicData);
                if (replicData != null)
                    fm.storeRebalanceFile(replicData);
                System.out.println("Completed");
            }
            /**
             * ask pre node delete next next node replication
             */
            if (!preNode.equals(HOSTPORT)) {
                System.out.println("3. ask pre node " + preNode + " delete next next node replication");
                StorageMessages.DataPacket deleteReplication = StorageMessages.DataPacket.newBuilder().setType(DELETE_BY_NODE).setDeleteNodeFile(nextnextNode).build();
                sendSomthing(preNode, deleteReplication);
                System.out.println("Completed");
            }
            /**
             * ask pre pre node delete next node replication
             */
            if (!prepreNode.equals(HOSTPORT)) {

                System.out.println("4. ask pre pre node " + prepreNode + " delete next node replication");
                StorageMessages.DataPacket deleteReplication = StorageMessages.DataPacket.newBuilder().setType(DELETE_BY_NODE).setDeleteNodeFile(nextNode).build();
                sendSomthing(prepreNode, deleteReplication);
                System.out.println("Completed");
                connectionSocket.close();
            }
        } else {
            System.out.println("Broken node Re_balance, I'm the leader " + HOSTPORT);

            String brokenNode = requestMessage.getDeleteNodeFile();

            /**
             * move broken node replication to local data map and delete it from replication
             */
            System.out.println("1. move broken node replication to local data map and delete it from replication");
            fm.moveReplicationToLocal(brokenNode);
            System.out.println("Completed");

            /**
             * Ask nextnextnode send local data as replication
             */


            StorageMessages.DataPacket request = StorageMessages.DataPacket.newBuilder().setType(REBALANCE_ACK).setIsBroken(true).setBeginRebalanceNode(HOSTPORT).build();

            StorageMessages.DataPacket replicData;
            System.out.println("nextnext node: " + nextnextNode);
            if (!nextnextNode.equals(HOSTPORT)) {
                System.out.println("2. Ask nextnextnode for local data as replication");
                replicData = sendRequest(nextnextNode, request);
//                System.out.println("Receive *********\n" + replicData);
                if (replicData.getRebalanceReplicDataList() != null)
                    fm.storeRebalanceFile(replicData);
                System.out.println("Completed");
            }

            /**
             * send nextnode local data to prenode as replication (find replication in replicMap and send directly)
             */
            if (!preNode.equals(HOSTPORT)) {
                System.out.println("3. send nextnode " + nextNode + " to update prenode's " + preNode + " replication(find replication in replicMap and send directly)");

                StorageMessages.DataPacket rebalanceFilePacket = fm.buildNextReplicationPacket(nextNode, brokenNode);
                System.out.println("rebalanceFilePacket built success");
                sendSomthing(preNode, rebalanceFilePacket);
                System.out.println("Completed");
            }


            /**
             * send localdate to preprenode and prenode as replication, if there is broken node replications, just delete
             */
            System.out.println("4. send localdate to preprenode and prenode as replication, if there is broken node replications, just delete");
            StorageMessages.DataPacket rebalanceFilePacket = fm.buildLocalReplicationPacket(brokenNode);
            sendSomthing(preNode, rebalanceFilePacket);
            sendSomthing(prepreNode, rebalanceFilePacket);
            System.out.println("Completed");

            /**
             * Update file Metadata map and broadcast
             */
            System.out.println("5. Update file Metadata map and broadcast");
            nm.remapMetadata(brokenNode, HOSTPORT);
            System.out.println("Completed");

        }


    }

    public void ifRebalanceACK(Socket connectionSocket, StorageMessages.DataPacket requestMessage) throws IOException {
        /**
         * This is follower, do what begin node request
         */
        System.out.println("Re_balance, I'm the follower " + HOSTPORT);
        OutputStream outstream = connectionSocket.getOutputStream();

        String leaderHostPort = requestMessage.getBeginRebalanceNode();
        String leaderHashVal = nm.getHashval(leaderHostPort);
        StorageMessages.DataPacket rebalanceFilePacket = fm.buildLocalDataPacket(leaderHashVal);
        System.out.println(rebalanceFilePacket);
        rebalanceFilePacket.writeDelimitedTo(outstream);
        connectionSocket.close();
    }

    public void ifFixFileCorruption(Socket connectionSocket, StorageMessages.DataPacket requestMessage) throws IOException {

        OutputStream outstream = connectionSocket.getOutputStream();
        String filename = requestMessage.getFileName();
        int chunkId = requestMessage.getChunkId();
        String hostport = requestMessage.getHostport();

        StorageMessages.DataPacket goodFileChunk = fm.getGoodFileChunkFromReplication(hostport,filename,chunkId);
        goodFileChunk.writeDelimitedTo(outstream);
        connectionSocket.close();
    }

}
