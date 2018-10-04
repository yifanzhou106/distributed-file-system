package edu.usfca.cs.dfs.StorageNode;

import edu.usfca.cs.dfs.StorageMessages;

import javax.sound.sampled.Port;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static edu.usfca.cs.dfs.StorageNode.StorageNode.*;

public class NodeMap extends Connection {
    private TreeMap<String, String> hostHashMap;
    private ReentrantReadWriteLock nodemaplock;
    private int numReplic = 2;
    private HashMap<String, StorageMessages.DataPacket> fileMeta;
    private ReentrantReadWriteLock fileMetalock;


    public NodeMap() {
        hostHashMap = new TreeMap();
        fileMeta = new HashMap<>();
        nodemaplock = new ReentrantReadWriteLock();
        fileMetalock = new ReentrantReadWriteLock();
    }


    public boolean checkExist(String hostport) {
        nodemaplock.readLock().lock();
        try {
            if (hostHashMap.containsValue(hostport))
                return true;
        } finally {
            nodemaplock.readLock().unlock();
            return false;
        }
    }

    public void updateNodeMap(StorageMessages.DataPacket requestMessage) {
        nodemaplock.writeLock().lock();
        try {
            List nodelist = requestMessage.getNodeListList();
            System.out.println(nodelist);
            hostHashMap.clear();
            for (int i = 0; i < nodelist.size(); i++) {
                StorageMessages.NodeHash nodeHash = (StorageMessages.NodeHash) nodelist.get(i);
                hostHashMap.put(nodeHash.getHashVal(), nodeHash.getHostPort());
            }
            System.out.println("Node map updated " + hostHashMap);
        } finally {
            nodemaplock.writeLock().unlock();
        }
    }

    public StorageMessages.DataPacket pickNodeList(String hashedName, int numChunks) {
        nodemaplock.readLock().lock();
        fileMetalock.writeLock().lock();
        try {
            StorageMessages.DataPacket.Builder nodeListPacket = StorageMessages.DataPacket.newBuilder();
            int i = findBeginLocation(hashedName);

            nodeListPacket = getNode(nodeListPacket, i, numChunks);
            nodeListPacket.setType(StorageMessages.DataPacket.packetType.NODELIST).setNumChunk(numChunks).setFileName(hashedName);
            fileMeta.put(hashedName, nodeListPacket.build());
            return nodeListPacket.build();
        } finally {
            nodemaplock.readLock().unlock();
            fileMetalock.writeLock().unlock();
        }
    }

    public void BcastAllFileMeta() {
        nodemaplock.readLock().lock();
        try {
            StorageMessages.DataPacket fileMetaPacket = getFileMetaPacket();
            for (Map.Entry<String, String> entry : hostHashMap.entrySet()) {
                String hostport = entry.getValue();
                sendSomthing(hostport, fileMetaPacket);
            }
        } finally {
            nodemaplock.readLock().unlock();

        }
    }

    public StorageMessages.DataPacket getFileMetaPacket() {
        fileMetalock.readLock().lock();
        try {
            StorageMessages.DataPacket.Builder fileMetaDataPacket = StorageMessages.DataPacket.newBuilder();
            for (Map.Entry<String, StorageMessages.DataPacket> entry : fileMeta.entrySet()) {
                fileMetaDataPacket.addFileMetaData(entry.getValue());
            }
            fileMetaDataPacket.setType(StorageMessages.DataPacket.packetType.FILE_META);
            return fileMetaDataPacket.build();
        } finally {
            fileMetalock.readLock().unlock();
        }
    }

    public void updateFileMeta(StorageMessages.DataPacket requestMessage) {
        fileMetalock.writeLock().lock();
        try {
            List fileMetaDataList = requestMessage.getFileMetaDataList();
            fileMeta.clear();
            for (int i = 0; i < fileMetaDataList.size(); i++) {
                StorageMessages.DataPacket singleMetaData = (StorageMessages.DataPacket) fileMetaDataList.get(i);
                fileMeta.put(singleMetaData.getFileName(), singleMetaData);
            }
            System.out.println("File Metadata map updated " + fileMeta);
        } finally {
            fileMetalock.writeLock().unlock();
        }
    }

    public StorageMessages.DataPacket getSingleFileMeta(String hashname) {
        fileMetalock.readLock().lock();
        try {
            return fileMeta.get(hashname);
        } finally {
            fileMetalock.readLock().unlock();
        }
    }

    public void remapMetadata(String brokenNode, String goodnode) {
        fileMetalock.writeLock().lock();
        try {
            StorageMessages.NodeHash goodNode = StorageMessages.NodeHash.newBuilder().setHostPort(goodnode).setHashVal(getHashval(goodnode)).build();

            for (Map.Entry<String, StorageMessages.DataPacket> entry : fileMeta.entrySet()) {
                StorageMessages.DataPacket singleMetaData = entry.getValue();
                StorageMessages.DataPacket.Builder newSingleMetaData = StorageMessages.DataPacket.newBuilder();
                List<StorageMessages.NodeHash> nodelist = singleMetaData.getNodeListList();
                int i = 0;
                String hashedName = singleMetaData.getFileName();

                ListIterator<StorageMessages.NodeHash> iterator = nodelist.listIterator();
                while (iterator.hasNext()) {
                    StorageMessages.NodeHash nodeHash = iterator.next();
                    String hostPort = nodeHash.getHostPort();
                    if (hostPort.equals(brokenNode)) {
                        nodeHash = goodNode;
                    }
                    newSingleMetaData.addNodeList(nodeHash);
                    i++;
                }

                newSingleMetaData.setType(StorageMessages.DataPacket.packetType.NODELIST).setNumChunk(i).setFileName(hashedName);
                fileMeta.put(singleMetaData.getFileName(), newSingleMetaData.build());
            }
            System.out.println("Replace all broken node" + brokenNode + " to " + goodnode);
            System.out.println("Broadcast to all");
            BcastAllFileMeta();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileMetalock.writeLock().unlock();
        }

    }

    public void deleteFileFromMetadaMap(String hashedFilename) {
        fileMetalock.writeLock().lock();
        try {
            if (fileMeta.containsKey(hashedFilename))
                fileMeta.remove(hashedFilename);

        } finally {
            fileMetalock.writeLock().unlock();
        }

    }


    public StorageMessages.DataPacket.Builder getNode(StorageMessages.DataPacket.Builder nodeListPacket, int begin, int numChunks) {
        int j = 0;
        int chunkCount = 0;
        while (chunkCount < numChunks) {
            for (Map.Entry<String, String> entry : hostHashMap.entrySet()) {
                if (chunkCount < numChunks) {
                    if (j >= begin) {
                        /**
                         * if this chunk contains
                         */
                        StorageMessages.NodeHash hashedNode = StorageMessages.NodeHash.newBuilder().setHashVal(entry.getKey()).setHostPort(entry.getValue()).build();
                        nodeListPacket.addNodeList(hashedNode);
                        chunkCount++;
                    }
                    j++;
                } else break;
            }
        }

        return nodeListPacket;
    }

    public String nodeToSha1(String input) throws NoSuchAlgorithmException {
        MessageDigest mDigest = MessageDigest.getInstance("SHA1");
        byte[] result = mDigest.digest(input.getBytes());
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < result.length; i++) {
            sb.append(Integer.toString((result[i] & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }

    public void replicateChunkToNodes(StorageMessages.DataPacket requestMessage) {

        nodemaplock.readLock().lock();
        try {
            String hostport = HOST + ":" + PORT;
            /**
             * send to pre node
             */
            String preNodeKey = findPreviousNodeKey(hostport);
            System.out.println("send replication " + requestMessage.getChunkId() + " to pre node" + preNodeKey);
            sendSomthing(hostHashMap.get(preNodeKey), requestMessage);
            /**
             * send to pre pre node
             */
            preNodeKey = findPreviousNodeKey(hostHashMap.get(preNodeKey));
            System.out.println("send replication " + requestMessage.getChunkId() + " to pre pre node" + preNodeKey);

            sendSomthing(hostHashMap.get(preNodeKey), requestMessage);

        } finally {
            nodemaplock.readLock().unlock();
        }
    }

    public int findBeginLocation(String hashVal) {
        int i = 0;
        for (Map.Entry<String, String> entry : hostHashMap.entrySet()) {
            if (entry.getKey().compareTo(hashVal) >= 0) {
                break;
            }
            i++;
        }
        System.out.println("The node bigger than file is " + i);
        if (i > hostHashMap.size()) {
            System.out.println("No node bigger than file, start from 0");
            i = 0;
        }
        return i;
    }

    public String findNextNodeKey(String hostport) {
        nodemaplock.readLock().lock();
        try {
            String nextNode = "";
            int i = 0;
            for (Map.Entry<String, String> entry : hostHashMap.entrySet()) {
                if (entry.getValue().equals(hostport)) {
                    break;
                }
                i++;
            }
            i++;
            if (i == hostHashMap.size())
                i = 0;

            int j = 0;
            for (Map.Entry<String, String> entry : hostHashMap.entrySet()) {
                if (i == j) {
                    nextNode = entry.getKey();
                    break;
                }
                j++;
            }
            return nextNode;
        } finally {
            nodemaplock.readLock().unlock();
        }
    }

    public String findPreviousNodeKey(String hostport) {
        nodemaplock.readLock().lock();
        try {
            String preNode = "";
            for (Map.Entry<String, String> entry : hostHashMap.entrySet()) {
                if (entry.getValue().equals(hostport)) {
                    break;
                }
                preNode = entry.getKey();
            }
            if (preNode.equals("")) {
                preNode = hostHashMap.lastEntry().getKey();
            }
            return preNode;
        } finally {
            nodemaplock.readLock().unlock();
        }
    }

    public void BcastDeleteFile(StorageMessages.DataPacket deleteFile) {
        nodemaplock.readLock().lock();
        try {
            for (Map.Entry<String, String> entry : hostHashMap.entrySet()) {
                String hostport = entry.getValue();
                if (!hostport.equals(HOSTPORT))
                    sendSomthing(hostport, deleteFile);
            }

        } finally {
            nodemaplock.readLock().unlock();
        }
    }


    public String getNextNode(String hostport) {
        return hostHashMap.get(findNextNodeKey(hostport));
    }

    public String getPreNode(String hostport) {
        return hostHashMap.get(findPreviousNodeKey(hostport));
    }

    public String getHashval(String hostport) {
        String hashVal = "";
        for (Map.Entry<String, String> entry : hostHashMap.entrySet()) {
            if (entry.getValue().equals(hostport)) {
                hashVal = entry.getKey();
                break;
            }
        }
        return hashVal;
    }

    public int getNodeNum() {
        nodemaplock.readLock().lock();
        try {
            return hostHashMap.size();
        } finally {
            nodemaplock.readLock().unlock();
        }
    }
}
