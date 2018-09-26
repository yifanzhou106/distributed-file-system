package edu.usfca.cs.dfs.StorageNode;

import edu.usfca.cs.dfs.StorageMessages;

import javax.sound.sampled.Port;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static edu.usfca.cs.dfs.StorageNode.StorageNode.*;

public class NodeMap extends Connection{
    private TreeMap<String, String> hostHashMap;
    private ReentrantReadWriteLock nodemaplock;
    private int numReplic = 2;

    public NodeMap() {
        hostHashMap = new TreeMap();
        nodemaplock = new ReentrantReadWriteLock();
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

    public void updateNodeMap(List nodeList) {
        nodemaplock.writeLock().lock();
        try {
            hostHashMap.clear();
            for (int i =0; i< nodeList.size();i++)
            {
                StorageMessages.NodeHash  nodeHash =(StorageMessages.NodeHash ) nodeList.get(i);
                hostHashMap.put(nodeHash.getHashVal(),nodeHash.getHostPort());
            }
            System.out.println("Node map updated "+hostHashMap);
        } finally {
            nodemaplock.writeLock().unlock();
        }
    }

    public StorageMessages.DataPacket pickNodeList(String hashedName, int numChunks) {
        nodemaplock.readLock().lock();
        try {
            StorageMessages.DataPacket.Builder nodeListPacket = StorageMessages.DataPacket.newBuilder();
            int i = findBeginLocation(hashedName);

            nodeListPacket = getNode(nodeListPacket, i, numChunks);
            nodeListPacket.setType(StorageMessages.DataPacket.packetType.NODELIST).setNumChunk(numChunks);
            return nodeListPacket.build();
        } finally {
            nodemaplock.readLock().unlock();
        }
    }

    public StorageMessages.DataPacket.Builder getNode(StorageMessages.DataPacket.Builder nodeListPacket, int begin, int numChunks) {
        int j = 0;
        int chunkCount = 0;
        while (chunkCount < numChunks) {
            for (Map.Entry<String, String> entry : hostHashMap.entrySet()) {
                if (chunkCount < numChunks) {
                    if (j >= begin) {
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

    public void replicateChunkToNodes (StorageMessages.DataPacket requestMessage){

        nodemaplock.readLock().lock();
        try {
            int i = 0;
            String hostport = HOST + ":" + PORT;
            for (Map.Entry<String, String> entry : hostHashMap.entrySet()) {
                if (entry.getValue().equals(hostport)) {
                    break;
                }
                i++;
            }
            int j = 0;
            int chunkCount = 0;
            while (chunkCount < numReplic) {
                for (Map.Entry<String, String> entry : hostHashMap.entrySet()) {
                    if (chunkCount < numReplic) {
                        if (j > i) {
                            sendSomthing(entry.getValue(), requestMessage);
                            chunkCount++;
                        }
                        j++;
                    } else break;
                }
            }

        } finally {
            nodemaplock.readLock().unlock();
        }
    }

    public int findBeginLocation (String hashVal)
    {
        int i = 0;
        for (Map.Entry<String, String> entry : hostHashMap.entrySet()) {
            if (entry.getKey().compareTo(hashVal) >= 0) {
                break;
            }
            i++;
        }
        System.out.println("The node bigger than file is " + i);
        if (i > hostHashMap.size())
        {
            System.out.println("No node bigger than file, start from 0");
            i = 0;
        }
        return i;
    }
}
