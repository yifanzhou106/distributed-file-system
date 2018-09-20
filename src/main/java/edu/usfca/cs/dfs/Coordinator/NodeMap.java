package edu.usfca.cs.dfs.Coordinator;

import edu.usfca.cs.dfs.StorageMessages;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class NodeMap extends Connection{
    private TreeMap<String, String> hostHashMap;
    private Map<Integer, String> hashLocation = new HashMap<>();
    private ReentrantReadWriteLock nodemaplock;

    public NodeMap (){
        hostHashMap = new TreeMap();
        nodemaplock = new ReentrantReadWriteLock();
        /**
         * Hard code first 12 nodes location to make the ring balance
         */
        hashLocation.put(1,"0");
        hashLocation.put(2,"8000000000000000000000000000000000000000");
        hashLocation.put(3,"4000000000000000000000000000000000000000");
        hashLocation.put(4,"1555555555555555555555555555555555555555");
        hashLocation.put(5,"2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        hashLocation.put(6,"5555555555555555555555555555555555555554");
        hashLocation.put(7,"6aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa9");
        hashLocation.put(8,"9555555555555555555555555555555555555553");
        hashLocation.put(9,"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa8");
        hashLocation.put(10,"bffffffffffffffffffffffffffffffffffffffd");
        hashLocation.put(11,"d555555555555555555555555555555555555552");
        hashLocation.put(12,"eaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa7");
    }

    public boolean checkExist(String hostport){
        nodemaplock.readLock().lock();
        try{
        if (hostHashMap.containsValue(hostport))
            return true;
        return false;
        }finally{
            nodemaplock.readLock().unlock();
        }
    }

    public void addNode (String hostport){
        nodemaplock.writeLock().lock();
        String hashVal;
        try{
            if (hostHashMap.size() <= 12)
                hashVal = hashLocation.get(hostHashMap.size() +1);
            else{
               do {
                   hashVal = nodeToSha1(hostport);
               } while (hostHashMap.containsKey(hashVal));
            }
            hostHashMap.put(hashVal, hostport);

            System.out.println(hostHashMap);
        }catch(NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        finally {
            nodemaplock.writeLock().unlock();
        }
    }

    public StorageMessages.DataPacket getNodeList (){
        nodemaplock.readLock().lock();
        try{
            StorageMessages.DataPacket.Builder nodeListPacket = StorageMessages.DataPacket.newBuilder();
            for (Map.Entry<String,String> entry :hostHashMap.entrySet())
            {
                StorageMessages.NodeHash hashedNode = StorageMessages.NodeHash.newBuilder().setHashVal(entry.getKey()).setHostPort(entry.getValue()).build();
                nodeListPacket.addNodeList(hashedNode);
            }
            nodeListPacket.setType(StorageMessages.DataPacket.packetType.NODELIST);
            return nodeListPacket.build();
        }finally {
            nodemaplock.readLock().unlock();

        }

    }

    public void BcastAllNode(){
        nodemaplock.readLock().lock();
        try{
            StorageMessages.DataPacket nodeListPacket = getNodeList();
            for (Map.Entry<String,String> entry :hostHashMap.entrySet())
            {
                String hostport = entry.getValue();
                sendSomthing(hostport, nodeListPacket);
            }
        }finally {
            nodemaplock.readLock().unlock();

        }
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
}
