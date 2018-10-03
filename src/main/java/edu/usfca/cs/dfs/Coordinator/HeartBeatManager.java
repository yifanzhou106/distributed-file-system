package edu.usfca.cs.dfs.Coordinator;

import edu.usfca.cs.dfs.StorageMessages;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class HeartBeatManager extends Connection implements Runnable {

    private Map<String, Map<String, Object>> nodeInfo;

    private ReentrantReadWriteLock timeStampMaplock;
    private NodeMap nm;
    private int sleepTime = 8000;

    public HeartBeatManager(NodeMap nm) {
        this.nm = nm;
        nodeInfo = new HashMap<>();
        timeStampMaplock = new ReentrantReadWriteLock();
    }

    @Override
    public void run() {

        try {
            System.out.println("Heartbeat Check");
            timeStampMaplock.writeLock().lock();
            for (Map.Entry<String,  Map<String, Object>> entry : nodeInfo.entrySet()) {
                Map<String, Object> nodeinfo = entry.getValue();
                Long timestamp = (Long)nodeinfo.get("timestamp");
                if ((System.currentTimeMillis() - timestamp) > sleepTime) {
                    String hostport = entry.getKey();
                    System.out.println("Node " + hostport + " fails, begin removing and re-balance");
                    String preNode = nm.getPreNode(hostport);
                    nm.removeNode(hostport);
                    nm.BcastAllNode();

                    StorageMessages.DataPacket reBalance = StorageMessages.DataPacket.newBuilder().setType(StorageMessages.DataPacket.packetType.REBALANCE).setBeginRebalanceNode(hostport).setIsBroken(true).setDeleteNodeFile(hostport).build();
                    sendSomthing(preNode, reBalance);
                    nodeInfo.remove(hostport);
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("Heartbeat error");
            e.printStackTrace();
        } finally {
            timeStampMaplock.writeLock().unlock();
        }
    }

    public void updateDataInfo(String hostPort, int usage, int numRequest) {
        timeStampMaplock.writeLock().lock();
        try {
            Map<String, Object> nodeinfo = new HashMap<>();
            nodeinfo.put("timestamp",System.currentTimeMillis() );
            nodeinfo.put("numRequest",numRequest );
            nodeinfo.put("usage",usage );

            nodeInfo.put(hostPort,nodeinfo );

        } finally {
            timeStampMaplock.writeLock().unlock();
        }
    }

    public Long getTimeStamp(String hostPort) {
        timeStampMaplock.readLock().lock();
        try {
            Map<String, Object> nodeinfo = nodeInfo.get(hostPort);
            return (Long) nodeinfo.get("timestamp");
        } finally {
            timeStampMaplock.readLock().unlock();
        }
    }
    public int getUsage(String hostPort) {
        timeStampMaplock.readLock().lock();
        try {
            Map<String, Object> nodeinfo = nodeInfo.get(hostPort);
            return (int) nodeinfo.get("usage");
        } finally {
            timeStampMaplock.readLock().unlock();
        }
    }
    public int getNumRequest(String hostPort) {
        timeStampMaplock.readLock().lock();
        try {
            Map<String, Object> nodeinfo = nodeInfo.get(hostPort);
            return (int) nodeinfo.get("numRequest");
        } finally {
            timeStampMaplock.readLock().unlock();
        }
    }

    public StorageMessages.DataPacket getNodeInfoPacket (){
        timeStampMaplock.readLock().lock();
        try {
            StorageMessages.DataPacket.Builder nodeInfoPacket = StorageMessages.DataPacket.newBuilder();
            for (Map.Entry<String, Map<String, Object>> entry : nodeInfo.entrySet())
            {
                Map<String, Object> nodeinfo = entry.getValue();
                int usage = (int) nodeinfo.get("usage");
                int numRequest = (int) nodeinfo.get("numRequest");
                StorageMessages.NodeHash singleNodeInfo =StorageMessages.NodeHash.newBuilder().setHostPort(entry.getKey()).setUsage(usage).setNumRequest(numRequest).build();
                nodeInfoPacket.addNodeList(singleNodeInfo);
            }
            return nodeInfoPacket.build();
        } finally {
            timeStampMaplock.readLock().unlock();
        }
    }
}
