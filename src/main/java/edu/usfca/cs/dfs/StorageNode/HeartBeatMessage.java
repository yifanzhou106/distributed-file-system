package edu.usfca.cs.dfs.StorageNode;

import edu.usfca.cs.dfs.StorageMessages;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static edu.usfca.cs.dfs.Coordinator.Coordinator.isShutdown;
import static edu.usfca.cs.dfs.StorageMessages.DataPacket.packetType.*;
import static edu.usfca.cs.dfs.StorageNode.StorageNode.*;

public class HeartBeatMessage extends Connection implements Runnable {

    private Map<String,Long> timeStampMap;
    private ReentrantReadWriteLock timeStampMaplock;

    public HeartBeatMessage(){
        timeStampMap = new HashMap<>();
        timeStampMaplock = new ReentrantReadWriteLock();
    }

    @Override
    public void run() {
        try {
            if (!isShutdown)
            {
                String hostPort = COORDINATOR_HOST+":"+COORDINATOR_PORT;
//                System.out.println(hostPort);
                StorageMessages.DataPacket heartBeatMessage = StorageMessages.DataPacket.newBuilder().setType(HEARTBEAT).setHost(HOST).setPort(PORT).setRequestNum(NumRequest).setUsage(USAGE).build();
                sendSomthing(hostPort, heartBeatMessage);
//                System.out.println("Send a heartbeat");
            }
        }
        catch (Exception e)
        {
                e.printStackTrace();
        }
    }

    public void updateTimestamp (String hostPort){
        timeStampMaplock.writeLock().lock();
        try{
            timeStampMap.put(hostPort,System.currentTimeMillis());

        }finally {
            timeStampMaplock.writeLock().unlock();
        }

    }
}
