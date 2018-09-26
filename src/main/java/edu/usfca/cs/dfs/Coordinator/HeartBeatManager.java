package edu.usfca.cs.dfs.Coordinator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static edu.usfca.cs.dfs.Coordinator.Coordinator.isShutdown;

public class HeartBeatManager implements Runnable {

    private Map<String,Long> timeStampMap;
    private ReentrantReadWriteLock timeStampMaplock;
    private NodeMap nm;
    private int sleepTime = 8000;

    public HeartBeatManager (NodeMap nm){
        this.nm = nm;
        timeStampMap = new HashMap<>();
        timeStampMaplock = new ReentrantReadWriteLock();
    }

    @Override
    public void run() {

                try {
                    System.out.println("Heartbeat Check");
                timeStampMaplock.readLock().lock();
                for (Map.Entry<String,Long> entry: timeStampMap.entrySet())
                {
                    if ((System.currentTimeMillis() - entry.getValue()) > sleepTime)
                    {
                        String hostport = entry.getKey();
                        System.out.println("Node " +hostport + " fails, begin removing and re-balance");
                        nm.removeNode(hostport);
                        nm.BcastAllNode();
                    }
                }
                }
                catch (Exception e)
                    {
                        e.printStackTrace();
                    }finally {
                timeStampMaplock.readLock().unlock();
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

    public Long getTimeStamp(String hostPort) {
        timeStampMaplock.readLock().lock();
        try{
            return timeStampMap.get(hostPort);
        }finally {
            timeStampMaplock.readLock().unlock();
        }

    }
}
