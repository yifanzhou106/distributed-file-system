package edu.usfca.cs.dfs.Coordinator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static edu.usfca.cs.dfs.Coordinator.Coordinator.isShutdown;

public class HeartBeatManager implements Runnable {

    private Map<String,Long> timeStampMap;
    private ReentrantReadWriteLock timeStampMaplock;
    private ReentrantReadWriteLock timetowakeup;
    private int checkPeriod = 10000;

    public HeartBeatManager (){
        timeStampMap = new HashMap<>();
        timeStampMaplock = new ReentrantReadWriteLock();
    }

    @Override
    public void run() {
        try {
            while (!isShutdown)
            {
                timetowakeup.wait(checkPeriod);
                for (Map.Entry<String,Long> entry: timeStampMap.entrySet())
                {
                    if ((System.currentTimeMillis()-entry.getValue())>checkPeriod)
                    {
                        /**
                         * this node fails, remove this node and tell nodes to fix this problem.
                         */
                    }
                }


            }
        }
        catch (Exception e)
        {

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
