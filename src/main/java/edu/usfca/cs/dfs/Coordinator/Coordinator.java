package edu.usfca.cs.dfs.Coordinator;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Coordinator {
    public static String HOST = "localhost";
    public static int PORT = 7000;

    public static boolean isDebug = false;
    public static volatile boolean isShutdown = false;
    final ScheduledExecutorService heartBeatService = Executors.newSingleThreadScheduledExecutor();

    final ExecutorService threads = Executors.newFixedThreadPool(4);

    private NodeMap nm = new NodeMap();
    private HeartBeatManager hbm = new HeartBeatManager();

    public static void main(String[] args) {
        Coordinator coo = new Coordinator();
        coo.startPlay();
        System.out.println("Starting coordinator...");

    }

    public void startPlay (){
        heartBeatService.scheduleAtFixedRate(hbm,0,10000, TimeUnit.MILLISECONDS);
        threads.submit(new ReceiveMessageWorker(threads,nm,hbm));

    }


}

