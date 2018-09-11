package edu.usfca.cs.dfs.Coordinator;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Coordinator {
    public static String HOST = "localhost";
    public static int PORT = 6000;

    public static boolean isDebug = false;
    public static volatile boolean isShutdown = false;

    final ExecutorService threads = Executors.newFixedThreadPool(4);

    private NodeMap nm = new NodeMap();
    private HeartBeatManager hbm = new HeartBeatManager();

    public static void main(String[] args) {
        Coordinator coo = new Coordinator();
        coo.startPlay();
        System.out.println("Starting coordinator...");

    }

    public void startPlay (){
        threads.submit(hbm);
        threads.submit(new ReceiveMessageWorker(threads,nm,hbm));

    }


}

