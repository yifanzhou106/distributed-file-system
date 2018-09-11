package edu.usfca.cs.dfs.Client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Client {
    public static String HOST = "localhost";
    public static int PORT = 6200;

    public static int NODE_PORT = 7600;
    public static String NODE_HOST = "localhost";
    public static boolean isDebug = false;
    public static volatile boolean isShutdown = false;

    private UI ui;
    private FileMap fm;
    final ExecutorService threads = Executors.newFixedThreadPool(4);
    private Map<String, ArrayList<String>> fileMap = new HashMap();


    public static void main(String[] args) {
        Client cl = new Client();
        cl.beginConnection();

    }

    public void beginConnection(){
        fm = new FileMap();
        ui = new UI(threads, fm);
        threads.submit(ui);
    }



}
