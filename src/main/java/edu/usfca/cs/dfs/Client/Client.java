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

    public static int NODE_PORT = 8000;
    public static String NODE_HOST = "localhost";

    public static int COOR_PORT = 7000;
    public static String COOR_HOST = "localhost";

    public static boolean isDebug = false;
    public static volatile boolean isShutdown = false;

    private UI ui;
    private FileMap fm;
    final ExecutorService threads = Executors.newFixedThreadPool(4);
    private Map<String, ArrayList<String>> fileMap = new HashMap();


    public static void main(String[] args) {
        Client cl = new Client();
        if (args.length > 0) {
            if (args[0].equals("-localhost")) {
                HOST = args[1];
                System.out.println(HOST);
            }
            if (args[2].equals("-localport")) {
                PORT = Integer.parseInt(args[3]);
                System.out.println(PORT);
            }
            if (args[4].equals("-nodehost")) {
                NODE_HOST = args[5];
                System.out.println(NODE_HOST);
            }
            if (args[6].equals("-nodeport")) {
                NODE_PORT = Integer.parseInt(args[7]);
                System.out.println(NODE_PORT);
            }
            if (args[8].equals("-coorhost")) {
                COOR_HOST = args[9];
                System.out.println(COOR_HOST);
            }
            if (args[10].equals("-coorport")) {
                COOR_PORT = Integer.parseInt(args[11]);
                System.out.println(COOR_PORT);
            }
        }
        cl.beginConnection();

    }

    public void beginConnection() {
        fm = new FileMap();
        ui = new UI(threads, fm);
        threads.submit(ui);
    }


}
