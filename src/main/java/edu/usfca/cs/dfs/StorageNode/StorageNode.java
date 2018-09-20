package edu.usfca.cs.dfs.StorageNode;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StorageNode {
    public static String HOST = "localhost";
    public static int PORT = 8200;
    public static String COORDINATOR_HOST = "localhost";
    public static int COORDINATOR_PORT = 7000;

    public static boolean isDebug = false;
    public static volatile boolean isShutdown = false;
    public static volatile int USAGE = 0;
    public static volatile int NumRequest = 0;
    private HeartBeatMessage hbm = new HeartBeatMessage();
    private NodeMap nm = new NodeMap();
    private FileMap fm = new FileMap();

    final ScheduledExecutorService heartBeatService = Executors.newSingleThreadScheduledExecutor();
    final ExecutorService threads = Executors.newFixedThreadPool(4);

    public static void main(String[] args) 
    throws Exception {
        String hostname = getHostname();
        System.out.println("Starting storage node on " + hostname + "...");
        StorageNode sn = new StorageNode();
        sn.startPlay();

    }

    /**
     * Retrieves the short host name of the current host.
     *
     * @return name of the current host
     */
    private static String getHostname()
    throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }

    public void startPlay (){
        heartBeatService.scheduleAtFixedRate(hbm,0,5000, TimeUnit.MILLISECONDS);
        threads.submit(new ReceiveMessageWorker(threads,fm,nm));

    }

}
