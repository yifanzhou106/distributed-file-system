package edu.usfca.cs.dfs.StorageNode;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StorageNode {
    public static String HOST = "localhost";
    public static int PORT = 8400;
    public static String COORDINATOR_HOST = "localhost";
    public static int COORDINATOR_PORT = 7000;
    public static String HOSTPORT;

    public static boolean isDebug = false;
    public static volatile boolean isShutdown = false;
    public static volatile int USAGE = 0;
    public static volatile int NumRequest = 0;
    private NodeMap nm = new NodeMap();
    private FileMap fm = new FileMap();
    private HeartBeatMessage hbm = new HeartBeatMessage(fm);

    final ScheduledExecutorService heartBeatService = Executors.newSingleThreadScheduledExecutor();
    final ExecutorService threads = Executors.newFixedThreadPool(4);
    private UI ui = new UI(threads, fm);

    public static void main(String[] args)
            throws Exception {
        String hostname = getHostname();
        System.out.println("Starting storage node on " + hostname + "...");
        if (args.length > 0) {
            if (args[0].equals("-localhost")) {
                HOST = args[1];
                System.out.println(HOST);
            }
            if (args[2].equals("-localport")) {
                PORT = Integer.parseInt(args[3]);
                System.out.println(PORT);
            }
            if (args[4].equals("-coorhost")) {
                COORDINATOR_HOST = args[5];
                System.out.println(COORDINATOR_HOST);
            }
            if (args[6].equals("-coorport")) {
                COORDINATOR_PORT = Integer.parseInt(args[7]);
                System.out.println(COORDINATOR_PORT);
            }
        }
        HOSTPORT = HOST + ":" + PORT;

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

    public void startPlay() {
        threads.submit(ui);
        heartBeatService.scheduleAtFixedRate(hbm, 0, 5000, TimeUnit.MILLISECONDS);
        threads.submit(new ReceiveMessageWorker(threads, fm, nm));

    }

}
