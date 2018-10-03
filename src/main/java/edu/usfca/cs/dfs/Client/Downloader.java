package edu.usfca.cs.dfs.Client;

import edu.usfca.cs.dfs.CheckSum;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static edu.usfca.cs.dfs.Client.Client.NODE_HOST;
import static edu.usfca.cs.dfs.Client.Client.NODE_PORT;
import static edu.usfca.cs.dfs.Client.Client.isDebug;
import static edu.usfca.cs.dfs.StorageMessages.DataPacket.packetType.DOWNLOAD;
import static edu.usfca.cs.dfs.StorageMessages.DataPacket.packetType.REQUEST;


public class Downloader extends FileManager implements Runnable {
    private ExecutorService threads;
    private String filename;
    private FileMap fm;
    private CountDownLatch countdowntimer;


    public Downloader(ExecutorService threads, FileMap fm, String filename) {
        this.threads = threads;
        this.fm = fm;
        this.filename = filename;
    }

    @Override
    public void run() {
        try {
            CheckSum stringSum = new CheckSum();

            InetAddress ip = InetAddress.getByName(NODE_HOST);
            String hostPort = NODE_HOST + ":" + NODE_PORT;
            StorageMessages.DataPacket helloMessage = StorageMessages.DataPacket.newBuilder().setType(REQUEST).setIsDownload(true).setFileName(filename).build();
            StorageMessages.DataPacket nodeListMessage = sendRequest(hostPort, helloMessage);
            List nodeList = nodeListMessage.getNodeListList();
            System.out.println(nodeList);
            int numChunk = nodeListMessage.getNumChunk();

            countdowntimer = new CountDownLatch(numChunk);
            for (int i = 0; i < nodeList.size(); i++) {
                StorageMessages.NodeHash nodeHash = (StorageMessages.NodeHash) nodeList.get(i);
                hostPort = nodeHash.getHostPort();
                threads.submit(new downLoadParallel(hostPort, filename,i ,countdowntimer, fm));
            }
            countdowntimer.await();


            /**
             * Receive all pieces and begin to combine all pieces together
             */
            System.out.println("Download Finished");
            byte[] byteValue = fm.getFile(filename, numChunk);

            if (isDebug) {
                String string = new String(byteValue);
                System.out.println("\nReceived message is:");
                System.out.println(string);
            } else {
                String newFileName = getNewFileName(filename);
                System.out.println(newFileName);
                String[] suffix = filename.split("\\.");
                if (suffix[1].equals("jpg"))
                    storeImage(newFileName, byteValue);
                else
                    storeVideo(newFileName, byteValue);

                System.out.println("Store " + newFileName + " successfully");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
