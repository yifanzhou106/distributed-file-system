package edu.usfca.cs.dfs.Client;


import com.sun.tools.doclets.formats.html.SourceToHTMLConverter;
import edu.usfca.cs.dfs.StorageMessages;

import javax.imageio.ImageIO;

import java.awt.image.BufferedImage;
import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.concurrent.CountDownLatch;

import static edu.usfca.cs.dfs.Client.Client.*;
import static edu.usfca.cs.dfs.StorageMessages.DataPacket.packetType.DOWNLOAD;


/**
 * Provides base functionality to all servlets.
 */
public class FileManager {

    public byte[] imageToBytes(String path) throws IOException {
        // open image

        byte[] byteItem;
        BufferedImage originalImage = ImageIO.read(new File(path));

        // convert BufferedImage to byte array
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(originalImage, "jpg", baos);
        baos.flush();
        byteItem = baos.toByteArray();
        baos.close();

        return byteItem;
    }

    protected void storeImage(String filename, byte[] byteValue) {
        try {
            // convert byte array back to BufferedImage
            InputStream in = new ByteArrayInputStream(byteValue);
            BufferedImage bImageFromConvert = ImageIO.read(in);
            ImageIO.write(bImageFromConvert, "jpg", new File("./download/" + filename));
        } catch (IOException e) {
            System.out.println("\nConvert image error");
        }
    }

    public byte[] videoToBytes(String path) throws IOException {

        FileInputStream fis = new FileInputStream(path);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] b = new byte[1024];

        for (int readNum; (readNum = fis.read(b)) != -1; ) {
            bos.write(b, 0, readNum);
        }
        byte[] bytes = bos.toByteArray();
        return bytes;
    }

    protected void storeVideo(String filename, byte[] bytearray) {
        try {
            FileOutputStream fileoutputstream = new FileOutputStream("./download/" + filename);
            fileoutputstream.write(bytearray);
            fileoutputstream.close();

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    protected String getNewFileName(String filename) {
        String newFileName;
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String[] filenamearray = filename.split("\\.");
        newFileName = filenamearray[0] + "_" + HOST + "_" + PORT + "_" + timestamp.getTime() + "." + filenamearray[1];
        return newFileName;
    }


    public void sendData(String hostport, StorageMessages.DataPacket message) {
        try {
            String[] address = hostport.split(":");
            InetAddress ip = InetAddress.getByName(address[0]);
            int port = Integer.parseInt(address[1]);
            Socket connectionSocket = new Socket(ip, port);
//            connectionSocket.setSoTimeout(timeout);
            OutputStream outstream = connectionSocket.getOutputStream();
            message.writeDelimitedTo(outstream);
            connectionSocket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public StorageMessages.DataPacket sendRequest(String hostport, StorageMessages.DataPacket message) {
        try {
            /**
             * Send self info and receive nodes and file detail info
             */
            String[] address = hostport.split(":");
            InetAddress ip = InetAddress.getByName(address[0]);
            int port = Integer.parseInt(address[1]);
            Socket connectionSocket = new Socket(ip, port);
//            connectionSocket.setSoTimeout(1000);
            OutputStream outstream = connectionSocket.getOutputStream();
            message.writeDelimitedTo(outstream);
            /**
             * Read list and begin to communicate with provided nodes
             */
            InputStream instream = connectionSocket.getInputStream();
            StorageMessages.DataPacket nodeListMessage = StorageMessages.DataPacket.getDefaultInstance();
            nodeListMessage = nodeListMessage.parseDelimitedFrom(instream);
            return nodeListMessage;
        } catch (Exception e) {
            System.out.println("Send Request fails");
            e.printStackTrace();
        }
        return null;
    }

    //Runable send post
    public class downLoadParallel implements Runnable {
        private String hostport;
        private String filename;
        private CountDownLatch countdowntimer;
        private int chunkId;
        private FileMap fm;

        public downLoadParallel(String hostport, String filename, int chunkId, CountDownLatch countdowntimer, FileMap fm) {
            this.hostport = hostport;
            this.filename = filename;
            this.countdowntimer = countdowntimer;
            this.chunkId = chunkId;
            this.fm = fm;
        }

        @Override
        public void run() {
            try {
                StorageMessages.DataPacket downloadMessage = StorageMessages.DataPacket.newBuilder().setType(DOWNLOAD).setFileName(filename).setChunkId(chunkId).build();
                StorageMessages.DataPacket fileChunk = sendRequest(hostport, downloadMessage);

                System.out.println("Chunk #" + fileChunk.getChunkId() + " is fine, store it.");
                fm.addFile(filename, chunkId, fileChunk);
                countdowntimer.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}