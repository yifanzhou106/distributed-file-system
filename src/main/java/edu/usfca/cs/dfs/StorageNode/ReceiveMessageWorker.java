package edu.usfca.cs.dfs.StorageNode;

import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static edu.usfca.cs.dfs.StorageMessages.DataPacket.packetType.*;
import static edu.usfca.cs.dfs.StorageNode.StorageNode.*;

public class ReceiveMessageWorker extends Connection implements Runnable {
    private ExecutorService threads;
    private NodeMap nm;
    private FileMap fm;

    public ReceiveMessageWorker(ExecutorService threads,FileMap fm ,NodeMap nm) {
        this.threads = threads;
        this.nm = nm;
        this.fm = fm;
    }
    @Override
    public void run() {
        try {
            ServerSocket welcomingSocket = new ServerSocket(PORT);
            while (!isShutdown) {
                Socket connectionSocket = welcomingSocket.accept();
                InputStream instream = connectionSocket.getInputStream();
                OutputStream outstream = connectionSocket.getOutputStream();

                StorageMessages.DataPacket requestMessage = StorageMessages.DataPacket.getDefaultInstance();
                requestMessage = requestMessage.parseDelimitedFrom(instream);
                if (requestMessage.getType() == StorageMessages.DataPacket.packetType.REQUEST)
                {
                    NumRequest++;
                    int numChunks;
                    /**
                     * Check request node info in list
                     */
                    InetAddress host = connectionSocket.getInetAddress();
                    int port = connectionSocket.getPort();
                    String hostport = host+":"+port;

                    /**
                     * Get data from protocol buffer
                     */
                    String filename = requestMessage.getFileName();
                    if (!fm.isFileExist(filename))
                        numChunks = requestMessage.getNumChunk();
                    else numChunks = fm.getPieceNum(filename);


                    System.out.println("Filename is " + filename);
                    System.out.println("\nnumChunks is " + numChunks);

                    /**
                     * Reply a hashed node address
                     */
                    String hashedName = nameToSha1(filename);
                    System.out.println("\nhashedName is " + hashedName);
                    System.out.println( nm.pickNodeList(hashedName,numChunks));
                    nm.pickNodeList(hashedName,numChunks).writeDelimitedTo(outstream);
                    connectionSocket.close();
                } else if (requestMessage.getType() == StorageMessages.DataPacket.packetType.DOWNLOAD)
                {
                    /**
                     * Send file chunks
                     */
//                    NumRequest++;
                    String filename = requestMessage.getFileName();
                    int chunkID = requestMessage.getChunkId();
                    StorageMessages.DataPacket dataPacket = fm.getPiece(filename, chunkID);
                    outstream = connectionSocket.getOutputStream();
                    dataPacket.writeDelimitedTo(outstream);
                    connectionSocket.close();

                }
                else if (requestMessage.getType() == StorageMessages.DataPacket.packetType.NODELIST)
                {
                    /**
                     * Update nodelist
                     */
                    List nodelist =  requestMessage.getNodeListList();
                    System.out.println(nodelist);
                    nm.updateNodeMap(nodelist);

                } else if (requestMessage.getType() == StorageMessages.DataPacket.packetType.DATA)
                {
                    /**
                     * Update nodelist
                     */
                    String filename = requestMessage.getFileName();
                    int chunkId = requestMessage.getChunkId();
                    fm.addFile(filename,chunkId,requestMessage);
                    System.out.println("Received file: "+filename+" ChunkId: "+chunkId);
                }


            }
            if (isShutdown) {
                welcomingSocket.close();
            }
        } catch (IOException e) {
            System.out.println(e);

        } catch (NoSuchAlgorithmException e)
        {
            System.out.println(e);

        }

    }
}
