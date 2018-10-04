package edu.usfca.cs.dfs.Client;

import edu.usfca.cs.dfs.StorageMessages;

import java.util.concurrent.ExecutorService;

import static edu.usfca.cs.dfs.Client.Client.*;
import static edu.usfca.cs.dfs.StorageMessages.DataPacket.packetType.DELETE_BY_FILENAME;

public class DeleteFile extends FileManager implements Runnable {
    private ExecutorService threads;
    private String filename;

    public DeleteFile(ExecutorService threads, String filename) {
        this.threads = threads;
        this.filename = filename;
    }

    @Override
    public void run() {
        try {
            String address = NODE_HOST + ":" + NODE_PORT;

            StorageMessages.DataPacket checkRequest = StorageMessages.DataPacket.newBuilder().setType(DELETE_BY_FILENAME).setHostport(address).setFileName(filename).build();

            sendData(address, checkRequest);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
