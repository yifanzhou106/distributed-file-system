package edu.usfca.cs.dfs.StorageNode;

import edu.usfca.cs.dfs.Client.FileManager;
import edu.usfca.cs.dfs.StorageMessages;

import java.util.concurrent.ExecutorService;

import static edu.usfca.cs.dfs.Client.Client.NODE_HOST;
import static edu.usfca.cs.dfs.Client.Client.NODE_PORT;
import static edu.usfca.cs.dfs.StorageMessages.DataPacket.packetType.DELETE_BY_FILENAME;
import static edu.usfca.cs.dfs.StorageNode.StorageNode.HOSTPORT;

public class DeleteFile extends FileManager implements Runnable {
    private ExecutorService threads;
    private String filename;
    private FileMap fm;

    public DeleteFile(ExecutorService threads, FileMap fm, String filename) {
        this.threads = threads;
        this.fm = fm;
        this.filename = filename;
    }

    @Override
    public void run() {
        try {
            System.out.println("**************************DELETE file "+ filename+ " on node "+ HOSTPORT);
           fm.deleteByFilenameLocally(filename);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
