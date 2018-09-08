package edu.usfca.cs.dfs.Client;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.StorageMessages;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import static edu.usfca.cs.dfs.Client.Client.*;

public class Uploader extends FileManager implements Runnable {
    private ExecutorService threads;
    private String filelocation;
    private int FIXED_PIECE_SIZE = 256 * 1024;
    private FileMap fm;


    public Uploader(ExecutorService threads, FileMap fm, String filelocation) {
        this.threads = threads;
        this.filelocation = filelocation;
        this.fm = fm;
    }

    @Override
    public void run() {
        try {
            byte[] byteItem;
            String filename;
            if (!isDebug) {
                FIXED_PIECE_SIZE = 256;
                String item = "As a globally-distributed database, Spanner provides several interesting features. First, the replication configurations for data can be dynamically controlled at a fine grain by applications Second, Spanner has two features that are difficult to implement in a distributed database: it provides externally consistent reads and writes, and globally-consistent reads across the database at a timestamp. These features enable Spanner to support consistent backups, consistent MapReduce executions, and atomic schema updates, all at global scale, and even in the presence of ongoing transactions.";
                filename = "file1";
                byteItem = item.getBytes(Charset.forName("UTF-8"));
            } else {
                filename = filelocation;
                String[] suffix = filename.split("\\.");
                if (suffix[1].equals("jpg"))
                    byteItem = imageToBytes(filelocation);
                else {
                    byteItem = videoToBytes(filelocation);
                }
            }

            int blockcount = (byteItem.length) / FIXED_PIECE_SIZE;
            byte[] piece;
//            System.out.println(byteItem.length);
            for (int i = 0; i < blockcount + 1; i++) {
                if (i == blockcount) {
                    piece = Arrays.copyOfRange(byteItem, i * FIXED_PIECE_SIZE, byteItem.length);
                } else {
                    piece = Arrays.copyOfRange(byteItem, i * FIXED_PIECE_SIZE, (i + 1) * FIXED_PIECE_SIZE);
                }
                StorageMessages.StoreChunk chunkPiece = StorageMessages.StoreChunk.newBuilder().setChunkId(i).setFileName(filename).setData(ByteString.copyFrom(piece)).build();
                fm.addFile(filename, i, chunkPiece);
            }

//            byte[] byteValue = fm.getFile(filename, blockcount + 1);
//            storeVideo(filename , byteValue);


            System.out.println("Upload successfully");
        } catch (Exception e) {
            System.out.println("Upload Error, cannot find file");
        }

    }

}