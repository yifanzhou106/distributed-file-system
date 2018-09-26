package edu.usfca.cs.dfs.StorageNode;


import edu.usfca.cs.dfs.StorageMessages;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static edu.usfca.cs.dfs.StorageMessages.DataPacket.packetType.DATA;
import static edu.usfca.cs.dfs.StorageNode.StorageNode.*;

/**
 * Handle with all data maps in client
 */
public class FileMap {

    private Map<String, TreeMap<Integer, StorageMessages.DataPacket>> localFileMap;
    private Map<String, TreeMap<String, StorageMessages.DataPacket>> replicFileMap;

    private TreeMap<Integer, StorageMessages.DataPacket> filePieces;
    private TreeMap<String, StorageMessages.DataPacket> replicPieces;


    private ReentrantReadWriteLock filemaplock;
    private ReentrantReadWriteLock replicFilemaplock;


    public FileMap() {
        replicFilemaplock = new ReentrantReadWriteLock();
        filemaplock = new ReentrantReadWriteLock();
        localFileMap = new HashMap<>();
        replicFileMap = new HashMap<>();
    }

    /**
     * Add a single piece in to filemap
     *
     * @param filename
     * @param pieceid
     * @param chunkPiece
     */
    public void addFile(String filename, int pieceid, StorageMessages.DataPacket chunkPiece) {
        filemaplock.writeLock().lock();
        try {
            if (!localFileMap.containsKey(filename)) {
                filePieces = new TreeMap<>();
                filePieces.put(pieceid, chunkPiece);
            } else {
                filePieces = localFileMap.get(filename);
                filePieces.put(pieceid, chunkPiece);
                localFileMap.remove(filename);
            }
            localFileMap.put(filename, filePieces);

        } finally {
            filemaplock.writeLock().unlock();
        }
    }

    public void addReplic(String hostport, String filename, StorageMessages.DataPacket chunkPiece) {
        replicFilemaplock.writeLock().lock();
        try {
            if (!replicFileMap.containsKey(hostport)) {
                replicPieces = new TreeMap<>();
                replicPieces.put(filename, chunkPiece);
            } else {
                replicPieces = replicFileMap.get(hostport);
                replicPieces.put(filename, chunkPiece);
                replicFileMap.remove(filename);
            }
            replicFileMap.put(hostport, replicPieces);

        } finally {
            replicFilemaplock.writeLock().unlock();
        }
    }

    /**
     * Get single file pieces
     *
     * @param filename
     * @param pieceid
     * @return
     */
    public StorageMessages.DataPacket getPiece(String filename, int pieceid) {
        filemaplock.readLock().lock();
        try {
            StorageMessages.DataPacket piece;
            filePieces = localFileMap.get(filename);
            piece = filePieces.get(pieceid);
            return piece;

        } finally {
            filemaplock.readLock().unlock();
        }
    }

    /**
     * Combine all pieces together
     *
     * @param filename
     * @param piecenum
     * @return
     */
    public byte[] getFile(String filename, int piecenum) {
        filemaplock.readLock().lock();
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            filePieces = localFileMap.get(filename);
            for (int i = 0; i < piecenum; i++)
                output.write(filePieces.get(i).getData().toByteArray());

            return output.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            return null;

        } finally {
            filemaplock.readLock().unlock();
        }
    }

    /**
     * Get file total piece number
     *
     * @param filename
     * @return
     */
    public int getPieceNum(String filename){
        filemaplock.readLock().lock();
        try{
            filePieces = localFileMap.get(filename);
            return filePieces.size();
        }finally {
            filemaplock.readLock().unlock();

        }
    }

    public Boolean isFileExist (String filename)
    {
        filemaplock.readLock().lock();
        try{
            return localFileMap.containsKey(filename);
        }finally {
            filemaplock.readLock().unlock();

        }
    }

    public StorageMessages.DataPacket rebuildReplicChunk (StorageMessages.DataPacket receiveMessage)
    {
        int chunkId = receiveMessage.getChunkId();
        String filename = receiveMessage.getFileName();
        StorageMessages.DataPacket replicChunk = StorageMessages.DataPacket.newBuilder().setType(DATA).setIsReplic(true).setChunkId(chunkId).setFileName(filename).setData(receiveMessage.getData()).setHost(HOST).setPort(PORT).build();
        return replicChunk;
    }


}
