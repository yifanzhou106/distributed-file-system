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

    public StorageMessages.DataPacket buildLocalDataPacket (String leaderHashVal){
        filemaplock.readLock().lock();
        try{
            StorageMessages.DataPacket.Builder rebalanceFilePacket = StorageMessages.DataPacket.newBuilder();

            for (Map.Entry<String, TreeMap<Integer, StorageMessages.DataPacket>> entry: localFileMap.entrySet()){
                filePieces =entry.getValue();

                if (leaderHashVal.compareTo(entry.getKey()) >0 )
                {
                    for (Map.Entry<Integer, StorageMessages.DataPacket> fileChunk : filePieces.entrySet())
                    {
                        rebalanceFilePacket.addRebalanceLocData(fileChunk.getValue());
                    }
                } else{
                    for (Map.Entry<Integer, StorageMessages.DataPacket> fileChunk : filePieces.entrySet())
                    {
                        rebalanceFilePacket.addRebalanceReplicData(fileChunk.getValue());
                    }
                }
            }

            return rebalanceFilePacket.setHost(HOST).setPort(PORT).build();
        }finally {
            filemaplock.readLock().unlock();

        }
    }

    public void storeRebalanceFile (StorageMessages.DataPacket filelist){
        filemaplock.writeLock().lock();
        replicFilemaplock.writeLock().lock();
        try{
            StorageMessages.DataPacket fileChunk;
                if (filelist.getRebalanceLocDataList().size()!=0)
                {
                    for (int i = 0; i< filelist.getRebalanceLocDataList().size();i++)
                    {
                        fileChunk = filelist.getRebalanceLocDataList().get(i);
                        addFile(fileChunk.getFileName(),fileChunk.getChunkId(),fileChunk);
                    }
                }
                if (filelist.getRebalanceReplicDataList().size()!=0)
                {
                    String hostport = filelist.getHost()+":"+filelist.getPort();
                    for (int i = 0; i< filelist.getRebalanceLocDataList().size();i++)
                    {
                        fileChunk = filelist.getRebalanceLocDataList().get(i);
                        addReplic(hostport,fileChunk.getFileName(),fileChunk);
                    }
                }
            System.out.println(localFileMap);
            System.out.println(replicFileMap);
        }finally {
            filemaplock.writeLock().unlock();
            replicFilemaplock.writeLock().unlock();
        }
    }
    public void removeReplicationByNode(String hostport)
    {
        replicFilemaplock.writeLock().lock();
        try{
                if (replicFileMap.containsKey(hostport))
                {
                    replicFileMap.remove(replicFileMap);
                }else {
                    System.out.println("I do not have this file");
                }
        }finally {
            replicFilemaplock.writeLock().unlock();
        }
    }

}
