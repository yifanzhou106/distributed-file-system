package edu.usfca.cs.dfs.StorageNode;


import edu.usfca.cs.dfs.StorageMessages;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static edu.usfca.cs.dfs.StorageMessages.DataPacket.packetType.DATA;
import static edu.usfca.cs.dfs.StorageMessages.DataPacket.packetType.UPDATE_REPLICATION;
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
    public int getPieceNum(String filename) {
        filemaplock.readLock().lock();
        try {
            filePieces = localFileMap.get(filename);
            return filePieces.size();
        } finally {
            filemaplock.readLock().unlock();

        }
    }

    public Boolean isFileExist(String filename) {
        filemaplock.readLock().lock();
        try {
            return localFileMap.containsKey(filename);
        } finally {
            filemaplock.readLock().unlock();

        }
    }

    public StorageMessages.DataPacket rebuildReplicChunk(StorageMessages.DataPacket receiveMessage) {
        int chunkId = receiveMessage.getChunkId();
        String filename = receiveMessage.getFileName();
        StorageMessages.DataPacket replicChunk = StorageMessages.DataPacket.newBuilder().setType(DATA).setIsReplic(true).setChunkId(chunkId).setFileName(filename).setData(receiveMessage.getData()).setHost(HOST).setPort(PORT).build();
        return replicChunk;
    }

    public StorageMessages.DataPacket buildLocalDataPacket(String leaderHashVal) {
        filemaplock.readLock().lock();
        try {
            StorageMessages.DataPacket.Builder rebalanceFilePacket = StorageMessages.DataPacket.newBuilder();

            for (Map.Entry<String, TreeMap<Integer, StorageMessages.DataPacket>> entry : localFileMap.entrySet()) {
                filePieces = entry.getValue();

                if (leaderHashVal.compareTo(fileToSha1(entry.getKey())) > 0) {
                    for (Map.Entry<Integer, StorageMessages.DataPacket> fileChunk : filePieces.entrySet()) {
                        rebalanceFilePacket.addRebalanceLocData(fileChunk.getValue());
                    }
                } else {
                    for (Map.Entry<Integer, StorageMessages.DataPacket> fileChunk : filePieces.entrySet()) {
                        rebalanceFilePacket.addRebalanceReplicData(fileChunk.getValue());
                    }
                }
            }

            return rebalanceFilePacket.setHost(HOST).setPort(PORT).build();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } finally {
            filemaplock.readLock().unlock();

        }
        return null;
    }

    public StorageMessages.DataPacket buildLocalReplicationPacket(String brokenNode) {
        filemaplock.readLock().lock();
        replicFilemaplock.readLock().lock();
        try {
            StorageMessages.DataPacket.Builder rebalanceFilePacket = StorageMessages.DataPacket.newBuilder();

            for (Map.Entry<String, TreeMap<Integer, StorageMessages.DataPacket>> entry : localFileMap.entrySet()) {
                filePieces = entry.getValue();
                /**
                 * node local data
                 */
                for (Map.Entry<Integer, StorageMessages.DataPacket> fileChunk : filePieces.entrySet()) {
                    rebalanceFilePacket.addRebalanceReplicData(fileChunk.getValue());
                }

            }
            return rebalanceFilePacket.setHostport(HOSTPORT).setType(UPDATE_REPLICATION).setDeleteNodeFile(brokenNode).build();
        } finally {
            filemaplock.readLock().unlock();
            replicFilemaplock.readLock().unlock();
        }
    }

    public StorageMessages.DataPacket buildNextReplicationPacket(String nextnode, String brokenNode) {
        filemaplock.readLock().lock();
        replicFilemaplock.readLock().lock();
        try {
            StorageMessages.DataPacket.Builder rebalanceFilePacket = StorageMessages.DataPacket.newBuilder();
            System.out.println("In buildNextReplicationPacket");

            /**
             * next node local data
             */
            replicPieces = replicFileMap.get(nextnode);
            System.out.println("replicPieces " + replicPieces);
            if (replicPieces != null)
                for (Map.Entry<String, StorageMessages.DataPacket> fileChunk : replicPieces.entrySet()) {
                    rebalanceFilePacket.addRebalanceReplicData(fileChunk.getValue());
                }

            return rebalanceFilePacket.setHostport(nextnode).setType(UPDATE_REPLICATION).setDeleteNodeFile(brokenNode).build();
        } finally {
            filemaplock.readLock().unlock();
            replicFilemaplock.readLock().unlock();
        }
    }


    public void storeRebalanceFile(StorageMessages.DataPacket filelist) {
        filemaplock.writeLock().lock();
        replicFilemaplock.writeLock().lock();
        try {
            StorageMessages.DataPacket fileChunk;
            if (filelist.getRebalanceLocDataList().size() != 0) {
                for (int i = 0; i < filelist.getRebalanceLocDataList().size(); i++) {
                    fileChunk = filelist.getRebalanceLocDataList().get(i);
                    addFile(fileChunk.getFileName(), fileChunk.getChunkId(), fileChunk);
                }
            }
            if (filelist.getRebalanceReplicDataList().size() != 0) {
                String hostport = filelist.getHostport();
                if (replicFileMap.containsKey(hostport))
                    replicFileMap.remove(hostport);
                for (int i = 0; i < filelist.getRebalanceReplicDataList().size(); i++) {
                    fileChunk = filelist.getRebalanceReplicDataList().get(i);
                    addReplic(hostport, fileChunk.getFileName(), fileChunk);
                }
            }
            if (filelist.getDeleteNodeFile() != null)
                removeReplicationByNode(filelist.getDeleteNodeFile());


        } finally {
            filemaplock.writeLock().unlock();
            replicFilemaplock.writeLock().unlock();
        }
    }

    public void removeReplicationByNode(String hostport) {
        replicFilemaplock.writeLock().lock();
        try {
            if (replicFileMap.containsKey(hostport)) {
                replicFileMap.remove(hostport);

                System.out.println("****************************Replication Map Now");
                System.out.println(replicFileMap);
                System.out.println("****************************End Replication Map");
            } else {
                System.out.println("I do not have this file replication");
            }
        } finally {
            replicFilemaplock.writeLock().unlock();
        }
    }

    public void moveReplicationToLocal(String hostport) {
        replicFilemaplock.writeLock().lock();
        try {

            if (replicFileMap.containsKey(hostport)) {
                replicPieces = replicFileMap.get(hostport);
                for (Map.Entry<String, StorageMessages.DataPacket> entry : replicPieces.entrySet()) {
                    StorageMessages.DataPacket fileChunk = entry.getValue();
                    addFile(entry.getKey(), fileChunk.getChunkId(), fileChunk);
                }
                removeReplicationByNode(hostport);
            } else {
                System.out.println("I do not have this node file");
            }
        } finally {
            replicFilemaplock.writeLock().unlock();
        }

    }


    public String fileToSha1(String input) throws NoSuchAlgorithmException {
        MessageDigest mDigest = MessageDigest.getInstance("SHA1");
        byte[] result = mDigest.digest(input.getBytes());
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < result.length; i++) {
            sb.append(Integer.toString((result[i] & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }

}
