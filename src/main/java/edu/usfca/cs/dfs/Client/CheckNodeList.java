package edu.usfca.cs.dfs.Client;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.CheckSum;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import static edu.usfca.cs.dfs.Client.Client.*;
import static edu.usfca.cs.dfs.StorageMessages.DataPacket.packetType.*;

public class CheckNodeList extends FileManager implements Runnable {
    private ExecutorService threads;


    public CheckNodeList(ExecutorService threads) {
        this.threads = threads;
    }

    @Override
    public void run() {
        try {

            StorageMessages.DataPacket checkRequest = StorageMessages.DataPacket.newBuilder().setType(CHECK_NODE_INFO).build();
            String coorAddress = COOR_HOST + ":" + COOR_PORT;

            StorageMessages.DataPacket nodeListMessage = sendRequest(coorAddress, checkRequest);

            System.out.println(nodeListMessage.getNodeListList());


        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
