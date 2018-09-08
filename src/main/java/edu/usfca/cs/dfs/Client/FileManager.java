package edu.usfca.cs.dfs.Client;


import javax.imageio.ImageIO;

import java.awt.image.BufferedImage;
import java.io.*;
import java.sql.Timestamp;
import static edu.usfca.cs.dfs.Client.Client.*;


/**
 * Provides base functionality to all servlets.
 */
public class FileManager  {

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


}