package org.wzj.fmq.core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * Created by wens on 15-6-12.
 */
public class TestMain {

    public static void main(String[] args) throws IOException {

        File file = new File("/home/wens/data/file1");
        file.deleteOnExit();

        if(!file.exists()){
            file.createNewFile();
        }

        RandomAccessFile randomAccessFile = new RandomAccessFile(file , "r");
        randomAccessFile.seek(6600);
        int read = randomAccessFile.read(new byte[4]);

        System.out.println(read);


    }
}
