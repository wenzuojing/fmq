package org.wzj.fmq.core;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by wens on 15-6-11.
 */
public class ReadMappedFileTest {


    @Test
    public void testRead_1() throws IOException {
        File file = new File("");
        file.deleteOnExit();

        WriteMappedFile writeMappedFile = new WriteMappedFile( "read_file_1", 0, 10 * 1024 * 1024 , 512 * 1024);

        StringBuilder sb = new StringBuilder(200);
        for (int i = 0; i < 1000; i++) {
            String s = "hello world!";
            sb.append(s);
            writeMappedFile.write(s.getBytes());
        }

        writeMappedFile.close();

        Assert.assertEquals(sb.toString(), FileUtils.readAsString(file).trim());

        ReadMappedFile readMappedFile = new ReadMappedFile("read_file_1" , 0, 40);

        StringBuilder sb2 = new StringBuilder(200);
        byte[] bytes = new byte[42];

        readMappedFile.read(bytes);
        sb2.append(new String(bytes));
        readMappedFile.read(bytes);
        sb2.append(new String(bytes));

        readMappedFile.close();

        Assert.assertEquals(sb2.toString(), sb.substring(0, sb2.length()));

    }


}
