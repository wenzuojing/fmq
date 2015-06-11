package org.wzj.fmq.core;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by wens on 15-6-11.
 */
public class WriteMappedFileTest {

    @Test
    public void testWrite_1() throws IOException {
        File file = new File("write_file_1");
        file.deleteOnExit();

        WriteMappedFile writeMappedFile = new WriteMappedFile( "write_file_1", 0, 10 * 1024 * 1024 , 512 * 1024);

        StringBuilder sb = new StringBuilder(200);
        for (int i = 0; i < 1000000; i++) {
            String s = "hello world!";
            sb.append(s);
            writeMappedFile.write(s.getBytes());
        }

        writeMappedFile.close();

        Assert.assertEquals(sb.toString(), FileUtils.readAsString(file).trim());
    }

}
