package org.wzj.fmq.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by wens on 15-6-12.
 */
public class FileStoreTest {

    @Before
    public void setup(){
        FileUtils.deleteDir("data/fmq") ;
    }


    @Test
    public void test_write_0() throws IOException {

        File file = new File("data/fmq/t0");

        FileStore fileStore = new FileStore(file.getPath());
        fileStore.init();

        Message message = new Message();
        message.setData("test".getBytes());

        fileStore.write(message);
        Assert.assertEquals( 0, message.getId());

        fileStore.close() ;

    }


    @Test
    public void test_write_1() throws IOException {

        File file = new File("data/fmq/t1");

        FileStore fileStore = new FileStore(file.getPath());
        fileStore.init();

        Message message = new Message();
        message.setData("testttttttttttttttttttttttttttttttttttttttttttttttttt".getBytes());

        for(int i = 0  ; i < 100000 ; i++ ){
            fileStore.write(message);
            Assert.assertEquals( i , message.getId());
        }

        fileStore.close();
    }

    @Test
    public void test_write_2() throws IOException {

        File file = new File("data/fmq/t2");

        FileStore fileStore = new FileStore(file.getPath());
        fileStore.init();

        Message message = new Message();

        byte[] buf = new byte[ 100 * 1024  ] ;

        message.setData(buf);

        for(int i = 0  ; i < 100000 ; i++ ){
            fileStore.write(message);
            Assert.assertEquals( i , message.getId());
        }

        fileStore.close();
    }

    @Test
    public void test_read_1() throws IOException {

        File file = new File("data/fmq/t3");

        FileStore fileStore = new FileStore(file.getPath());
        fileStore.init();

        for(int i = 0  ; i < 100000 ; i++ ){
            Message message = new Message();
            message.setData( ( "test-"+i).getBytes() );
            fileStore.write(message);
            Assert.assertEquals( i , message.getId());
        }

        List<Message> messageList = fileStore.query(10, 5);

        Assert.assertEquals(5 , messageList.size() );

        fileStore.close();
    }

}
