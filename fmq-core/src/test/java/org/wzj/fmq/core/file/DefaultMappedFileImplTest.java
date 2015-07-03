package org.wzj.fmq.core.file;

import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.wzj.fmq.core.store.StoreMessage;
import org.wzj.fmq.core.store.file.DefaultMappedFileImpl;
import org.wzj.fmq.core.store.file.MappedFile;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by wens on 15-6-20.
 */
public class DefaultMappedFileImplTest {

    private MappedFile mappedFile ;


    @Before
    public void before() throws IOException {
        mappedFile = new DefaultMappedFileImpl("data/mappedFile" , 1 * 1024 * 1024 ) ;
    }


    @After
    public void after(){
        mappedFile.delete();
    }


    @Test
    public void test_1(){
        StoreMessage storeMessage = new StoreMessage();
        storeMessage.setBody(("append_1").getBytes());
        storeMessage.setCheckSum(1);
        storeMessage.setCreateTimestamp(100);
        storeMessage.setTopic("topic_1");
        storeMessage.setDataOffset(0);
        storeMessage.setStoreSize(storeMessage.calNeedByteSize());

        ByteBuffer buffer = ByteBuffer.allocate(storeMessage.getStoreSize());

        storeMessage.encode(buffer);

        mappedFile.getByteBuffer(0).put(buffer.array(), 0, storeMessage.getStoreSize()) ;


        ByteBuffer buffer1 = mappedFile.getByteBuffer(0, storeMessage.getStoreSize());



        StoreMessage storeMessage1 = new StoreMessage();


        storeMessage1.decode(buffer1);

        Assert.assertEquals("append_1", new String(storeMessage1.getBody()));

        Assert.assertEquals("topic_1" ,storeMessage1.getTopic() );


    }
}
