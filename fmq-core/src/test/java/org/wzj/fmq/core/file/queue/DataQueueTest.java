package org.wzj.fmq.core.file.queue;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wzj.fmq.core.store.StoreMessage;
import org.wzj.fmq.core.store.file.AppendMessageResult;
import org.wzj.fmq.core.store.file.AppendMessageStatus;
import org.wzj.fmq.core.store.file.SelectMappedBufferResult;
import org.wzj.fmq.core.store.file.queue.DataQueue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wens on 15-6-20.
 */
public class DataQueueTest {

    private DataQueue dataQueue ;

    @Before
    public void before(){
        dataQueue = new DataQueue("data/data/00000000000000" , 1 * 1024 * 1024 , 100 * 1024 ) ;
    }

    @After
    public void after(){
        //dataQueue.delete();
        dataQueue.commit();
    }

    @Test
    public void test_append_message(){

        int wroteSize  = 0 ;

        for(int i = 0 ;  ; i++ ){
            StoreMessage storeMessage = new StoreMessage();
            storeMessage.setBody(("append_" + i).getBytes());
            storeMessage.setCheckSum(i);
            storeMessage.setCreateTimestamp(i * 100);
            storeMessage.setTopic("topic_" + i);
            storeMessage.setDataOffset(wroteSize);
            storeMessage.setStoreSize(storeMessage.calNeedByteSize());
            AppendMessageResult appendMessageResult = dataQueue.appendMessage(storeMessage);
            System.out.println(appendMessageResult);
            wroteSize += appendMessageResult.getWroteBytes() ;

            if(appendMessageResult.getStatus() == AppendMessageStatus.END_OF_FILE ){
                break;
            }
        }
        Assert.assertEquals(wroteSize, dataQueue.getWritePosition());
    }


    private List< AppendMessageResult> prepareGetMessage(int mount){

        int wroteSize  = 0 ;

        List<AppendMessageResult> ret = new ArrayList<>(mount) ;
        for(int i = 0 ; i < mount ; i++ ){
            StoreMessage storeMessage = new StoreMessage();
            storeMessage.setBody(("append_" + i).getBytes());
            storeMessage.setCheckSum(i);
            storeMessage.setCreateTimestamp(i * 100);
            storeMessage.setTopic("topic_" + i);
            storeMessage.setDataOffset(wroteSize);
            storeMessage.setStoreSize(storeMessage.calNeedByteSize());
            AppendMessageResult appendMessageResult = dataQueue.appendMessage(storeMessage);
            wroteSize += appendMessageResult.getWroteBytes() ;
            ret.add(appendMessageResult);
        }

        return ret ;
    }

    @Test
    public void test_get_message(){
        List<AppendMessageResult> appendMessageResults = prepareGetMessage(1);

        int i = 0 ;
        for(AppendMessageResult ar : appendMessageResults ){

            System.out.println("read:" +ar);

            SelectMappedBufferResult selectMappedBufferResult = dataQueue.selectMappedBuffer((int)ar.getWroteOffset() , ar.getWroteBytes() );

            ByteBuffer byteBuffer = selectMappedBufferResult.getByteBuffer();

            StoreMessage storeMessage = new StoreMessage();

            storeMessage.decode(byteBuffer);

            Assert.assertEquals(ar.getWroteBytes(), storeMessage.getStoreSize()) ;
            Assert.assertEquals("append_" + i, new String(storeMessage.getBody()));
            Assert.assertEquals("topic_" + i, storeMessage.getTopic());
            Assert.assertEquals(ar.getWroteOffset() , storeMessage.getDataOffset());
            Assert.assertEquals(i, storeMessage.getCheckSum());
            Assert.assertEquals(i * 100 , storeMessage.getCreateTimestamp() );

            i++ ;

        }



    }
}
