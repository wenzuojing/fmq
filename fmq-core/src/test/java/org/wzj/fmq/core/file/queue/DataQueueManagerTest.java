package org.wzj.fmq.core.file.queue;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wzj.fmq.core.store.StoreMessage;
import org.wzj.fmq.core.store.file.AppendMessageResult;
import org.wzj.fmq.core.store.file.AppendMessageStatus;
import org.wzj.fmq.core.store.file.queue.DataQueue;
import org.wzj.fmq.core.store.file.queue.DataQueueManager;
import org.wzj.fmq.core.util.Utils;

import java.io.File;

/**
 * Created by wens on 15-6-20.
 */
public class DataQueueManagerTest {

    private DataQueueManager dataQueueManager ;

    @Before
    public void before(){
        this.dataQueueManager = new DataQueueManager("data/data" , 1 * 1024 * 1024 , 100 ) ;

        this.dataQueueManager.init();
        this.dataQueueManager.start();
    }

    @After
    public void after(){

        this.dataQueueManager.commit();
        this.dataQueueManager.shutdown();
        Utils.deleteDir("data/data");

    }

    @Test
    public void test_create_new_queue(){
        DataQueue newQueue = this.dataQueueManager.createNewQueue();
        Assert.assertNotNull(newQueue);

        DataQueue newQueue1 = this.dataQueueManager.createNewQueue();

        Assert.assertEquals(newQueue, newQueue1);
    }


    public void prepareQueue(){
        DataQueue newQueue = this.dataQueueManager.createNewQueue();
        Assert.assertNotNull(newQueue);
    }

    @Test
    public void test_get_last_queue(){
        Assert.assertNull(this.dataQueueManager.getLastQueue());
        prepareQueue();
        Assert.assertNotNull(this.dataQueueManager.getLastQueue());
    }


    @Test
    public void test_get_first_queue(){
        Assert.assertNull(this.dataQueueManager.getFirstQueue());
        prepareQueue();
        Assert.assertNotNull(this.dataQueueManager.getFirstQueue());
    }


    @Test
    public void test_append_message(){


        int files = 1 ;

        for(int i = 0 ;  ; i++ ){
            StoreMessage storeMessage = new StoreMessage();
            storeMessage.setBody(("append_" + i).getBytes());
            storeMessage.setCheckSum(i);
            storeMessage.setCreateTimestamp(i * 100);
            storeMessage.setTopic("topic_" + i);
            storeMessage.setStoreSize(storeMessage.calNeedByteSize());
            AppendMessageResult appendMessageResult = dataQueueManager.appendMessage(storeMessage);

            System.out.println(appendMessageResult);

            if(appendMessageResult.getStatus() == AppendMessageStatus.END_OF_FILE ){
                files++ ;
                if(files ==  4 ){
                    break;
                }
            }
        }

        Assert.assertEquals(files -1  , new File("data/data").list().length ) ;

    }


}
