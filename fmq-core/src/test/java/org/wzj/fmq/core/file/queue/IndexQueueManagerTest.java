package org.wzj.fmq.core.file.queue;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wzj.fmq.core.store.file.StoreMessagePosition;
import org.wzj.fmq.core.store.file.queue.IndexQueueManager;
import org.wzj.fmq.core.util.Utils;

/**
 * Created by wens on 15-6-21.
 */
public class IndexQueueManagerTest  {


    private IndexQueueManager indexQueueManager ;

    @Before
    public void before(){
        indexQueueManager = new IndexQueueManager("my-topic" , "data/index" , 1000 * 20 ) ;
        indexQueueManager.init();
        indexQueueManager.start();
    }

    @After
    public void after(){
        indexQueueManager.commit();
        indexQueueManager.shutdown();
        Utils.deleteDir("data/index");
    }

    @Test
    public void test_build_message_index(){

        for(int i = 1  ; i < 2001 ; i++ ){
            indexQueueManager.buildMessageIndex(99 * i, 999, 9999 * i);
        }

        Assert.assertEquals(0, indexQueueManager.getMinIndex());
        Assert.assertEquals(2000, indexQueueManager.getMaxIndex());
        Assert.assertEquals(2000, indexQueueManager.getTotalMessageNum());
    }



    @Test
    public void test_index_message_index(){

        for(int i = 1  ; i < 2001 ; i++ ){
            indexQueueManager.buildMessageIndex( 99 * i, 999, 9999 * i);
        }

        StoreMessagePosition s_0 = indexQueueManager.indexStoreMessagePosition(0);

        Assert.assertNotNull( s_0);

        StoreMessagePosition s_1000 = indexQueueManager.indexStoreMessagePosition(1000);

        Assert.assertNotNull(s_1000);

        StoreMessagePosition s_2000 = indexQueueManager.indexStoreMessagePosition(2000);

        Assert.assertNotNull(s_2000);

        StoreMessagePosition s_2001 = indexQueueManager.indexStoreMessagePosition(2001);

        Assert.assertNull(s_2001);




    }
}
