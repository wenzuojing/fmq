package org.wzj.fmq.core.file.queue;

import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.wzj.fmq.core.store.file.StoreMessagePosition;
import org.wzj.fmq.core.store.file.queue.IndexQueue;

/**
 * Created by wens on 15-6-20.
 */
public class IndexQueueTest  {

    private IndexQueue indexQueue ;

    @Before
    public void before(){
        indexQueue = new IndexQueue("data/index/0000000", 2001 * IndexQueue.INDEX_UNIT_SIZE  ) ;
        indexQueue.init();
    }

    @After
    public void after() {

        indexQueue.delete();
    }


    @Test
    public void test_append_message_index(){


        for(int i = 1  ; i < 2001 ; i++ ){
            indexQueue.appendMessageIndex(99 * i , 999 , 9999 * i );
        }

        Assert.assertEquals( 9999 , indexQueue.getMinDataCreateTimestamp() );

        Assert.assertEquals( 9999 * 2000 , indexQueue.getMaxDataCreateTimestamp() );

        Assert.assertTrue(indexQueue.isFull());


    }

    @Test
    public void test_index(){


        for(int i = 1  ; i < 2001 ; i++ ){
            indexQueue.appendMessageIndex(99 * i , 999 , 9999 * i );
        }

        for(int i = 0 ;i < 2000 ; i++ ){
            StoreMessagePosition storeMessagePosition = indexQueue.indexFor(i);
            Assert.assertEquals( (i+1) * 99 ,  storeMessagePosition.getDataQueueOffset());
            Assert.assertEquals( (i+1) * 9999, storeMessagePosition.getCreateTimestamp());
            Assert.assertEquals(  999, storeMessagePosition.getMsgSize() );
        }
    }

    @Test
    public void test_get_index_by_time(){


        for(int i = 1  ; i < 2001 ; i++ ){
            indexQueue.appendMessageIndex(99 * i , 999 , 9999 * i );
        }

        for(int i = 1 ;i <= 2000 ; i++ ){
            long index = indexQueue.getIndexByTime(9999 * i);


            Assert.assertEquals(i - 1, index);
        }

        for(int i = 1 ;i <= 1999 ; i++ ){
            long index = indexQueue.getIndexByTime(9999 * i  + 2 );

            Assert.assertEquals(i, index);
        }

        for(int i = 2 ;i <= 1999 ; i++ ){
            long index = indexQueue.getIndexByTime(9999 * i  -  2 );

            Assert.assertEquals(i - 1 , index);
        }
    }
}
