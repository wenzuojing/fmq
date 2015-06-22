package org.wzj.fmq.core.file.service;

import org.junit.Assert;
import org.junit.Test;
import org.wzj.fmq.core.store.PutMessageResult;
import org.wzj.fmq.core.store.PutMessageStatus;
import org.wzj.fmq.core.store.StoreMessage;
import org.wzj.fmq.core.store.file.AppendMessageResult;
import org.wzj.fmq.core.store.file.AppendMessageStatus;
import org.wzj.fmq.core.store.file.service.MessageStoreService;

import java.io.File;

/**
 * Created by wens on 15-6-21.
 */
public class MessageStoreServiceTest extends  BaseServiceTest{

    @Test
    public void test_put_message_1(){
        MessageStoreService messageStoreService = serviceManager.getMessageStoreService();

        for(int i = 0 ; i < 1000 ; i++ ){
            StoreMessage storeMessage = new StoreMessage();
            storeMessage.setBody(("append_" + i).getBytes());
            storeMessage.setCheckSum(i);
            storeMessage.setCreateTimestamp(i * 100);
            storeMessage.setTopic("my-topic");
            storeMessage.setStoreSize(storeMessage.calNeedByteSize());
            PutMessageResult putMessageResult = messageStoreService.putMessage(storeMessage);
            Assert.assertEquals(PutMessageStatus.PUT_OK , putMessageResult.getPutMessageStatus());
        }
    }

    @Test
    public void test_put_message_2(){
        MessageStoreService messageStoreService = serviceManager.getMessageStoreService();
        StoreMessage storeMessage = new StoreMessage();
        storeMessage.setBody(new byte[1024 * 1024 ]);
        storeMessage.setCheckSum(3);
        storeMessage.setCreateTimestamp(100);
        storeMessage.setTopic("topic");
        storeMessage.setStoreSize(storeMessage.calNeedByteSize());
        PutMessageResult putMessageResult = messageStoreService.putMessage(storeMessage);
        Assert.assertEquals(PutMessageStatus.MESSAGE_ILLEGAL , putMessageResult.getPutMessageStatus());
    }



}
