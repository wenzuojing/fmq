package org.wzj.fmq.core.file.service;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.wzj.fmq.core.store.file.MessageStoreConfig;
import org.wzj.fmq.core.store.file.service.ServiceManager;

/**
 * Created by wens on 15-6-21.
 */
public class BaseServiceTest {

    protected static ServiceManager serviceManager ;


    @BeforeClass
    public static void beforeClass(){
        serviceManager = new ServiceManager(new MessageStoreConfig()) ;
        serviceManager.init();
        serviceManager.start();
    }

    @AfterClass
    public static void afterClass(){
        serviceManager.shutdown();
    }
}
