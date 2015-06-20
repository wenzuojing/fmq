package org.wzj.fmq.core.store.file.service;

import org.wzj.fmq.core.store.Lifecycle;
import org.wzj.fmq.core.store.file.MessageStoreConfig;

/**
 * Created by wens on 15-6-17.
 */
public class ServiceManager implements Lifecycle {

    private MessageStoreService messageStoreService;
    private IndexService indexService;
    private FlushService flushService;
    private CleanService cleanService;
    private DispatchMessageService dispatchMessageService;
    private MessageQueryService messageQueryService;
    private RunningService runningService ;


    private MessageStoreConfig messageStoreConfig;

    public ServiceManager(MessageStoreConfig messageStoreConfig) {

        this.messageStoreConfig = messageStoreConfig;
        this.messageStoreService = new MessageStoreService(this) ;
        this.indexService = new IndexService(this);
        this.messageQueryService = new MessageQueryService(this);
        this.flushService = new FlushService(this);
        this.cleanService = new CleanService(this);
        this.dispatchMessageService = new DispatchMessageService(this, this.messageStoreConfig.getPutMsgIndexHighWater());
        this.runningService = new RunningService();
    }

    public MessageStoreService getMessageStoreService() {
        return messageStoreService;
    }

    public IndexService getIndexService() {
        return indexService;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public FlushService getFlushService() {
        return flushService;
    }


    public CleanService getCleanService() {
        return cleanService;
    }

    public DispatchMessageService getDispatchMessageService() {
        return dispatchMessageService;
    }

    public MessageQueryService getMessageQueryService() {
        return messageQueryService;
    }

    @Override
    public void init() {

        this.dispatchMessageService.init();
        this.cleanService.init();
        this.flushService.init();
        this.messageStoreService.init();
        this.indexService.init();
        this.messageQueryService.init();
        runningService.init();

    }

    @Override
    public void start()  {
        this.dispatchMessageService.start();
        this.cleanService.start();
        this.flushService.start();
        this.messageStoreService.start();
        this.indexService.start();
        this.messageQueryService.start();
        this.runningService.start();
    }

    @Override
    public void shutdown() {

        this.cleanService.shutdown();
        this.dispatchMessageService.shutdown();
        this.flushService.shutdown();
        this.messageStoreService.shutdown();
        this.indexService.shutdown();
        this.messageQueryService.shutdown();
        this.runningService.shutdown();

    }

    public RunningService getRunningFlagsService() {
        return this.runningService ;
    }
}
