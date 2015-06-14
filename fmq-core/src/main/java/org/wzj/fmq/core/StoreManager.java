package org.wzj.fmq.core;

import java.io.IOException;
import java.util.List;

/**
 * Created by wens on 15-6-14.
 */
public class StoreManager implements  DataStore {



    @Override
    public void write(Message message) throws IOException {

    }

    @Override
    public List<Message> query(long startId, int size) throws IOException {
        return null;
    }

    @Override
    public void init() {

    }

    @Override
    public void close() {

    }
}
