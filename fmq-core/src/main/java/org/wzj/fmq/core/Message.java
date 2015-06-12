package org.wzj.fmq.core;

/**
 * Created by wens on 15-6-12.
 */
public class Message {

    private long id ;

    private byte[] data ;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
