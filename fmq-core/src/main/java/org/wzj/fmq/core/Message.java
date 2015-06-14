package org.wzj.fmq.core;

/**
 * Created by wens on 15-6-12.
 */
public class Message {

    private long id ;

    private byte[] data ;

    public Message(long id, byte[] data) {
        this.id = id;
        this.data = data;
    }

    public Message() {

    }

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
