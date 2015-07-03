package org.wzj.fmq.core.store.file.queue;


import org.wzj.fmq.core.store.Lifecycle;

/**
 * Created by wens on 15-6-17.
 */
public interface FileQueue extends Lifecycle {


    void delete();

    void setCommittedPosition(int i);

    long getFromOffset();

    void setWritePosition(int i);

    boolean isFull();

    int getWritePosition();

    void commit();

    boolean isDirty();

    boolean isValid();

}
