package org.wzj.fmq.core.store.file.queue;

import org.wzj.fmq.core.store.file.Lifecycle;

/**
 * Created by wens on 15-6-17.
 */
public interface FileQueue extends Lifecycle {

    long getQueueCreateTimestamp();

    void delete();

    void setCommittedPosition(int i);

    long getFromOffset();

    void setWrotePosition(int i);

    boolean isFull();

    long getWritePosition();

    void commit();

    boolean isDirty();

    boolean isValid();

    long recover();

}
