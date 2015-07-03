package org.wzj.fmq.core.store.file.queue;

/**
 * Created by wens on 15-6-23.
 */
public class DataQueueRecoverResult {

    private final int status ; // 1 -> end queue  2 -> illegal message 3 -> normal end

    private final int process  ;

    public DataQueueRecoverResult(int status, int process) {
        this.status = status;
        this.process = process;
    }

    public int getStatus() {
        return status;
    }

    public int getProcess() {
        return process;
    }
}
