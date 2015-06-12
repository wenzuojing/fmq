package org.wzj.fmq.core;

/**
 * Created by wens on 15-6-11.
 */
public class SegmentStoreFullException  extends RuntimeException {

    public SegmentStoreFullException() {
    }

    public SegmentStoreFullException(String message) {
        super(message);
    }

    public SegmentStoreFullException(String message, Throwable cause) {
        super(message, cause);
    }

    public SegmentStoreFullException(Throwable cause) {
        super(cause);
    }

    public SegmentStoreFullException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
