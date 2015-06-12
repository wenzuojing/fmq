package org.wzj.fmq.core;

/**
 * Created by wens on 15-6-12.
 */
public class ClosedIndexException extends RuntimeException {

    public ClosedIndexException() {
    }

    public ClosedIndexException(String message) {
        super(message);
    }

    public ClosedIndexException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClosedIndexException(Throwable cause) {
        super(cause);
    }

    public ClosedIndexException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
