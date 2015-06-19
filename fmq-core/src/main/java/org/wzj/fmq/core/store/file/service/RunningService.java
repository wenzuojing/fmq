package org.wzj.fmq.core.store.file.service;

import org.wzj.fmq.core.store.file.Lifecycle;

/**
 * Created by wens on 15-6-17.
 */
public class RunningService implements Lifecycle {

    // 禁止读权限
    private static final int NotReadableBit = 1;
    // 禁止写权限
    private static final int NotWriteableBit = 1 << 1;
    // 逻辑队列是否发生错误
    private static final int WriteLogicsQueueErrorBit = 1 << 2;
    // 索引文件是否发生错误
    private static final int WriteIndexFileErrorBit = 1 << 3;
    // 磁盘空间不足
    private static final int DiskFullBit = 1 << 4;
    private volatile int flagBits = 0;

    public int getFlagBits() {
        return flagBits;
    }


    public boolean getAndMakeReadable() {
        boolean result = this.isReadable();
        if (!result) {
            this.flagBits &= ~NotReadableBit;
        }
        return result;
    }


    public boolean isReadable() {
        if ((this.flagBits & NotReadableBit) == 0) {
            return true;
        }

        return false;
    }


    public boolean getAndMakeNotReadable() {
        boolean result = this.isReadable();
        if (result) {
            this.flagBits |= NotReadableBit;
        }
        return result;
    }


    public boolean getAndMakeWriteable() {
        boolean result = this.isWriteable();
        if (!result) {
            this.flagBits &= ~NotWriteableBit;
        }
        return result;
    }


    public boolean isWriteable() {
        if ((this.flagBits & (NotWriteableBit | WriteLogicsQueueErrorBit | DiskFullBit | WriteIndexFileErrorBit)) == 0) {
            return true;
        }

        return false;
    }


    public boolean getAndMakeNotWriteable() {
        boolean result = this.isWriteable();
        if (result) {
            this.flagBits |= NotWriteableBit;
        }
        return result;
    }


    public void makeLogicsQueueError() {
        this.flagBits |= WriteLogicsQueueErrorBit;
    }


    public boolean isLogicsQueueError() {
        if ((this.flagBits & WriteLogicsQueueErrorBit) == WriteLogicsQueueErrorBit) {
            return true;
        }

        return false;
    }


    public void makeIndexFileError() {
        this.flagBits |= WriteIndexFileErrorBit;
    }


    public boolean isIndexFileError() {
        if ((this.flagBits & WriteIndexFileErrorBit) == WriteIndexFileErrorBit) {
            return true;
        }

        return false;
    }


    /**
     * 返回Disk是否正常
     */
    public boolean getAndMakeDiskFull() {
        boolean result = !((this.flagBits & DiskFullBit) == DiskFullBit);
        this.flagBits |= DiskFullBit;
        return result;
    }


    /**
     * 返回Disk是否正常
     */
    public boolean getAndMakeDiskOK() {
        boolean result = !((this.flagBits & DiskFullBit) == DiskFullBit);
        this.flagBits &= ~DiskFullBit;
        return result;
    }

    @Override
    public void init() {

    }

    @Override
    public void start()  {

    }

    @Override
    public void shutdown() {

    }
}
