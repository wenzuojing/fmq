package org.wzj.fmq.core;

import java.io.*;

/**
 * Created by wens on 15-6-14.
 */
public class BufferedRandomReadFile implements Closeable {

    private int bufSize;
    private byte[] buf;
    private int bufPos ;
    private int freeBufSize ;
    private long realPos;

    private RandomAccessFile raf ;

    public BufferedRandomReadFile(String name , int bufSize)
            throws FileNotFoundException {
        this(new File(name), bufSize) ;
    }

    public BufferedRandomReadFile(File file, int bufSize)
            throws FileNotFoundException {
        this.raf  = new RandomAccessFile(file , "r" ) ;
        this.bufSize = bufSize;
        initBuf();
    }

    public int read() throws IOException {

        fillBufIfNeed();

        if (freeBufSize == 0)
            return -1;

        realPos++;
        freeBufSize-- ;
        return buf[bufPos++];
    }

    private void fillBufIfNeed() throws IOException {

        if(freeBufSize == 0 ){
            int n = this.raf.read(buf, 0, bufSize);
            bufPos = 0 ;
            if (n >= 0) {
                freeBufSize = n;
            }else{
                freeBufSize = 0  ;
            }
        }

    }

    public int read(byte[] b, int off, int len) throws IOException {
        int n = 0 ;
        while (true){
            fillBufIfNeed();

            if ( freeBufSize == 0){
                break;
            }


            int needRead = len - n ;

            if ( needRead <= freeBufSize ) {
                System.arraycopy(buf, bufPos, b, off + n , needRead );
                bufPos += needRead ;
                realPos += needRead ;
                n += needRead ;
                freeBufSize -= needRead ;
                return n ;
            }else{
                System.arraycopy(buf, bufPos, b, off + n , freeBufSize );
                bufPos += freeBufSize ;
                realPos += freeBufSize ;
                n += freeBufSize ;
                freeBufSize = 0 ;
            }
        }
        return n == 0 ? -1 : n ;
    }

    public int read(byte[] b) throws IOException {
        return this.read(b, 0, b.length );
    }

    public void seek(long pos) throws IOException {

        //  bufPos freeBufSize

        if( pos < realPos - bufPos  || pos >= realPos + freeBufSize -1  ){
            reset();
            realPos = pos;
            this.raf.seek(pos);
        }else{
            bufPos = (int) (pos - ( realPos - bufPos) ) ;
            freeBufSize = (int) (realPos + freeBufSize  - pos );
            realPos = pos;
        }

    }

    public long getFilePointer() {
        return realPos;
    }


    @Override
    public void close() throws IOException {
        if(this.raf != null ){
            this.raf.close();
        }
    }


    private void initBuf() {
        buf = new byte[bufSize];
        reset();
    }

    private void reset(){
        freeBufSize = 0 ;
        bufPos = 0 ;
        realPos = 0;
    }
}
