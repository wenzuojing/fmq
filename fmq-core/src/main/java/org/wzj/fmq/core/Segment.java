package org.wzj.fmq.core;

import java.io.*;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by wens on 15-6-12.
 */
public abstract class Segment implements Comparable<Segment> , Lifecycle  {

    protected long sid ;

    protected long cid ;

    protected int maxByteSize ;

    protected int byteSize ;

    protected ReadWriteLock rwLock = new ReentrantReadWriteLock() ;

    protected SortedSet<ChunkInfo>  chunkInfos ;

    public Segment(long sid , int maxByteSize ) {
        this.sid = sid;
        this.cid = sid -1  ;
        this.maxByteSize = maxByteSize ;
        this.byteSize = 0 ;
        this.chunkInfos = new TreeSet<>() ;
    }

    @Override
    public void init(){
        loadChunkInfo();
    }

    public Segment(long sid) {
        this.sid = sid ;
    }

    protected abstract void loadChunkInfo() ;


    public Message query(long id ) throws IOException {

        ChunkInfo findChunkInfo ;

        rwLock.readLock().lock();
        try{
            ChunkInfo first = chunkInfos.tailSet(ChunkInfo.toChunkInfo(id)).first();
            if(first.id != id ){
                return null ;
            }
            findChunkInfo = first ;
        }finally {
            rwLock.readLock().unlock();
        }

        return query( findChunkInfo ) ;
    }


    protected abstract Message query(ChunkInfo chunkInfo ) throws IOException;

    @Override
    public int compareTo(Segment o) {
        return (int)( sid - o.sid ) ;
    }

    public void write(Message message  , ChunkInfo  chunkInfo  ) throws IOException {

        rwLock.writeLock().lock();
        try{
            long id  = cid  + 1 ;
            chunkInfo.id = id ;
            chunkInfo.offset = byteSize ;
            byte[] header = chunkInfo.encode();
            write_0(header) ;
            write_0(message.getData()) ;
            byteSize += header.length + message.getData().length ;
            message.setId(id);
            this.cid = id ;
            this.chunkInfos.add(chunkInfo);
        }finally {
            rwLock.writeLock().unlock();
        }
    }

    protected abstract void write_0(byte[] bytes) throws IOException ;

    public long getSid() {
        return sid;
    }


    public boolean isFull() {

        rwLock.readLock().lock(); ;
        try{
            if(byteSize > maxByteSize) {
                return true ;
            }else {
                return false ;
            }
        }finally {
            rwLock.readLock().unlock();
        }

    }

    @Override
    public void close()  {
        chunkInfos.clear();
    }



    public long getCid() {
        return cid;
    }

    public static Segment toSegment(long sid){
        return new Segment(sid){

            @Override
            protected void loadChunkInfo() {
                throw new UnsupportedOperationException() ;
            }

            @Override
            public Message query(long id) throws IOException {
                throw new UnsupportedOperationException() ;
            }

            @Override
            protected Message query(ChunkInfo chunkInfo) {
                return null;
            }

            @Override
            public int compareTo(Segment o) {
                return super.compareTo(o);
            }

            @Override
            public void write(Message message, ChunkInfo chunkInfo) throws IOException {
                throw new UnsupportedOperationException() ;
            }

            @Override
            protected void write_0(byte[] bytes) throws IOException {
                throw new UnsupportedOperationException() ;
            }

            @Override
            public long getSid() {
                return super.getSid();
            }

            @Override
            public boolean isFull() {
                throw new UnsupportedOperationException() ;
            }

            @Override
            public void close()  {
                throw new UnsupportedOperationException() ;
            }

            @Override
            public long getCid() {
                throw new UnsupportedOperationException() ;
            }
        } ;
    }
}
