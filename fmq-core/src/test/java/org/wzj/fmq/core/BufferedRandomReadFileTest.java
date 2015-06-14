package org.wzj.fmq.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by wens on 15-6-14.
 */
public class BufferedRandomReadFileTest {

    private byte[] content = "dfdjfdjskfjdsfkldsjfdskljfdfjdkfsdgiioruttts,sdfjlzkidfdfdjdffldfkdfpowlsdffdsjkfdsghfgifdhlthjfdjhgfhdfjd dfdfdfdfdjfdjfjsdkfitrgjfgfdg ".getBytes() ;

    private File file ;

    @Before
    public void before(){
        file = new File( System.getProperty("user.home") + File.separator +".file") ;
        file.deleteOnExit();
        FileUtils.write( file, content ) ;

    }

    @Test
    public void test_read_0() throws IOException {

        BufferedRandomReadFile bufferedRandomReadFile = new BufferedRandomReadFile(file, 2);

        int n = 0 ;

        while(true){

            int read = bufferedRandomReadFile.read();

            if(read == -1 ){
                break;
            }
            Assert.assertEquals(content[n], read);

            n++ ;
        }

        Assert.assertEquals(n, content.length);
    }

    @Test
    public void test_read_1() throws IOException {

        BufferedRandomReadFile bufferedRandomReadFile = new BufferedRandomReadFile(file, 2);


        Random random = new Random();
        for (int i = 0 ; i < 100000 ; i++ ){
            int n = random.nextInt(content.length) ;
            bufferedRandomReadFile.seek(n);
            int read = bufferedRandomReadFile.read();
            Assert.assertEquals(content[n], read);
        }

    }

    @Test
    public void test_read_2() throws IOException {

        BufferedRandomReadFile bufferedRandomReadFile = new BufferedRandomReadFile(file, 5 );

        Random random = new Random();
        for (int i = 0 ; i < 100000 ; i++ ){
            int n = random.nextInt(content.length) ;
            bufferedRandomReadFile.seek(n);
            int r = Math.min(5,content.length - n ) ;
            byte[] bytes = new byte[r];
            int read = bufferedRandomReadFile.read(bytes);
            Assert.assertEquals(r , read);
            Assert.assertArrayEquals(Arrays.copyOfRange(content, n, n + r), bytes);
        }

    }

    @Test
    public void test_read_3() throws IOException {

        BufferedRandomReadFile bufferedRandomReadFile = new BufferedRandomReadFile(file, 5 );

        int nn = 9  ;
        int n = content.length - nn  ;
        bufferedRandomReadFile.seek(n);
        byte[] bytes = new byte[nn ];
        int r = bufferedRandomReadFile.read(bytes);
        Assert.assertEquals( nn  , r );
        Assert.assertArrayEquals(Arrays.copyOfRange(content, n, content.length), Arrays.copyOfRange(bytes, 0, r));

        bufferedRandomReadFile.seek(n + 6);
        r = bufferedRandomReadFile.read(bytes);
        Assert.assertEquals(  content.length - (n +6)  , r );
        Assert.assertArrayEquals(Arrays.copyOfRange(content , n+6 , content.length) , Arrays.copyOfRange(bytes , 0 , r ));


    }





}
