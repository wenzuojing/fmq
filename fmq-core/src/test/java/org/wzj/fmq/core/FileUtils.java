package org.wzj.fmq.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by wens on 15-6-11.
 */
public class FileUtils {

    public static String readAsString(File file) {
        FileInputStream fileInputStream = null;

        try {
            fileInputStream = new FileInputStream(file);
            byte[] bytes = new byte[fileInputStream.available()];
            fileInputStream.read(bytes);
            return new String(bytes);
        } catch (Exception e) {
            throw new RuntimeException("read file fail", e);
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    //
                }
            }
        }

    }

    public static void deleteDir(String dir) {

        File file = new File(dir);

        if(!file.exists()){
            return ;
        }

        File[] files = file.listFiles();

        for(File f : files ){
            if(f.isFile()){
                f.delete();
            }

            try {
                deleteDir(f.getCanonicalPath());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        file.delete() ;


    }

    public static void write(File file, byte[] content) {
        FileOutputStream out = null;
        try{
            out = new FileOutputStream(file) ;
            out.write(content);
        }catch (Exception e ){
            e.printStackTrace();
        }finally {
            if(out != null ){
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


    }
}
