package org.wzj.fmq.core;

import java.io.File;
import java.io.FileInputStream;
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
}
