package com.shxliang.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

/**
 *
 * @author lsx
 * @date 2017/7/14
 */
public class WriteObjectUtil {
    public static void writeObjectToFile(Object obj, String path) throws FileNotFoundException {
        File file =new File(path);
        FileOutputStream out;
        try {
            out = new FileOutputStream(file);
            ObjectOutputStream objOut = new ObjectOutputStream(out);
            objOut.writeObject(obj);
            objOut.flush();
            objOut.close();
            System.out.println("Write object successfully!");
        } catch (IOException e) {
            System.out.println("Failed to write object!");
            e.printStackTrace();
        }
    }

    public static Object readObjectFromFile(String path)
    {
        Object temp=null;
        File file =new File(path);
        FileInputStream in;
        try {
            in = new FileInputStream(file);
            ObjectInputStream objIn=new ObjectInputStream(in);
            temp=objIn.readObject();
            objIn.close();
            System.out.println("Read object successfully!");
        } catch (IOException e) {
            System.out.println("Failed to read object!");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return temp;
    }

    public static Object readObjectFromHDFS(FileSystem fileSystem, String path)
    {
        Object temp=null;
        Path filePath =new Path(path);
        InputStream inputStream;
        try {
            inputStream = fileSystem.open(filePath);
            ObjectInputStream objIn=new ObjectInputStream(inputStream);
            temp=objIn.readObject();
            objIn.close();
            System.out.println("Read object successfully!");
        } catch (IOException e) {
            System.out.println("Failed to read object!");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return temp;
    }
}
