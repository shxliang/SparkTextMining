package com.shxliang.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lsx
 */
public class HdfsUtil {
    private static Pattern hdfsPattern = Pattern.compile("hdfs://[\\d.]+(:\\d+)*?/", Pattern.CASE_INSENSITIVE);
    private static Pattern ipPattern = Pattern.compile(":\\d+?/", Pattern.CASE_INSENSITIVE);

    public static boolean hdfsFileExist(String hdfsPath) throws IOException
    {
        Configuration config = new Configuration();
        String serverIP = "";
        Matcher pathMatcher = hdfsPattern.matcher(hdfsPath);
        if (pathMatcher.find()) {
            serverIP = pathMatcher.group();
            serverIP = ipPattern.matcher(serverIP).replaceAll("/");
        }
        config.set("fs.default.name", serverIP);
        FileSystem hdfs = FileSystem.get(config);
        Path path = new Path(hdfsPath);
        return hdfs.exists(path);
    }
}
