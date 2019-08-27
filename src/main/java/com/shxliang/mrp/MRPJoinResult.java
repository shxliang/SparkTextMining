package com.shxliang.mrp;

import com.shxliang.utils.ArgumentUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 * @author lsx
 * @date 2017/5/31
 * 合并分析结果
 */
public class MRPJoinResult {
    public static void main(String[] args) throws IOException {
        /**
         * @param inputPath
         * @param outputPath
         * @param newsGroupPath
         * @param newsTopicPath
         */

        SparkConf sc = new SparkConf();
//        sc.setMaster("local[*]").setAppName("test");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);
        List<String> mustParam = Arrays.asList("inputPath", "outputPath");
        Map<String, String> paraMap = ArgumentUtil.load(args, mustParam);
        final Broadcast<Map<String, String>> paraMapBroadcast = jsc.broadcast(paraMap);

        DataFrame inputData = sqlContext.read()
                .parquet(paraMapBroadcast.value().get("inputPath"));
        inputData.registerTempTable("inputData");

        DataFrame newsGroup = sqlContext.read()
                .parquet(paraMapBroadcast.value().get("newsGroupPath"));
        newsGroup.registerTempTable("newsGroup");

        DataFrame newsTopic = sqlContext.read()
                .parquet(paraMapBroadcast.value().get("newsTopicPath"));
        newsTopic.registerTempTable("newsTopic");

        DataFrame result = sqlContext.sql("SELECT inputData.*," +
                "newsGroup.groupId," +
                "newsGroup.signature " +
                "FROM inputData,newsGroup " +
                "WHERE inputData.docId=newsGroup.docId");

        result.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet(paraMapBroadcast.value().get("outputPath"));

        jsc.stop();
    }
}
