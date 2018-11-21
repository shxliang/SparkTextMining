package cn.com.cloudpioneer.mrp;

import cn.com.cloudpioneer.textclassification.TextClassifier;
import cn.com.cloudpioneer.textkeywords.TextRankKeyword;
import cn.com.cloudpioneer.textsentiment.TextSentiment;
import cn.com.cloudpioneer.textsummary.TextRankSummary;
import cn.com.cloudpioneer.utils.ArgumentUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

/**
 * @author lsx
 * @date 2017/5/31
 */

public class Main {
    public static void main(String[] args) throws IOException {

        /*
         * @param inputPath
         * @param outputPath
         * @param sourceColName
         * @param siteColName
         * @param allTitleColName
         * @param allContentColName
         * @param removeWordsPath
         * @param nKeywords
         * @param windowSize
         * @param d
         * @param contentColName
         * @param maxLength
         * @param titleColName
         * @param sentimentPath
         * @param advPath
         * @param negPath
         * @param titlePlaceColName
         * @param contentPlaceColName
         * @param placeNum
         * @param trainDataPath
         *
         * @param word2vecModelPath
         * @param mlpcModelPath
         * @param indexerModelPath
         */

        /*
        Spark环境初始化
         */
        SparkConf sc = new SparkConf();
//        sc.setMaster("local[*]").setAppName("test");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        jsc.setLogLevel("WARN");
        SQLContext sqlContext = new SQLContext(jsc);

        List<String> mustParam = Arrays.asList("inputPath", "outputPath");
        Map<String, String> paraMap = ArgumentUtil.load(args, mustParam);
        final Broadcast<Map<String, String>> paraMapBroadcast = jsc.broadcast(paraMap);

        // 关键词过滤列表
        final List<String> remove = jsc.textFile(paraMapBroadcast.value().get("removeWordsPath")).collect();
        // 情感词列表
        List<Tuple2<String, Double>> senList = jsc.textFile(paraMapBroadcast.value().get("sentimentPath"))
                .map(new Function<String, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> call(String s) {
                        String[] parts = s.split("\t");
                        int correctLength = 2;
                        if (parts.length == correctLength) {
                            return new Tuple2<String, Double>(parts[0].trim(), Double.parseDouble(parts[1].trim()));
                        } else {
                            return null;
                        }
                    }
                })
                .collect();
        // 副词列表
        List<Tuple2<String, Double>> advList = jsc.textFile(paraMapBroadcast.value().get("advPath"))
                .map(new Function<String, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> call(String s) {
                        String[] parts = s.split("\t");
                        int correctLength = 2;
                        if (parts.length == correctLength) {
                            return new Tuple2<String, Double>(parts[0].trim(), Double.parseDouble(parts[1].trim()));
                        } else {
                            return null;
                        }
                    }
                })
                .collect();
        // 否定词列表
        List<String> negList = jsc.textFile(paraMapBroadcast.value().get("negPath")).collect();
        ArrayList<String> negArrayList = new ArrayList<>();
        negArrayList.addAll(negList);
        // 将词列表转化为Map<词,权重值>
        Map<String, Double> senMap = new HashMap<>(senList.size());
        for (int i = 0; i < senList.size(); i++) {
            if (senList.get(i) != null) {
                senMap.put(senList.get(i)._1().trim(), senList.get(i)._2());
            }
        }
        Map<String, Double> advMap = new HashMap<>(advList.size());
        for (int i = 0; i < advList.size(); i++) {
            if (advList.get(i) != null) {
                advMap.put(advList.get(i)._1().trim(), advList.get(i)._2());
            }
        }
        final Broadcast<Map<String, Double>> senWords = jsc.broadcast(senMap);
        final Broadcast<Map<String, Double>> advWords = jsc.broadcast(advMap);
        final Broadcast<ArrayList<String>> negWords = jsc.broadcast(negArrayList);

        // 配置hdfs文件系统
        Configuration config = new Configuration();
        config.set("fs.default.name", "90.90.90.5");
        FileSystem fileSystem = FileSystem.get(config);

        // registerUDF
        // 原创识别
        sqlContext.udf().register("getOrigin", new UDF2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return FindOrigin.getOrigin(s, s2);
            }
        }, DataTypes.StringType);

        // 关键词提取
        sqlContext.udf().register("getKeywords", new UDF2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                if (s2 == null || s2.length() < 1) {
                    return null;
                }
                int nKeywords = Integer.parseInt(paraMapBroadcast.value().get("nKeywords"));
                int windowSize = Integer.parseInt(paraMapBroadcast.value().get("windowSize"));
                double d = Double.parseDouble(paraMapBroadcast.value().get("d"));
                int maxIter = 200;
                double minDiff = 0.0001;
                return TextRankKeyword.getKeywords(s, s2, nKeywords, windowSize, d, maxIter, minDiff, remove);
            }
        }, DataTypes.StringType);

        // 摘要提取
        sqlContext.udf().register("getSummary", new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception {
                if (s == null || s.length() < 1) {
                    return null;
                }
                int maxLength = Integer.parseInt(paraMapBroadcast.value().get("maxLength").trim());
                double d = Double.parseDouble(paraMapBroadcast.value().get("d").trim());
                int maxIter = 200;
                double minDiff = 0.0001;
                return TextRankSummary.getSummary(s, maxLength, d, maxIter, minDiff);
            }
        }, DataTypes.StringType);

        // 情感分析
        sqlContext.udf().register("getSentiment", new UDF2<String, String, ArrayList<Double>>() {
            @Override
            public ArrayList<Double> call(String s, String s2) throws Exception {
                //合并标题
                String str = s + "。" + s2;
                //分割成句子
                String[] parts = str.split("[，,。？?！!；;\n\r]");
                ArrayList<Double> sentimentResult = new ArrayList<Double>();
                sentimentResult = TextSentiment.getSentiment(parts, senWords.value(), advWords.value(), negWords.value(), "percent");
                return sentimentResult;
            }
        }, DataTypes.createArrayType(DataTypes.DoubleType, false));

        // 地名识别
        sqlContext.udf().register("getRegion", new UDF2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                int placeNum = Integer.parseInt(paraMapBroadcast.value().get("placeNum"));
                return RegionRecognition.getRegion(s, s2, placeNum);
            }
        }, DataTypes.StringType);


        DataFrame inputData = sqlContext.read()
                .parquet(paraMapBroadcast.value().get("inputPath"));

        // 读取汽车品牌词典
        List<String> carBrand = jsc.textFile("hdfs://90.90.90.5:8020/user/lsx/mlModule/汽车品牌.txt").collect();

        // 文本分类
        String word2vecModelPath = paraMapBroadcast.getValue().get("word2vecModelPath");
        String[] mlpcModelPath = paraMapBroadcast.getValue().get("mlpcModelPath").split(",");
        String indexerModelPath = paraMapBroadcast.getValue().get("indexerModelPath");
        inputData = TextClassifier.doTextClassify(fileSystem, jsc, sqlContext, inputData, word2vecModelPath, mlpcModelPath, indexerModelPath, carBrand);
        inputData.registerTempTable("inputData");

        DataFrame outputData = sqlContext.sql("SELECT *," +
                "getOrigin(" + paraMapBroadcast.value().get("siteColName") + "," + paraMapBroadcast.value().get("sourceColName") + ") AS original," +
                "getKeywords(" + paraMapBroadcast.value().get("allTitleColName") + "," + paraMapBroadcast.value().get("allContentColName") + ") AS keywords," +
                "getSummary(" + paraMapBroadcast.value().get("contentColName") + ") AS summary," +
                "getSentiment(" + paraMapBroadcast.value().get("titleColName") + "," + paraMapBroadcast.value().get("contentColName") + ") AS sentiment," +
                "getRegion(" + paraMapBroadcast.value().get("titlePlaceColName") + "," + paraMapBroadcast.value().get("contentPlaceColName") + ") AS region " +
                "FROM inputData");


        outputData.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet(paraMapBroadcast.value().get("outputPath"));

//        outputData.show();

        jsc.stop();
    }
}
