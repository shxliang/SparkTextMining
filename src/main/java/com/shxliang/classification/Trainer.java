package com.shxliang.classification;

import com.shxliang.utils.MapUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lsx
 */
public class Trainer {
    public static void main(String[] args) throws IOException {
        SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("test");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        jsc.setLogLevel("WARN");
        SQLContext sqlContext = new SQLContext(jsc);

        registerUDF(sqlContext);

        DataFrame allDF = sqlContext.read()
                .parquet("hdfs://90.90.90.5:8020/user/lsx/mlModule/TextClassifierTrain/removed_sr_allData2.parquet");

        DataFrame[] dataFrames = allDF.randomSplit(new double[]{0.8, 0.2}, 1231);
        DataFrame trainDF = dataFrames[0];
        DataFrame testDF = dataFrames[1];

        String word2vecModelPath = "hdfs://90.90.90.5:8020/user/lsx/mlModule/word2vec.model";
        String[] mlpBaggingPath = ("hdfs://90.90.90.5:8020/user/lsx/mlModule/mlpc1.model," +
                "hdfs://90.90.90.5:8020/user/lsx/mlModule/mlpc2.model," +
                "hdfs://90.90.90.5:8020/user/lsx/mlModule/mlpc3.model," +
                "hdfs://90.90.90.5:8020/user/lsx/mlModule/mlpc4.model," +
                "hdfs://90.90.90.5:8020/user/lsx/mlModule/mlpc5.model").split(",");
        int[][] layers = new int[][]{{200, 20, 8},
                {200, 50, 8},
                {200, 100, 8},
                {200, 150, 8},
                {200, 50, 20, 8}};
        String indexerModelPath = "hdfs://90.90.90.5:8020/user/lsx/mlModule/indexer.model";
//        List<String> carBrand = jsc.textFile("hdfs://90.90.90.5:8020/user/lsx/mlModule/汽车品牌.txt").collect();

//        train(trainDF, "class", "segmentedWords", word2vecModelPath, mlpBaggingPath, indexerModelPath, layers);

        test(sqlContext, testDF, "class", "segmentedWords", word2vecModelPath, mlpBaggingPath, indexerModelPath);
    }

    private static void train(DataFrame trainDF, String labelCol, String wordsCol, String word2vecModelPath, String[] mlpBaggingPath, String indexerModelPath, int[][] layers) throws IOException {
        StringIndexerModel stringIndexerModel = PreProcessUtil.trainIndexerModel(indexerModelPath, trainDF, labelCol);
        trainDF = stringIndexerModel.transform(trainDF);

        Word2VecModel word2VecModel = Word2VecUtil.trainWord2Vec(word2vecModelPath, trainDF, wordsCol);
        trainDF = word2VecModel.transform(trainDF);

        MLPCUtil.trainMLPBagging(trainDF, "wordVec", "indexedClass", mlpBaggingPath, layers);
    }

    private static void test(SQLContext sqlContext, DataFrame testDF, String labelCol, String wordsCol, String word2vecModelPath, String[] mlpBaggingPath, String indexerModelPath) throws IOException {
        Configuration config = new Configuration();
        config.set("fs.default.name", "hdfs://90.90.90.5");
        FileSystem fileSystem = FileSystem.get(config);

        StringIndexerModel stringIndexerModel = StringIndexerModel.load(indexerModelPath);
        System.out.println("loaded indexer model from " + indexerModelPath);
        testDF = stringIndexerModel.transform(testDF);

        testDF = Word2VecUtil.runWord2Vec(word2vecModelPath, testDF, wordsCol);

        testDF = MLPCUtil.testMLPBagging(fileSystem, testDF, "wordVec", "indexedClass", mlpBaggingPath);

        testDF = testDF.withColumn("prediction",
                functions.callUDF("bagging",
                        functions.col("prediction_1"),
                        functions.col("prediction_2"),
                        functions.col("prediction_3"),
                        functions.col("prediction_4"),
                        functions.col("prediction_5")));
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedClass")
                .setPredictionCol("prediction")
                .setMetricName("f1");
        System.out.println("MLP-Bagging model F1: " + evaluator.evaluate(testDF));
    }

    private static void registerUDF(SQLContext sqlContext) {
        sqlContext.udf().register("bagging", new UDF5<Double, Double, Double, Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2, Double aDouble3, Double aDouble4, Double aDouble5) throws Exception {
                double[] prediction = new double[]{aDouble, aDouble2, aDouble3, aDouble4, aDouble5};
                Map<Double, Double> weight = new HashMap<>();
                for (int i = 0; i < prediction.length; i++) {
                    double curPre = prediction[i];
                    if (weight.containsKey(curPre)) {
                        weight.put(curPre, weight.get(curPre) + 1D);
                    } else {
                        weight.put(curPre, 1D);
                    }
                }
                // 按投票结果对类别降序排序
                weight = MapUtil.sortByValue(weight);
                return weight.keySet().iterator().next();
            }
        }, DataTypes.DoubleType);
    }
}
