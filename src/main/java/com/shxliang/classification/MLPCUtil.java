package com.shxliang.classification;

import com.shxliang.utils.WriteObjectUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.util.Arrays;

/**
 * MLPC工具类
 *
 * @author lsx
 */
public class MLPCUtil {

    /**
     * 训练MLP-Bagging模型，并存储模型
     *
     * @param trainDF        训练集DataFrame
     * @param featureCol     word2vec特征列列名
     * @param labelCol       类别ID列列名
     * @param mlpBaggingPath MLP-Bagging模型保存路径
     * @param layers         每个MLP参数
     * @throws IOException
     */
    public static void trainMLPBagging(DataFrame trainDF, String featureCol, String labelCol, String[] mlpBaggingPath, int[][] layers) throws IOException {
        assert mlpBaggingPath.length == layers.length;

        for (int i = 0; i < mlpBaggingPath.length; i++) {
            String curModelPath = mlpBaggingPath[i];
            int[] curLayers = layers[i];
            MultilayerPerceptronClassifier mlpc = new MultilayerPerceptronClassifier()
                    .setFeaturesCol(featureCol)
                    .setLabelCol(labelCol)
                    .setPredictionCol("prediction_" + (i + 1))
                    .setLayers(curLayers)
                    .setMaxIter(200);
            MultilayerPerceptronClassificationModel mlpcModel = mlpc.fit(trainDF);
            WriteObjectUtil.writeObjectToFile(mlpcModel, curModelPath);
            System.out.println("saved mlpc_" + (i + 1) + " model to " + curModelPath);
        }
    }

    public static DataFrame testMLPBagging(FileSystem fileSystem, DataFrame testDF, String featureCol, String labelCol, String[] mlpBaggingPath) {
        DataFrame result = testDF;
        String hdfsPrefix = "hdfs";

        for (int i = 0; i < mlpBaggingPath.length; i++) {
            String curModelPath = mlpBaggingPath[i];
            MultilayerPerceptronClassificationModel mlpcModel;

            /*
            判断从HDFS加载模型还是从本地加载模型
            */
            if (!curModelPath.startsWith(hdfsPrefix)) {
                mlpcModel = (MultilayerPerceptronClassificationModel) WriteObjectUtil.readObjectFromFile(curModelPath);
            } else {
                mlpcModel = (MultilayerPerceptronClassificationModel) WriteObjectUtil.readObjectFromHDFS(fileSystem, curModelPath);
            }
            System.out.println("loaded mlpc_" + (i + 1) + " model from " + curModelPath + ", layers: " + Arrays.toString(mlpcModel.layers()));
            mlpcModel.setPredictionCol("prediction_" + (i + 1));
            result = mlpcModel.transform(result);

            MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                    .setLabelCol(labelCol)
                    .setPredictionCol("prediction_" + (i + 1))
                    .setMetricName("f1");
            System.out.println("mlpc_" + (i + 1) + " model F1: " + evaluator.evaluate(result));
        }

        return result;
    }

    /**
     * 训练MLPC模型，并将模型保存
     *
     * @param jsc
     * @param sqlContext
     * @param modelSavePath 模型存储路径
     * @param trainDF       训练集DataFrame
     * @param testDF        测试集DataFrame
     * @param featureCol    特征列列名
     * @param labelCol      类别列列名
     * @return
     * @throws IOException
     */
    public static DataFrame runMLPC(JavaSparkContext jsc, SQLContext sqlContext, String modelSavePath, DataFrame trainDF, DataFrame testDF, String featureCol, String labelCol) throws IOException {

        // 每层节点数
        int[] layers = new int[]{200, 50, 8};

        MultilayerPerceptronClassifier mlpc = new MultilayerPerceptronClassifier()
                .setFeaturesCol(featureCol)
                .setLabelCol(labelCol)
                .setPredictionCol("prediction")
                .setLayers(layers)
                .setMaxIter(200);
        MultilayerPerceptronClassificationModel mlpcModel = mlpc.fit(trainDF);

        WriteObjectUtil.writeObjectToFile(mlpcModel, modelSavePath);

        DataFrame result = mlpcModel.transform(testDF);

        return result;
    }

    /**
     * 读取保存的MLPC模型，并对testDF进行预测
     *
     * @param fileSystem    HDFS File System
     * @param modelLoadPath 模型路径
     * @param testDF        待预测数据DataFrame
     * @return
     * @throws IOException
     */
    public static DataFrame runMLPC(FileSystem fileSystem, String modelLoadPath, DataFrame testDF) throws IOException {
        MultilayerPerceptronClassificationModel mlpcModel;
        String hdfsPrefix = "hdfs";
        /*
        判断从HDFS加载模型还是从本地加载模型
         */
        if (!modelLoadPath.startsWith(hdfsPrefix)) {
            mlpcModel = (MultilayerPerceptronClassificationModel) WriteObjectUtil.readObjectFromFile(modelLoadPath);
        } else {
            mlpcModel = (MultilayerPerceptronClassificationModel) WriteObjectUtil.readObjectFromHDFS(fileSystem, modelLoadPath);
        }
        System.out.println("loaded MLPC model from " + modelLoadPath + ", layers: " + Arrays.toString(mlpcModel.layers()));
        DataFrame result = mlpcModel.transform(testDF);
        return result;
    }
}
