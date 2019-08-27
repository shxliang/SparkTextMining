package com.shxliang.classification;

import com.shxliang.utils.MapUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.*;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;
import java.util.*;

import static com.shxliang.classification.MLPCUtil.runMLPC;
import static com.shxliang.classification.Word2VecUtil.runWord2Vec;

/**
 * @author lsx
 * @date 2017/1/5
 */
public class TextClassifier {
    /**
     * 读取模型进行文本分类
     *
     * @param fileSystem        HDFS File System
     * @param jsc
     * @param sqlContext
     * @param testDF            待预测数据DataFrame
     * @param word2vecModelPath word2vec模型路径
     * @param mlpcModelPath     mlp-bagging模型路径
     * @param indexerModelPath  类别-类别ID映射模型路径
     * @param carBrand          车牌词表路径
     * @return 增加分类结果新列
     * @throws IOException
     */
    public static DataFrame doTextClassify(FileSystem fileSystem, JavaSparkContext jsc, SQLContext sqlContext, DataFrame testDF,
                                           String word2vecModelPath, String[] mlpcModelPath, String indexerModelPath,
                                           final List<String> carBrand) throws IOException {

        final StringIndexerModel indexerModel = StringIndexerModel.load(indexerModelPath);
        System.out.println("loaded label indexer model from " + indexerModelPath);

        /*
        合并标题和正文
         */
        sqlContext.udf().register("combine", new UDF2<String, String, String>() {
            @Override
            public String call(String s1, String s2) {
                String result = s1 + " " + s2;
                return result;
            }
        }, DataTypes.StringType);

//        sqlContext.udf().register("bagging5", new UDF5<Double, Double, Double, Double, Double, String>() {
//            @Override
//            public String call(Double aDouble, Double aDouble2, Double aDouble3, Double aDouble4, Double aDouble5) throws Exception {
//                Map<Double,Double> weight = new HashMap<>();
//                double[] prediction = new double[]{aDouble, aDouble2, aDouble3, aDouble4, aDouble5};
//                for (int i=0;i<prediction.length;i++)
//                {
//                    double curPre = prediction[i];
//                    if (weight.containsKey(curPre))
//                        weight.put(curPre, weight.get(curPre) + 1D);
//                    else
//                        weight.put(curPre, 1D);
//                }
//                weight = MapUtil.sortByValue(weight);
//                return indexerModel.labels()[weight.keySet().iterator().next().intValue()];
//            }
//        }, DataTypes.StringType);
//
//        sqlContext.udf().register("bagging5_2", new UDF5<Double, Double, Double, Double, Double, String>() {
//            @Override
//            public String call(Double aDouble, Double aDouble2, Double aDouble3, Double aDouble4, Double aDouble5) throws Exception {
//                Map<Double,Double> weight = new HashMap<>();
//                double[] prediction = new double[]{aDouble, aDouble2, aDouble3, aDouble4, aDouble5};
//                for (int i=0;i<prediction.length;i++)
//                {
//                    double curPre = prediction[i];
//                    if (weight.containsKey(curPre))
//                        weight.put(curPre, weight.get(curPre) + 1D);
//                    else
//                        weight.put(curPre, 1D);
//                }
//                weight = MapUtil.sortByValue(weight);
//                StringBuilder result = new StringBuilder();
//                Iterator<Double> iteKey = weight.keySet().iterator();
//                Iterator<Double> iteValue = weight.values().iterator();
//                while (iteKey.hasNext())
//                {
//                    String curKey = indexerModel.labels()[iteKey.next().intValue()];
//                    result.append(curKey + " ");
//                }
//                return result.toString().trim();
//            }
//        }, DataTypes.StringType);

        /*
        5个MLP集成bagging预测
         */
        sqlContext.udf().register("bagging5_3", new UDF6<Double, Double, Double, Double, Double, String, String>() {
            @Override
            public String call(Double aDouble, Double aDouble2, Double aDouble3, Double aDouble4, Double aDouble5, String s) throws Exception {
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

                List<String> docClassLabel = new ArrayList<>();
                Iterator<Double> iteKey = weight.keySet().iterator();
//                Iterator<Double> iteValue = weight.values().iterator();

                while (iteKey.hasNext()) {
                    boolean containCarBrand = false;
                    // 将预测值转化为中文标签
                    String curKey = indexerModel.labels()[iteKey.next().intValue()];
                    // 对汽车类别的文章，判断文章中是否含车牌名，不含的话就舍弃汽车类别
                    if ("汽车".equals(curKey)) {
                        for (String brand : carBrand) {
                            if (s.contains(brand)) {
                                containCarBrand = true;
                                break;
                            }
                        }
                        if (!containCarBrand) {
                            continue;
                        }
                    }
                    docClassLabel.add(curKey);
                }

                // 若文章只分为汽车类且不含车牌名，则将其标为时事类
                if (docClassLabel.size() == 0) {
                    docClassLabel.add("时事");
                }

                // 若只有一个类别，则可以直接返回
                if (docClassLabel.size() == 1) {
                    return docClassLabel.toString().replaceAll("[\\[\\]]", "");
                }

                /*
                根据firstLabel来判断后续label是否满足firstLabel的约束条件
                 */
                String firstLabel = docClassLabel.get(0);
                for (String str : docClassLabel) {
                    if (firstLabel.equals(str)) {
                        continue;
                    }
                    if (isIllegalExist(firstLabel, str)) {
                        docClassLabel.remove(str);
                    }
                }

                return docClassLabel.toString().replaceAll("[,\\[\\]]", "");
            }
        }, DataTypes.StringType);


        /*
        判断是否含segmentedWords字段，若不含则将segmentedTitle和segmentedText合并
         */
        boolean containSegmentedWords = false;
        String[] columns = testDF.columns();
        for (String column : columns) {
            if ("segmentedWords".equals(column)) {
                containSegmentedWords = true;
                break;
            }
        }
        if (!containSegmentedWords) {
            testDF = testDF.withColumn("segmentedWords", functions.callUDF("combine", functions.col("segmentedTitle"), functions.col("segmentedText")));
        }

        // 计算待预测数据的word2vec特征，输出结果列名为“words”
        testDF = runWord2Vec(word2vecModelPath, testDF, "segmentedWords");

        int index = 0;
        // 存储预测的中文类别标签集合
        StringBuilder preStr = new StringBuilder();
//        final double[] f1Array = new double[MLPCModelPath.length];

        /*
        循环遍历MLPC-bagging中的模型进行预测，不断添加新列
         */
        DataFrame result = testDF;
        for (String curMLPC : mlpcModelPath) {
            result = runMLPC(fileSystem, curMLPC, result)
                    .withColumnRenamed("prediction", "prediction_" + index);

            // 记录每个MLP结果的列名
            preStr.append("prediction_").append(index).append(",");

//            MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
//                    .setMetricName("f1")
//                    .setLabelCol("indexedClass")
//                    .setPredictionCol("prediction_"+index);
//            f1Array[index] = evaluator.evaluate(result);

            index++;
        }

        result.registerTempTable("result");

        // 对预测的中文类别标签集合进行计算，得出最终预测结果
        result = sqlContext.sql("SELECT *," +
                "bagging5_3(" + preStr.toString().substring(0, preStr.length() - 1) + ",segmentedWords) AS docClass " +
                "FROM result");
        // 删除word2vec的结果列words
        result = result.drop("words");

        // transform prediction to docClass，使用bagging5时才需要
//        IndexToString indexToString = new IndexToString().setInputCol("prediction")
//                .setOutputCol("docClass")
//                .setLabels(indexerModel.labels());
//        result = indexToString.transform(result);

        return result;
    }



    /**
     * 基于第一类别判断当前类别是否不满足规则，“军事”类别不能和某些类别同时存在
     *
     * @param firstLabel 第一类别（概率最大的类别）
     * @param curLabel   当前类别
     * @return 是否不满足规则
     */
    private static boolean isIllegalExist(String firstLabel, String curLabel) {
        return "军事".equals(firstLabel) && isExistWithMilitary(curLabel);
    }

    /**
     * 判断是否是不能和“军事”类别同时存在的类别
     *
     * @param label 类别
     * @return 是否存在
     */
    private static boolean isExistWithMilitary(String label) {
        return "娱乐".equals(label) || "旅游".equals(label) || "体育".equals(label) || "汽车".equals(label);
    }
}
