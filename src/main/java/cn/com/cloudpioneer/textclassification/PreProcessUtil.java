package cn.com.cloudpioneer.textclassification;

import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.DataFrame;

import java.io.IOException;

/**
 * 文档预处理工具类
 *
 * @author lsx
 */
public class PreProcessUtil {
    /**
     * 将给定列按空格切分为词向量列
     *
     * @param inputDF   输入数据DataFrame
     * @param inputCol  输入列列名
     * @param outputCol 切分后词向量列列名
     * @return
     */
    public static DataFrame runTokenizer(DataFrame inputDF, String inputCol, String outputCol) {
        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol(inputCol)
                .setOutputCol(outputCol)
                .setPattern(" ");
        DataFrame outputDF = regexTokenizer.transform(inputDF);
        return outputDF;
    }

    /**
     * 将给定列按给定符号切分为列向量
     *
     * @param inputDF   输入数据DataFrame
     * @param inputCol  输入列列名
     * @param outputCol 切分后词向量列列名
     * @param sep       分隔符
     * @return
     */
    public static DataFrame runTokenizer(DataFrame inputDF, String inputCol, String outputCol, String sep) {
        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol(inputCol)
                .setOutputCol(outputCol)
                .setPattern(sep);
        DataFrame outputDF = regexTokenizer.transform(inputDF);
        return outputDF;
    }

    /**
     * 保存类别-类别ID的映射模型，并存储模型，模型的outputCol为"indexedClass"
     *
     * @param modelSavePath 模型存储路径
     * @param trainDF       训练集DataFrame
     * @param classCol      类别列列名
     * @return 训练好的类别Indexer模型
     * @throws IOException
     */
    public static StringIndexerModel trainIndexerModel(String modelSavePath, DataFrame trainDF, String classCol) throws IOException {
        StringIndexer indexer = new StringIndexer().setInputCol(classCol)
                .setOutputCol("indexedClass");
        StringIndexerModel indexerModel = indexer.fit(trainDF);

        indexerModel.write()
                .overwrite()
                .save(modelSavePath);
        System.out.println("saved indexer model to " + modelSavePath);

        return indexerModel;
    }
}
