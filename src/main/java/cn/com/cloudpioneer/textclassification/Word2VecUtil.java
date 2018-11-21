package cn.com.cloudpioneer.textclassification;

import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.DataFrame;

import java.io.IOException;

/**
 * @author lsx
 */
public class Word2VecUtil {
    /**
     * 训练word2vec模型，并将模型保存，模型的outputCol为"wordVec"
     *
     * @param modelSavePath 模型存储路径
     * @param trainDF       训练集DataFrame
     * @param wordsCol      分词列列名
     * @return 训练好的Word2vec模型
     * @throws IOException
     */
    public static Word2VecModel trainWord2Vec(String modelSavePath, DataFrame trainDF, String wordsCol) throws IOException {
        // 按空格分割词
        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol(wordsCol)
                .setOutputCol("words")
                .setPattern(" ");
        trainDF = regexTokenizer.transform(trainDF);

        /*
        用训练集训练word2vec模型，并保存
         */
        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("words")
                .setOutputCol("wordVec")
                .setVectorSize(200)
                .setMinCount(1);
        Word2VecModel word2VecModel = word2Vec.fit(trainDF);
        word2VecModel.write().overwrite().save(modelSavePath);

        System.out.println("saved word2vec model to " + modelSavePath);

        return word2VecModel;
    }

    /**
     * 训练word2vec模型，并将模型保存，生成文档的word2vec向量列
     *
     * @param modelSavePath 模型存储路径
     * @param trainDF       训练集DataFrame
     * @param testDF        测试集DataFrame
     * @param wordsCol      分词列列名
     * @return 训练集和测试集的word2vec转换结果
     * @throws IOException
     */
    public static DataFrame[] runWord2Vec(String modelSavePath, DataFrame trainDF, DataFrame testDF, String wordsCol) throws IOException {
        // 按空格分割词
        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol(wordsCol)
                .setOutputCol("words")
                .setPattern(" ");
        DataFrame result = regexTokenizer.transform(testDF);
        trainDF = regexTokenizer.transform(trainDF);

        /*
        用训练集训练word2vec模型，并保存
         */
        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("words")
                .setOutputCol("wordVec")
                .setVectorSize(200)
                .setMinCount(1);
        Word2VecModel word2VecModel = word2Vec.fit(trainDF);
        word2VecModel.write().overwrite().save(modelSavePath);

        /*
        用word2vec模型将训练集和测试集文档转换为word2vec向量
         */
        result = word2VecModel.transform(result);
        trainDF = word2VecModel.transform(trainDF);

        return new DataFrame[]{trainDF, result};
    }

    /**
     * 读取保存的word2vec模型来转换testDF
     *
     * @param modelLoadPath 模型路径
     * @param testDF        待转换数据DataFrame
     * @param wordsCol      分词列列名
     * @return 增加word2vec转换结果新列
     * @throws IOException
     */
    public static DataFrame runWord2Vec(String modelLoadPath, DataFrame testDF, String wordsCol) throws IOException {
        DataFrame result = PreProcessUtil.runTokenizer(testDF, wordsCol, "words");
        Word2VecModel word2VecModel = Word2VecModel.load(modelLoadPath);
        System.out.println("loaded word2vec model from " + modelLoadPath + ", vectorSize: " + word2VecModel.getVectorSize() + ", windowSize: " + word2VecModel.getWindowSize());
        result = word2VecModel.transform(result);
        return result;
    }

    /**
     * 训练word2vec模型，生成文档的word2vec向量列
     *
     * @param trainDF  训练集DataFrame
     * @param testDF   测试集DataFrame
     * @param wordsCol 分词列列名
     * @return 训练集和测试集的word2vec转换结果
     * @throws IOException
     */
    public static DataFrame[] runWord2Vec(DataFrame trainDF, DataFrame testDF, String wordsCol) {
        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol(wordsCol)
                .setOutputCol("words")
                .setPattern(" ");
        DataFrame result = regexTokenizer.transform(testDF);
        trainDF = regexTokenizer.transform(trainDF);

        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("words")
                .setOutputCol("wordVec")
                .setVectorSize(200)
                .setMinCount(1);
        Word2VecModel word2VecModel = word2Vec.fit(trainDF);

        result = word2VecModel.transform(result);
        trainDF = word2VecModel.transform(trainDF);

        return new DataFrame[]{trainDF, result};
    }
}
