package cn.com.cloudpioneer.textclassification;

import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * @author lsx
 */
public class NaiveBayesUtil {
    public static DataFrame runNaiveBayes(SQLContext sqlContext, DataFrame testDF) {

        DataFrame trainData = sqlContext.read()
                .parquet("/Users/lsx/Desktop/TextClassification/removed.parquet")
                .filter("class!='汽车'");
        DataFrame testData = testDF;


        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("segmentedWords")
                .setOutputCol("words");
        trainData = tokenizer.transform(trainData);
//        testData = tokenizer.transform(testData);

        int numFeatures = 1000000;
        HashingTF hashingTF = new HashingTF()
                .setInputCol("words")
                .setOutputCol("tf")
                .setNumFeatures(numFeatures);
        trainData = hashingTF.transform(trainData);
        testData = hashingTF.transform(testData);

        IDF idf = new IDF().setInputCol("tf")
                .setOutputCol("tfidf");
        IDFModel idfModel = idf.fit(trainData);
        trainData = idfModel.transform(trainData);
        testData = idfModel.transform(testData);

        StringIndexerModel stringIndexerModel = new StringIndexer()
                .setInputCol("class")
                .setOutputCol("indexedClass")
                .fit(trainData);
        trainData = stringIndexerModel.transform(trainData);

        NaiveBayes naiveBayes = new NaiveBayes()
                .setFeaturesCol("tfidf")
                .setPredictionCol("prediction_nb")
                .setLabelCol("indexedClass");
        NaiveBayesModel naiveBayesModel = naiveBayes.fit(trainData);
        DataFrame result = naiveBayesModel.transform(testData);

        IndexToString indexToString = new IndexToString()
                .setLabels(stringIndexerModel.labels())
                .setInputCol("prediction_nb")
                .setOutputCol("nbClass");
        result = indexToString.transform(result);

        return result;
    }
}
