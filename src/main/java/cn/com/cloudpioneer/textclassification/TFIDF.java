package cn.com.cloudpioneer.textclassification;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * @author lsx
 */
public class TFIDF {
    private static Set<String> wordSet = new HashSet<>();
    private static Set<String> idSet = new HashSet<>();

    /**
     * 计算所有文档所有词的TF-IDF值
     *
     * @param sqlContext
     * @param inputDF    输入数据DataFrame
     * @param idCol      文档ID列列名
     * @param wordsCol   分词列列名
     * @return
     */
    public static DataFrame getTFIDF(SQLContext sqlContext, DataFrame inputDF, String idCol, String wordsCol) {
        JavaRDD<Row> rdd = inputDF.select(idCol, wordsCol)
                .toJavaRDD()
                .flatMapToPair(new PairFlatMapFunction<Row, String, Row>() {
                    @Override
                    public Iterable<Tuple2<String, Row>> call(Row row) throws Exception {
                        String id = row.getString(0);
                        idSet.add(id);
                        String[] words = row.getString(1).split(" ");
                        List<Tuple2<String, Row>> result = new ArrayList<>();
                        for (String word : words) {
                            wordSet.add(word);
                            result.add(new Tuple2<String, Row>(word, RowFactory.create(id)));
                        }
                        return result;
                    }
                })
                .groupByKey()
                .flatMapValues(new Function<Iterable<Row>, Iterable<Row>>() {
                    @Override
                    public Iterable<Row> call(Iterable<Row> strings) throws Exception {
                        Iterator<Row> iter = strings.iterator();
                        Map<String, Integer> map = new HashMap<>();
                        while (iter.hasNext()) {
                            String curId = iter.next().getString(0);
                            if (map.containsKey(curId)) {
                                map.put(curId, map.get(curId) + 1);
                            } else {
                                map.put(curId, 1);
                            }
                        }
                        List<Row> result = new ArrayList<>();
                        Integer idCount = map.keySet().size();
                        for (Map.Entry<String, Integer> entry : map.entrySet()) {
                            result.add(
                                    //id
                                    RowFactory.create(entry.getKey(),
                                            //wordCount
                                            entry.getValue(),
                                            //idCount
                                            idCount));
                        }
                        return result;
                    }
                })
                .map(new Function<Tuple2<String, Row>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Row> stringRowTuple2) throws Exception {
                        double tfidf = calTFIDF(stringRowTuple2._2().getInt(1),
                                wordSet.size(),
                                stringRowTuple2._2().getInt(2),
                                idSet.size());
                        return RowFactory.create(stringRowTuple2._2().getString(0),
                                stringRowTuple2._1(),
                                tfidf);
                    }
                });

        StructType schema = new StructType(new StructField[]{
                new StructField(idCol, DataTypes.StringType, false, Metadata.empty()),
                new StructField(wordsCol, DataTypes.StringType, false, Metadata.empty()),
                new StructField("tfidf", DataTypes.DoubleType, false, Metadata.empty())
        });

        DataFrame result = sqlContext.createDataFrame(rdd, schema);
        return result;
    }

    /**
     * 计算TF-IDF值
     *
     * @param wordCount 词频
     * @param totalWord 总词频
     * @param idCount   包含该词的文档数
     * @param totalId   总文档数
     * @return
     */
    private static double calTFIDF(int wordCount, int totalWord, int idCount, int totalId) {
        double tf = (double) wordCount / totalWord;
        double idf = Math.log((double) totalId / idCount + 0.01);
        return tf * idf;
    }
}
