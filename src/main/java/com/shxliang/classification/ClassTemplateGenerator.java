package com.shxliang.classification;

import com.shxliang.utils.MapUtil;
import com.shxliang.utils.StringUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * 提取每个类别下特征词模板，按类别的TFIDF值排序
 *
 * @author lsx
 */
public class ClassTemplateGenerator {
    public static void main(String[] args) {
        SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("test");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        DataFrame inpuDF = sqlContext.read()
                .parquet("/Users/lsx/Desktop/TextClassification/removed.parquet");
        DataFrame tfidf = TFIDF.getTFIDF(sqlContext, inpuDF, "class", "segmentedWords");
        JavaRDD<Row> rdd = tfidf.select("class", "segmentedWords", "tfidf")
                .toJavaRDD()
                .mapToPair(new PairFunction<Row, String, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Tuple2<String, Double>> call(Row row) throws Exception {
                        return new Tuple2<>(row.getString(0),
                                new Tuple2<String, Double>(row.getString(1), row.getDouble(2)));
                    }
                })
                .groupByKey()
                .flatMapValues(new Function<Iterable<Tuple2<String, Double>>, Iterable<Tuple2<String, Double>>>() {
                    @Override
                    public Iterable<Tuple2<String, Double>> call(Iterable<Tuple2<String, Double>> tuple2s) throws Exception {
                        Map<String, Double> map = new HashMap<>();
                        Iterator<Tuple2<String, Double>> iter = tuple2s.iterator();
                        while (iter.hasNext()) {
                            Tuple2<String, Double> curTup = iter.next();
                            map.put(curTup._1(), curTup._2());
                        }
                        map = MapUtil.sortByValue(map);
                        int count = 0;
                        List<Tuple2<String, Double>> tempResult = new ArrayList<>();
                        double sum = 0;
                        for (Map.Entry<String, Double> entry : map.entrySet()) {
                            if (count >= 500) {
                                break;
                            }
                            if (StringUtil.isContainChinese(entry.getKey())) {
                                tempResult.add(new Tuple2<String, Double>(entry.getKey(), entry.getValue()));
                                sum += entry.getValue();
                            }
                            count++;
                        }
                        List<Tuple2<String, Double>> result = new ArrayList<>();
                        for (Tuple2<String, Double> tuple2 : tempResult) {
                            result.add(new Tuple2<String, Double>(tuple2._1, tuple2._2 / sum));
                        }
                        return result;
                    }
                })
                .map(new Function<Tuple2<String, Tuple2<String, Double>>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Tuple2<String, Double>> stringTuple2Tuple2) throws Exception {
                        return RowFactory.create(stringTuple2Tuple2._1(),
                                stringTuple2Tuple2._2()._1(),
                                stringTuple2Tuple2._2()._2());
                    }
                });
        StructType schema = new StructType(new StructField[]{
                new StructField("class", DataTypes.StringType, false, Metadata.empty()),
                new StructField("word", DataTypes.StringType, false, Metadata.empty()),
                new StructField("tfidf", DataTypes.DoubleType, false, Metadata.empty())
        });
        DataFrame dataFrame = sqlContext.createDataFrame(rdd, schema);
//        dataFrame.filter("class='旅游'").sort(dataFrame.col("tfidf").desc()).show(500);

        dataFrame.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet("/Users/lsx/Desktop/TextClassification/ClassTemplate.parquet");
    }

    public static Map<String, List<Tuple2<String, Double>>> getWordMap(DataFrame classTemplate) {
        return classTemplate.toJavaRDD()
                .mapToPair(new PairFunction<Row, String, List<Tuple2<String, Double>>>() {
                    @Override
                    public Tuple2<String, List<Tuple2<String, Double>>> call(Row row) throws Exception {
                        List<Tuple2<String, Double>> value = new ArrayList<>();
                        value.add(new Tuple2<String, Double>(row.getString(0), row.getDouble(2)));
                        return new Tuple2<>(row.getString(1), value);
                    }
                })
                .reduceByKey(new Function2<List<Tuple2<String, Double>>, List<Tuple2<String, Double>>, List<Tuple2<String, Double>>>() {
                    @Override
                    public List<Tuple2<String, Double>> call(List<Tuple2<String, Double>> tuple2s, List<Tuple2<String, Double>> tuple2s2) throws Exception {
                        tuple2s.addAll(tuple2s2);
                        return tuple2s;
                    }
                })
                .collectAsMap();
    }
}
