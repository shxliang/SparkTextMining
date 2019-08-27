package com.shxliang.classification;

import com.shxliang.utils.MapUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lsx
 */
public class ClassTemplateClassifier {
    public static void main(String[] args) {
        SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("test");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        DataFrame classTemplate = sqlContext.read()
                .parquet("/Users/lsx/Desktop/TextClassification/ClassTemplate.parquet");
        final Map<String, List<Tuple2<String, Double>>> templateMap = ClassTemplateGenerator.getWordMap(classTemplate);

        DataFrame data = sqlContext.read()
                .parquet("/Users/lsx/Desktop/TextClassification/removed.parquet");

//        data.select("docId","segmentedWords")
//                .toJavaRDD()
//                .flatMapToPair(new PairFlatMapFunction<Row, String, Row>() {
//                    @Override
//                    public Iterable<Tuple2<String, Row>> call(Row row) throws Exception {
//                        List<Tuple2<String, Row>> result = new ArrayList<>();
//                        String[] words = row.getString(1).split(" ");
//                        for (String word : words)
//                        {
//                            result.add(new Tuple2<String, Row>(row.getString(0),
//                                    RowFactory.create(word)));
//                        }
//                        return result;
//                    }
//                })
//                .flatMapValues(new Function<Row, Iterable<Row>>() {
//                    @Override
//                    public Iterable<Row> call(Row row) throws Exception {
//                        String word = row.getString(0);
//                        List<Row> result = new ArrayList<>();
//                        if (templateMap.containsKey(word))
//                        {
//                            List<Tuple2<String,Double>> tuple2List = templateMap.get(word);
//                            for (Tuple2<String,Double> tuple2 : tuple2List)
//                                result.add(RowFactory.create(tuple2._1,tuple2._2));
//                        }
//                        return result;
//                    }
//                })
//                .groupByKey();

        sqlContext.udf().register("templateClassifier", new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception {
                String[] words = s.split(" ");
                Map<String, Double> result = new HashMap<>();
                for (String word : words) {
                    if (templateMap.containsKey(word)) {
                        List<Tuple2<String, Double>> curClass = templateMap.get(word);
                        for (Tuple2<String, Double> curTuple : curClass) {
                            String c = curTuple._1;
                            Double w = curTuple._2;
                            if (result.containsKey(c)) {
                                result.put(c, result.get(c) + w);
                            } else {
                                result.put(c, w);
                            }
                        }
                    }
                }
                if (result.size() < 1) {
                    return "Unknown";
                }
                MapUtil.sortByValue(result);
                return result.keySet().iterator().next();
            }
        }, DataTypes.StringType);


        data = data.withColumn("docClass", functions.callUDF("templateClassifier", functions.col("segmentedWords")));

        System.out.println(data.filter("class=docClass").count() / (double) data.filter("docClass!='Unknown'").count());
    }
}
