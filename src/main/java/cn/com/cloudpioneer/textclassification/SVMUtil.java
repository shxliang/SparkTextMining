package cn.com.cloudpioneer.textclassification;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @author lsx
 */
public class SVMUtil {
    public static DataFrame runSVM(SQLContext sqlContext, DataFrame trainDF, DataFrame testDF, String idCol, String featureCol, String labelCol) {
        JavaRDD<LabeledPoint> trainRDD = trainDF.select(featureCol, labelCol)
                .toJavaRDD()
                .map(new Function<Row, LabeledPoint>() {
                    @Override
                    public LabeledPoint call(Row row) throws Exception {
                        return new LabeledPoint(row.getDouble(1), (Vector) row.get(0));
                    }
                });
        final SVMModel svmModel = SVMWithSGD.train(trainRDD.rdd(), 200);

        JavaRDD<Row> resultRDD = testDF.select(idCol, featureCol)
                .toJavaRDD()
                .map(new Function<Row, Row>() {
                    @Override
                    public Row call(Row row) throws Exception {
                        double prediction = svmModel.predict((Vector) row.get(1));
                        return RowFactory.create(row.getString(0), prediction);
                    }
                });
        StructType resultSchema = new StructType(new StructField[]{
                new StructField(idCol, DataTypes.StringType, false, Metadata.empty()),
                new StructField("prediction", DataTypes.DoubleType, false, Metadata.empty())
        });
        DataFrame resultDF = sqlContext.createDataFrame(resultRDD, resultSchema);
        return resultDF;
    }
}
