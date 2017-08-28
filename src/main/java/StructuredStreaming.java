import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.TimerTask;

public class StructuredStreaming {
    public static void main(String[] args) throws StreamingQueryException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaKafkaExperiment")
                .master("local")
                .enableHiveSupport()
                .config("spark.sql.hive.thriftServer.singleSession", true)
                .getOrCreate();

        Dataset<String> lines = spark
                .readStream()
                .format("com.databricks.spark.avro")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test")
                .load()

                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());



        SQLContext context = new SQLContext(spark);
        HiveThriftServer2.startWithContext(context);

        // Generate running word count
        Dataset<Row> wordCounts = lines.flatMap(
                (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
                Encoders.STRING()).groupBy("value").count();


        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();
        StreamingQuery dbqueary = wordCounts.writeStream()
                .queryName("registrations")    // this query name will be the table name
                .outputMode("complete")
                .format("memory")
                .start();

        java.util.Timer t = new java.util.Timer();
        t.schedule(new TimerTask() {

            @Override
            public void run() {
                context.sql("INSERT OVERWRITE TABLE registrations_hive select * from registrations");
            }
        }, 5000, 5000);

        dbqueary.awaitTermination();
        query.awaitTermination();

    }
}
