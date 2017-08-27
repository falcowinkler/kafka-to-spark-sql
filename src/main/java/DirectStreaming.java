import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

public class DirectStreaming {
    public static void main(String[] args) throws InterruptedException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("JavaKafkaExperiment")
                .master("local")
                .enableHiveSupport()
                .config("spark.sql.hive.thriftServer.singleSession", true)
                .getOrCreate();

        Set<String> topicsSet = new HashSet<>(Arrays.asList("test"));
        SparkContext spark = sparkSession.sparkContext();
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark);
        SQLContext sqlContext = SQLContext.getOrCreate(spark);
        JavaStreamingContext jssc = new JavaStreamingContext(sparkContext, Durations.seconds(2));
        HiveThriftServer2.startWithContext(sqlContext);

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        JavaInputDStream<ConsumerRecord<String, String>> directKafkaStream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topicsSet, kafkaParams)
                );
        JavaDStream<String> json = directKafkaStream.map(
                (Function<ConsumerRecord<String, String>, String>)
                ConsumerRecord::value);
        json.foreachRDD(rdd -> {
            Dataset<Row> dataFrame = sqlContext.read().json(rdd);
            if (dataFrame.count() > 0 && !dataFrame.columns()[0].equals("_corrupt_record")) {
                try {
                    dataFrame.write().saveAsTable("reg_data_persistent");
                } catch (Exception  e) {

                    dataFrame.write().insertInto("reg_data_persistent");
                }
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
