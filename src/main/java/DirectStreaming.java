import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import kafka.serializer.DefaultDecoder;

import java.io.File;
import java.io.IOException;
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

        Set<String> jsonTopicsSet = new HashSet<>(Arrays.asList(
                "customer_registration",
                "age_verification",
                "SCHUFA_AGE_VERIFICATION",
                "alinghi_basic_product_purchased",
                "alinghi_voucher_used",
                "iwg_ticket_bought",
                "alinghi_iwg_ticket_bought",
                "alinghi_iwg_tracking",
                "purchase_status")
        );

        Set<String> avroTopicsSet = new HashSet<>(Arrays.asList(
                "iwg_ticket_bought"
        ));

        SparkContext spark = sparkSession.sparkContext();
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark);
        SQLContext sqlContext = SQLContext.getOrCreate(spark);
        JavaStreamingContext jssc = new JavaStreamingContext(sparkContext, Durations.seconds(2));
        HiveThriftServer2.startWithContext(sqlContext);

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "app-02.apps.ham.sg-cloud.co.uk:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", AvroDeserializer.class);
        kafkaParams.put("group.id", "kafka-spark-tableu-pipeline");
        kafkaParams.put("auto.offset.reset", "latest");

        JavaInputDStream<ConsumerRecord<String, GenericRecord>> directKafkaAVROStream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, GenericRecord>Subscribe(avroTopicsSet, kafkaParams)
                );

        directKafkaAVROStream.foreachRDD(genericRecord -> {
                    genericRecord.foreach(record -> {
                        record.value().get(0);
                        //record.getSchema().getField("").name();
                        System.out.println("str1= " + record.value().get(0));
                    });
            });


        List<JavaInputDStream<ConsumerRecord<String, String>>> topicStreams = new ArrayList<>();

        jsonTopicsSet.forEach(topic -> {
            JavaInputDStream<ConsumerRecord<String, String>> directKafkaJSONStream =
                    KafkaUtils.createDirectStream(
                            jssc,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.<String, String>Subscribe((Arrays.asList(
                                    topic
                            )), kafkaParams)
                    );
            topicStreams.add(directKafkaJSONStream);
        });

        topicStreams.forEach(topicStream -> {
            topicStream.foreachRDD(rdd -> {
                    Dataset<Row> dataFrame = sqlContext.read().json(rdd.map(ConsumerRecord::value));
                    if (rdd.map(ConsumerRecord::topic).count() > 0) {
                        if (dataFrame.count() > 0 && !dataFrame.columns()[0].equals("_corrupt_record")) {
                            String topic = rdd.map(ConsumerRecord::topic).first();
                            try {
                                dataFrame.write().saveAsTable(topic);
                            } catch (Exception e) {
                                try {
                                    dataFrame.write().insertInto(topic);
                                } catch (Exception analysisExeption) {
                                    // Format was invalid
                                }
                            }
                        } else {
                            System.out.println(rdd.map(ConsumerRecord::topic).first());
                            System.out.println(rdd.map(ConsumerRecord::value).first());
                        }
                    }
            });
        });


        jssc.start();
        jssc.awaitTermination();
    }
}
