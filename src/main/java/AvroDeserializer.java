import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class AvroDeserializer implements Deserializer<GenericRecord> {

    private static Injection<GenericRecord, byte[]> recordInjection;

    static {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = null;
        try {
            ClassLoader classLoader = DirectStreaming.class.getClassLoader();
            File file = new File(classLoader.getResource("TicketBoughtEvent.avsc").getFile());
            schema = parser.parse(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        recordInjection = GenericAvroCodecs.toBinary(schema);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public GenericRecord deserialize(String topic, byte[] data) {
        System.out.println("TOPIC: " + topic);
        return recordInjection.invert(data).get();
    }

    @Override
    public void close() {

    }
}