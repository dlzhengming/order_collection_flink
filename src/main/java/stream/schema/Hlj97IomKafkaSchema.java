package stream.schema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

public class Hlj97IomKafkaSchema implements KafkaDeserializationSchema<Hlj97IomKafkaRecord> {

    @Override
    public boolean isEndOfStream(Hlj97IomKafkaRecord nextElement) {
        return false;
    }

    @Override
    public Hlj97IomKafkaRecord deserialize(ConsumerRecord<byte[], byte[]> record){
        return new Hlj97IomKafkaRecord(new String(record.value(), StandardCharsets.UTF_8),record.timestamp(),record.topic(),record.partition(),record.offset());
    }

    @Override
    public TypeInformation<Hlj97IomKafkaRecord> getProducedType() {
        return TypeInformation.of(Hlj97IomKafkaRecord.class);
    }
}
