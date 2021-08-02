package stream.schema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RelationKafkaSchema implements KafkaDeserializationSchema<RelationKafkaRecord> {

    @Override
    public boolean isEndOfStream(RelationKafkaRecord nextElement) {
        return false;
    }

    @Override
    public RelationKafkaRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        return new RelationKafkaRecord(new String(record.value()),record.timestamp(),record.topic(),record.partition(),record.offset());
    }

    @Override
    public TypeInformation<RelationKafkaRecord> getProducedType() {
        return TypeInformation.of(RelationKafkaRecord.class);
    }
}
