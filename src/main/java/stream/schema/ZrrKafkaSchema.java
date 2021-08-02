package stream.schema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ZrrKafkaSchema implements KafkaDeserializationSchema<ZrrKafkaRecord> {

    @Override
    public boolean isEndOfStream(ZrrKafkaRecord nextElement) {
        return false;
    }

    @Override
    public ZrrKafkaRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        // public KafkaMsgDTO(byte[] mesg, long timestep, String topic, int partition, long offset)
        return new ZrrKafkaRecord(record.value(),record.timestamp(),record.topic(),record.partition(),record.offset());
    }

    @Override
    public TypeInformation<ZrrKafkaRecord> getProducedType() {
        return TypeInformation.of(ZrrKafkaRecord.class);
    }
}
