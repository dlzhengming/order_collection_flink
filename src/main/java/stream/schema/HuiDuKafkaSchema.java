package stream.schema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class HuiDuKafkaSchema implements KafkaDeserializationSchema<HuiDuKafkaRecord> {

    @Override
    public boolean isEndOfStream(HuiDuKafkaRecord nextElement) {
        return false;
    }

    @Override
    public HuiDuKafkaRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        return new HuiDuKafkaRecord(new String(record.value()),record.timestamp(),record.topic(),record.partition(),record.offset());
    }

    @Override
    public TypeInformation<HuiDuKafkaRecord> getProducedType() {
        return TypeInformation.of(HuiDuKafkaRecord.class);
    }
}
