package stream.Test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class ZrrKafkaConsumer {
    public static void main(String[] args) throws Exception {
        System.out.println(System.getProperty("user.dir"));
        String topic = "STREAM_ORACLE_8802_CRM01";
        String groupId = "ZRR_KAFKA_PROCESS_191214";
        int partition = 4;
        //long offset = 0L;

        String client_ssl_dir = "./client-ssl/";
        //String client_ssl_dir = "/Users/zhengm/asiainfo/workspace/idea-wk/wk-asiainfo-order-dataoffload/DataOffload/order_collection_util/src/main/resources/client-ssl/";
        Properties props = new Properties();
        props.put("bootstrap.servers", "ZRR-PRODUCT-109:9062,ZRR-PRODUCT-110:9062,ZRR-PRODUCT-111:9062,ZRR-PRODUCT-112:9062,ZRR-PRODUCT-113:9062,ZRR-PRODUCT-114:9062,ZRR-PRODUCT-116:9062,ZRR-PRODUCT-117:9062");
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put("max.partition.fetch.bytes", 51200);
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        //props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("max.poll.records",1);
        props.put("ssl.key.password", "zrr@kafkacenter");
        props.put("ssl.keystore.password", "zrr@kafkacenter");
        props.put("ssl.truststore.location", client_ssl_dir+"kafkacenter_client.truststore.jks");
        props.put("ssl.keystore.location", client_ssl_dir+"kafkacenter_client.keystore.jks");
        props.put("ssl.key.location", client_ssl_dir+"kafkacenter_client.key");
        props.put("ssl.certificate.location", client_ssl_dir+"kafkacenter_client.pem");
        props.put("ssl.ca.location", client_ssl_dir+"ca-cert");
        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_SSL");
        props.put("ssl.keystore.type", "JKS");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"new_sale\" password=\"ns@hrb\";");
        KafkaConsumer consumer = new KafkaConsumer(props);
        TopicPartition partition0 = new TopicPartition(topic, partition);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        consumer.assign(Arrays.asList(partition0));
        //consumer.seek(partition0,offset);
        System.out.println("begin.....");
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(1000);
            try {
                for (ConsumerRecord<String, byte[]> record : records) {
                    System.out.printf("topic = %s,partition = %d, offset = %s, time = %s, value = %s\n",record.topic(),record.partition(),record.offset(),sdf.format(new Date(Long.valueOf(record.timestamp()))),record.value());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
