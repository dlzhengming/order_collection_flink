package stream.Test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import stream.schema.ZrrKafkaRecord;
import stream.schema.ZrrKafkaSchema;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ZrrKafkaProcess {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //* 确保检查点之间至少有500ms的间隔【checkpoint的最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //* 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //* 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以变根据时间需要恢复到指定的Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // State数据保存在taskmanager的内存中，执行checkpoint的时候，会把state的快照数据保存到配置的文件系统
        // env.setStateBackend(new FsStateBackend("hdfs://nn02.hadoop.unicom:8020/flink/checkpoints/zrr_kafka_process/"));
        // env.setStateBackend(new FsStateBackend(new URI("file:/home/ocdp/app/flink-1.8.3/checkpoints/zrr_kafka_process/"), 0));
        env.setStateBackend(new FsStateBackend(new URI("file:/tmp/checkpoints/zrr_kafka_process/"), 0));
        // kafka topic
        List<String> topicList = new ArrayList<>();
        topicList.add("STREAM_ORACLE_8802_CRM01");
        // kafka props
        Properties props = getProperties();
        // kafka consumer
        FlinkKafkaConsumer<ZrrKafkaRecord> consumer = new FlinkKafkaConsumer(topicList, new ZrrKafkaSchema(), props);
        consumer.setStartFromGroupOffsets();
        // kafka connector source
        env.addSource(consumer).map(new MapFunction<ZrrKafkaRecord, String>() {
            @Override
            public String map(ZrrKafkaRecord zrrKafkaRecord) throws Exception {
                System.out.println(zrrKafkaRecord.toString());
                return zrrKafkaRecord.toString();
            }
        });

        try {
            env.execute("zrr-kafka-process");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Properties getProperties() throws Exception {

        Configuration conf = new Configuration();
        //String hdfsUrl = "hdfs://dn171.hadoop.unicom:8020/";
        String hdfsUrl = "hdfs://dn171.hadoop.unicom:8020/";
        conf.set("fs.default.name", hdfsUrl);

        String groupId = "zrr_kafka_process_191217";
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
        //props.put("max.poll.records",1);
        props.put("ssl.key.password", "zrr@kafkacenter");
        props.put("ssl.keystore.password", "zrr@kafkacenter");

        FileSystem fs = FileSystem.get(new URI("hdfs://dn171.hadoop.unicom:8020"),new Configuration(),"hdfs");
        fs.copyToLocalFile(false,new Path("/client-ssl/kafkacenter_client.truststore.jks"),new Path("/tmp/client-ssl/kafkacenter_client.truststore.jks"),true);
        fs.copyToLocalFile(false,new Path("/client-ssl/kafkacenter_client.keystore.jks"),new Path("/tmp/client-ssl/kafkacenter_client.keystore.jks"),true);
        fs.copyToLocalFile(false,new Path("/client-ssl/kafkacenter_client.key"),new Path("/tmp/client-ssl/kafkacenter_client.key"),true);
        fs.copyToLocalFile(false,new Path("/client-ssl/kafkacenter_client.pem"),new Path("/tmp/client-ssl/kafkacenter_client.pem"),true);
        fs.copyToLocalFile(false,new Path("/client-ssl/ca-cert"),new Path("/tmp/client-ssl/ca-cert"),true);

        String client_ssl_dir = "/tmp/client-ssl/";
        props.put("ssl.truststore.location", client_ssl_dir+"kafkacenter_client.truststore.jks");
        props.put("ssl.keystore.location", client_ssl_dir+"kafkacenter_client.keystore.jks");
        props.put("ssl.key.location", client_ssl_dir+"kafkacenter_client.key");
        props.put("ssl.certificate.location", client_ssl_dir+"kafkacenter_client.pem");
        props.put("ssl.ca.location", client_ssl_dir+"ca-cert");

        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_SSL");
        props.put("ssl.keystore.type", "JKS");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"new_sale\" password=\"ns@hrb\";");
        return props;
    }
}
