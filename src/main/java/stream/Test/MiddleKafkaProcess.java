package stream.Test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MiddleKafkaProcess {
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
        env.setStateBackend(new FsStateBackend("hdfs://nn02.hadoop.unicom:8020/flink/checkpoints/cBss-order-cnt/"));
        // env.setStateBackend(new FsStateBackend(new URI("file:/home/ocdp/app/flink-1.8.3/checkpoints/middle_kafka_process/"), 0));
        // kafka topic
        List<String> topicList = new ArrayList<>();
        topicList.add("PACKAGE_CHANGE");
        // kafka props
        Properties props = getProperties();
        // kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer(topicList, new SimpleStringSchema(), props);
        consumer.setStartFromGroupOffsets();
        // kafka connector source
        env.addSource(consumer).map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                System.out.println(s);
                return s;
            }
        }).print();

        try {
            env.execute("package-change-process");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Properties getProperties() {
        String groupId = "middle_kafka_process_191214";
        Properties props = new Properties();
        props.put("bootstrap.servers", "dn49.hadoop.unicom:6667,dn50.hadoop.unicom:6667,dn51.hadoop.unicom:6667,dn54.hadoop.unicom:6667,dn55.hadoop.unicom:6667,dn56.hadoop.unicom:6667");
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put("max.partition.fetch.bytes", 51200);
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
