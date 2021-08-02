package stream.task;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import stream.schema.RelationKafkaRecord;
import stream.schema.RelationKafkaSchema;
import stream.utils.HbasePutUtil;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HbaseRelationTableProcess {

    public static void main(String[] args) throws Exception {
        // bootstrapServers
        String bootstrapServers = "localhost:6667";
        // groupId
        String groupId = "ORDER_RELATION_ACCESS_KAFKA_20200226";
        // topicList
        String topics = "ORDER_RELATION_ACCESS_KAFKA";
        // windowTime
        String windowTime = "30";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.enableCheckpointing(1000*60*5);
        // kafka topic
        List<String> topicList = new ArrayList<>();
        for(String topic :topics.split(",")){
            topicList.add(topic);
        }
        // kafka props
        Properties props = getProperties(bootstrapServers,groupId);
        // kafka consumer
        FlinkKafkaConsumer<RelationKafkaRecord> consumer = new FlinkKafkaConsumer(topicList, new RelationKafkaSchema(), props);
        consumer.setStartFromGroupOffsets();
        // kafka connector source
        DataStreamSource<RelationKafkaRecord> sourceStream = env.addSource(consumer);
        sourceStream.filter(new FilterFunction<RelationKafkaRecord>() {
            @Override
            public boolean filter(RelationKafkaRecord relationKafkaRecord) throws Exception {
                JSONObject msg = JSONObject.parseObject(relationKafkaRecord.getMesg());
                String tradeId = msg.getString("TRADE_ID");
                String provinceCode = msg.getString("PROVINCE_CODE");

                if (tradeId != null && provinceCode != null) {
                    return true;
                } else {
                    System.out.println(LocalDateTime.now() + ",order relation data:" + relationKafkaRecord.toString());
                    return false;
                }
            }
        }).windowAll(TumblingEventTimeWindows.of(Time.seconds(Long.valueOf(windowTime))))
        .apply(new AllWindowFunction<RelationKafkaRecord, List<Put>, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<RelationKafkaRecord> iterable, Collector<List<Put>> collector) throws Exception {
                List<Put> puts = new ArrayList<>();
                for (RelationKafkaRecord record : iterable) {
                    JSONObject msg = JSONObject.parseObject(record.getMesg());
                    String tradeId = msg.getString("TRADE_ID");
                    String provinceCode = msg.getString("PROVINCE_CODE");
                    String isOuterOrder = msg.getString("IS_OUTER_ORDER");
                    String outerOrderId = msg.getString("OUTER_ORDER_ID");
                    String sysCode = msg.getString("SYS_CODE");

                    String shaHexRowKey = DigestUtils.shaHex(tradeId);
                    Put put = new Put(Bytes.toBytes(shaHexRowKey.substring(shaHexRowKey.length() - 3) + sysCode + shaHexRowKey));
                    put.addColumn(Bytes.toBytes("F1"), Bytes.toBytes("TRADE_ID"), Bytes.toBytes(tradeId));
                    put.addColumn(Bytes.toBytes("F1"), Bytes.toBytes("PROVINCE_CODE"), Bytes.toBytes(provinceCode));
                    put.addColumn(Bytes.toBytes("F1"), Bytes.toBytes("IS_OUTER_ORDER"), Bytes.toBytes(isOuterOrder));
                    put.addColumn(Bytes.toBytes("F1"), Bytes.toBytes("OUTER_ORDER_ID"), Bytes.toBytes(outerOrderId));
                    put.addColumn(Bytes.toBytes("F1"), Bytes.toBytes("SYS_CODE"), Bytes.toBytes(sysCode));
                    puts.add(put);
                }
                collector.collect(puts);
            }
        }).addSink(new SinkFunction<List<Put>>() {
            @Override
            public void invoke(List<Put> puts) throws Exception {
                //System.out.println(puts.toString());
                HbasePutUtil.put(puts);
            }
        });
        try {
            env.execute("HbaseRelationTableProcess");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Properties getProperties(String bootstrapServers,String groupId) throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
