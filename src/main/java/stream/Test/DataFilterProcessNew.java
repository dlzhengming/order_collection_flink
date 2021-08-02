package stream.Test;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import stream.schema.HuiDuKafkaRecord;
import stream.schema.HuiDuKafkaSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 数据核对
 * */
public class DataFilterProcessNew {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000*60*5);
        // kafka topic
        List<String> topicList = new ArrayList<>();
        topicList.add("ES_CB_STANDARD");
        // kafka props
        Properties props = getProperties("DataFilterProcessStandard");
        // kafka consumer
        FlinkKafkaConsumer<HuiDuKafkaRecord> consumer = new FlinkKafkaConsumer(topicList, new HuiDuKafkaSchema(), props);
        consumer.setStartFromGroupOffsets();
        // kafka connector source
        DataStreamSource<HuiDuKafkaRecord> sourceStream = env.addSource(consumer);
        SingleOutputStreamOperator<HuiDuKafkaRecord> filterStream = sourceStream.filter(new FilterFunction<HuiDuKafkaRecord>() {
            @Override
            public boolean filter(HuiDuKafkaRecord record) throws Exception {
                try{
                    JSONObject data = JSONObject.parseObject(record.getMesg());
                    String operationType__ = data.getString("OPERATION_TYPE__");
                    if("I".equals(operationType__)){
                        JSONObject tableData = JSONObject.parseObject(data.getString("DATA__"));
                        String orderTime = tableData.getString("ORDER_TIME");
                        String tradeTypeCode = tableData.getString("TRADE_TYPE_CODE");
                        String netTypeCode = tableData.getString("NET_TYPE_CODE");
                        String provinceCode = tableData.getString("PROVINCE_CODE");
                        if("11".equals(provinceCode) && orderTime.contains("2020-02-10") && ("100".equals(tradeTypeCode) || "200".equals(tradeTypeCode) || "300".equals(tradeTypeCode)) && ("40".equals(netTypeCode) || "50".equals(netTypeCode) || "CP".equals(netTypeCode))){
                            return true;
                        }else{
                            return false;
                        }
                    }else{
                        return false;
                    }
                }catch (Exception e){
                    System.out.println(record.getMesg());
                    return false;
                }
            }
        }).name("myFilter");
        SingleOutputStreamOperator<String> tableData = filterStream.map(new MapFunction<HuiDuKafkaRecord, String>() {
            @Override
            public String map(HuiDuKafkaRecord huiDuKafkaRecord) throws Exception {
                JSONObject data = JSONObject.parseObject(huiDuKafkaRecord.getMesg());
                return data.getString("DATA__");
            }
        }).name("myMap");

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>("logcal:6667", "ES_CB_STANDARD_NEW_11_20200110", new SimpleStringSchema());
        tableData.addSink(myProducer).name("mySink");

        try {
            env.execute("DataFilterProcess");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Properties getProperties(String groupName) throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", "logcal:6667");
        props.put("group.id", groupName);
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
