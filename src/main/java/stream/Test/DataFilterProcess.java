package stream.Test;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
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
public class DataFilterProcess {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String filterDay = params.get("filterDay");
        String outTopic = params.get("outTopic");
        String groupName = params.get("groupName");
        System.out.println("filterDay:"+filterDay);
        System.out.println("outTopic:"+outTopic);
        System.out.println("outTopic:"+groupName);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.enableCheckpointing(1000*60*10);
        // kafka topic
        List<String> topicList = new ArrayList<>();
        topicList.add("ES_CB_SPARK");
        // kafka props
        Properties props = getProperties(groupName);
        // kafka consumer
        FlinkKafkaConsumer<HuiDuKafkaRecord> consumer = new FlinkKafkaConsumer(topicList, new HuiDuKafkaSchema(), props);
        consumer.setStartFromGroupOffsets();
        // kafka connector source
        DataStreamSource<HuiDuKafkaRecord> sourceStream = env.addSource(consumer);
        sourceStream.print();
        SingleOutputStreamOperator<HuiDuKafkaRecord> filterStream = sourceStream.filter(new FilterFunction<HuiDuKafkaRecord>() {
            @Override
            public boolean filter(HuiDuKafkaRecord record) throws Exception {
                System.out.println("myFilter:"+record.getMesg());
                JSONObject data = JSONObject.parseObject(record.getMesg());
                String ACTION = data.getString("ACTION");
                JSONObject tableData = JSONObject.parseObject(data.getString("TABLE_DATA"));
                String orderTime = tableData.getString("ORDER_TIME");

                if(filterDay != null && "0".equals(ACTION) && orderTime.contains(filterDay)){
                    return true;
                }else if(filterDay == null && "0".equals(ACTION)){
                    return true;
                }else{
                    return false;
                }
            }
        }).name("myFilter");
        SingleOutputStreamOperator<String> tableData = filterStream.map(new MapFunction<HuiDuKafkaRecord, String>() {
            @Override
            public String map(HuiDuKafkaRecord huiDuKafkaRecord) throws Exception {
                JSONObject data = JSONObject.parseObject(huiDuKafkaRecord.getMesg());
                JSONObject tableData = JSONObject.parseObject(data.getString("TABLE_DATA"));
                System.out.println("myMap:"+tableData.toString());
                return tableData.toString();
            }
        }).name("myMap");

        //if(filterDay != null){
        //    tableData.writeAsText("/tmp/data_check/"+filterDay+"/"+Thread.currentThread().getId(), FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("mySink");
        //}else{
        //    tableData.writeAsText("/tmp/data_check/2020MMdd/"+Thread.currentThread().getId(), FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("mySink");
        //}

        if(outTopic == null){
            outTopic = "ES_CB_SPARK_OUT";
        }
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>("localhost:6667", outTopic, new SimpleStringSchema());
        tableData.addSink(myProducer).name("sink tableData kafka");

        try {
            env.execute("DataFilterProcess");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Properties getProperties(String groupName) throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:6667");
        if(groupName == null){
            groupName = "DataFilterProcess_"+System.currentTimeMillis();
            props.put("group.id", groupName);
            System.out.println("groupName:"+groupName);
        }else{
            props.put("group.id", groupName);
        }
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
