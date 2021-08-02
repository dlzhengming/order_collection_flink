package stream.task;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import stream.Zrr.ZrrFlinkKafkaConsumer;
import stream.schema.Hlj97IomKafkaRecord;
import stream.schema.Hlj97IomKafkaSchema;
import stream.schema.StreamsEnum;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * KanbanCenter_DS_97_Topic 0-4
 * */
public class Hlj97IomOrderMsgCntProcess {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(5);
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.enableCheckpointing(50000);
        env.registerCachedFile("/home/unicomhdp/app/flink-1.8.3/client-ssl-two/kafkacenter_client.truststore.jks","truststore");
        env.registerCachedFile("/home/unicomhdp/app/flink-1.8.3/client-ssl-two/kafkacenter_client.keystore.jks","keystore");
        //env.registerCachedFile("/Users/zhengm/client-ssl-two/kafkacenter_client.truststore.jks","truststore");
        //env.registerCachedFile("/Users/zhengm/client-ssl-two/kafkacenter_client.keystore.jks","keystore");
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //* 确保检查点之间至少有500ms的间隔【checkpoint的最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //* 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //* 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以变根据时间需要恢复到指定的Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // State数据保存在taskmanager的内存中，执行checkpoint的时候，会把state的快照数据保存到配置的文件系统
        env.setStateBackend(new FsStateBackend("hdfs://dn171.hadoop.unicom:8020/flink/checkpoints/hlj97_iom_order_msg_cnt/"));
        // kafka topic
        List<String> topicList = new ArrayList<>();
        topicList.add("KanbanCenter_DS_97_Topic");
        // kafka props
        Properties props = getProperties();
        // kafka consumer
        ZrrFlinkKafkaConsumer<Hlj97IomKafkaRecord> consumer = new ZrrFlinkKafkaConsumer(topicList, new Hlj97IomKafkaSchema(), props);
        consumer.setStartFromGroupOffsets();
        // kafka connector source
        DataStreamSource<Hlj97IomKafkaRecord> hlj97IomDs = env.addSource(consumer);
        SingleOutputStreamOperator<Tuple3<String, String, Long>> mapDataStream = hlj97IomDs.map(new MapFunction<Hlj97IomKafkaRecord, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(Hlj97IomKafkaRecord hlj97IomKafkaRecord) {
                DateTimeFormatter ftf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                String dt = ftf.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(hlj97IomKafkaRecord.getTimestep()), ZoneId.of("Asia/Shanghai")));
                JSONObject data = JSONObject.parseObject(hlj97IomKafkaRecord.getMesg());
                String tableName = String.valueOf(data.get("TABLE_NAME"));
                //System.out.println(hlj97IomKafkaRecord.toString());
                // DAY,TABLE_NAME,1L
                return new Tuple3(dt, tableName, 1L);
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Long>> apply = mapDataStream.keyBy(0, 1)
                .timeWindow(Time.minutes(30))
                .apply(new RichWindowFunction<Tuple3<String, String, Long>, Tuple2<String, Long>, Tuple, TimeWindow>() {

                    // day+tableName,sum
                    private transient MapState<String, Long> tableDayCnt;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<String, Long>(
                                "table-day-cnt", // 状态名称
                                TypeInformation.of(String.class),
                                TypeInformation.of(Long.class)
                        );
                        tableDayCnt = getRuntimeContext().getMapState(descriptor);
                    }

                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<String, String, Long>> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String tableName = "";
                        String currentDay = "";
                        String currentKey = "";
                        Long sum = 0L;

                        // DAY,TABLE_NAME,1L
                        for (Tuple3<String, String, Long> element : iterable) {
                            currentDay = element.f0;
                            tableName = element.f1;
                            currentKey = currentDay + tableName;
                            sum += element.f2;
                        }
                        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        LocalDateTime currentDataTime = LocalDateTime.parse(currentDay + " 00:00:00", dtf);
                        LocalDateTime removeDateTime = currentDataTime.minusMonths(1);
                        //LocalDateTime removeDateTime = currentDataTime.minusWeeks(1);
                        DateTimeFormatter ftf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                        String removeKey = ftf.format(removeDateTime) + tableName;
                        if (tableDayCnt.contains(removeKey)) {
                            System.out.println("removeKey:" + removeKey);
                            tableDayCnt.remove(removeKey);
                        }
                        if (tableDayCnt.contains(currentKey)) {
                            Long currentVal = tableDayCnt.get(currentKey);
                            // 汇总订单量
                            Long applyVal = sum + currentVal;
                            tableDayCnt.put(currentKey, applyVal);
                            collector.collect(new Tuple2<>(currentKey, applyVal));
                        } else {
                            tableDayCnt.put(currentKey, sum);
                            collector.collect(new Tuple2<>(currentKey, sum));
                        }
                    }
                });
        apply.addSink(new SinkFunction<Tuple2<String, Long>>() {
            @Override
            public void invoke(Tuple2<String, Long> val) throws Exception {
                System.out.println("<"+LocalDateTime.now()+">,<"+val.f0+">:<"+val.f1+">");
            }
        }).name("hlj97-iom-order-msg-cnt-sink-hBase");

        try {
            env.execute("hlj97-iom-order-msg-cnt");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Properties getProperties() throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", "ZRR-PRODUCT-109:9062,ZRR-PRODUCT-110:9062,ZRR-PRODUCT-111:9062,ZRR-PRODUCT-112:9062,ZRR-PRODUCT-113:9062,ZRR-PRODUCT-114:9062,ZRR-PRODUCT-116:9062,ZRR-PRODUCT-117:9062");
        props.put("group.id", "Hlj97IomOrderMsgCntProcess_191229113140");
        props.put("enable.auto.commit", "true");
        props.put("max.partition.fetch.bytes", 51200);
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        //props.put("auto.offset.reset", "earliest");
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("ssl.key.password", "zrr@kafkacenter");
        props.put("ssl.keystore.password", "zrr@kafkacenter");
        //String client_ssl_dir = "/Users/zhengm/client-ssl-two/";
        //props.put("ssl.truststore.location", client_ssl_dir+"kafkacenter_client.truststore.jks");
        //props.put("ssl.keystore.location", client_ssl_dir+"kafkacenter_client.keystore.jks");
        props.put("ssl.key.location", "Proc-Type: 4,ENCRYPTEDDEK-Info: DES-EDE3-CBC,C5515E0F1616E33CAwSHDHNko9+FE6SKGUbHZKN3DkOWW2aRWp7XEJQNvirZ/iTrogMMaC7Jj8rSGFDeDclQjXPZjGPnJNGItpIy5yMMGV+1JqW87sUYWF7OQit6aRJeE9/tv2oFuGSWsAnsfW8Kh/S+L1wa7lGi7mnolgT4blVpUmKNMbZtyi17+VdiA1FJzn1CjFuF4Mljuk6igY3y+d7RcBgKtVsBTm9ijeRFHmOupmzKaIbOA3BPsRAUQIDmIOexPvI3kGGGZM9mni35YDhl69wbX/FFQAOT2OtB0+BgbqTlErjfKggIeSKkiw56YnAZn2q0amcouofvCeYLZbpzVZUIjzHE5/lfJ8Xd9bSQkv/T+xDOxbbTPbASKt31Bh+FBck91whWdOuskBntoVvdm5SdyniUW5zdtnZlZvNTzts0TuanzoP7WSIK2ysV0CQ10ujuIQG4DPGC1+rkFKAzp54BFC4L4/LzIrOyywi2qlFo6SzL7HqPt4Km0ovicXb/W/WDXpqXoMVcc0khWBqGKMVf5cWLvI7wo/1QjELxr9fwTENwn40i6gVkje2ISsICypsB664F1PnTnKAL3aulx8AS8hJa9d0IgxfFDg3PlSHOrKGap/mU/ulgY1VasZmX74F3gVeSsc4mZljXKIX83l5XeC/21V4aapweo36V65LJ2EAUQZ1HpPY829W3w/AkXIrG863btDfwdBVhM4RhYamhSJ/N1MUSxK5PS1xmA/AUekv3G3nvYEooowrRKUFLfT6fG0S2NjQp8dIYo4Yf/iPn+1tiCr8pTk6T2LuVLim38fZCkAkcN5CHLbXoEBRPow==");
        props.put("ssl.certificate.location", "MIICtzCCAZ8CCQCiBJfS5IWn2zANBgkqhkiG9w0BAQUFADBwMQswCQYDVQQGEwJOTjELMAkGA1UECAwCTk4xCzAJBgNVBAcMAk5OMQswCQYDVQQKDAJOTjELMAkGA1UECwwCTk4xDDAKBgNVBAMMA3pycjEfMB0GCSqGSIb3DQEJARYQa2Fma2FfY2VudGVyQHpycjAeFw0xODA0MjgwMjIyNDNaFw0xODA1MjgwMjIyNDNaME8xCzAJBgNVBAYTAk5OMQswCQYDVQQIDAJOTjELMAkGA1UEBwwCTk4xCzAJBgNVBAoMAk5OMQswCQYDVQQLDAJOTjEMMAoGA1UEAwwDenJyMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCt8atL9fncmiIUVD0EX1nuxZS9j62C/orhyWdNj9TaOfrMohyZ8YksovPlxCx5NYBhPoOEN6FEmRb+z22Qo8Kxgokm7Tasd/HUzsD2NzpaFEYUoPA1+BcdmJpS3uCyd3d5O8aJcSLYsn9+ETHiB/ptj+J5G25k7UkImzLgA97yPQIDAQABMA0GCSqGSIb3DQEBBQUAA4IBAQAI3b9x9dyhscC8qWvxf9bzNBF9hHfPu9vIMWTmO7dw4ynVlRo+td6oaVlHMO9fCvL4oArr1PlIAQYxPL0sDeaT1NzdC/HeICNfol8xbsFZI7bFwnoFm9faCM5Osa0FA8AQeyIRFj6/BDfbC9Nvb5srTw0ebfiFZdEnWFY4r2RH3CRTpq29gREx3sJ5nU44FZNmPjflm2hb+qHZi0E5Gya3o0MxP/Bkvwy+BctcnrRs/KS80B5+YfzPRwK4wPMFV5kWj5VouRnd37Zvq5oYnlqBn6YF+dLf6RX2HegxwTnVukRtek2BfwvayC7H9GffxqOrUBKzY3BqwYArw+v0pVhe");
        props.put("ssl.ca.location", "MIIDszCCApugAwIBAgIJAIjoMUp17jL6MA0GCSqGSIb3DQEBCwUAMHAxCzAJBgNVBAYTAk5OMQswCQYDVQQIDAJOTjELMAkGA1UEBwwCTk4xCzAJBgNVBAoMAk5OMQswCQYDVQQLDAJOTjEMMAoGA1UEAwwDenJyMR8wHQYJKoZIhvcNAQkBFhBrYWZrYV9jZW50ZXJAenJyMB4XDTE4MDQyODAyMTgyOFoXDTI4MDQyNTAyMTgyOFowcDELMAkGA1UEBhMCTk4xCzAJBgNVBAgMAk5OMQswCQYDVQQHDAJOTjELMAkGA1UECgwCTk4xCzAJBgNVBAsMAk5OMQwwCgYDVQQDDAN6cnIxHzAdBgkqhkiG9w0BCQEWEGthZmthX2NlbnRlckB6cnIwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDIZKUcIIpuE12LCTWTXLhr3snsVKXCUbA4XOpEB535wF1gWB7AkJMyj+syVthHZaobiDpJNgDGxrGa79uvKcu9UfMmR2RQlPxwwyaXx6xBnv4RVkT9P4UkjU4u70nP+jpe+vObVqHmwtYT6L/aoP751SJ3Siqbmw7XpmMJ3Il0eV4wL9QerKo4NOLr0/X4vK4EP6/gb9r/AucYbhiRi1MqswZAUi4RyqTTo7hWonY8B1loBNnP1DVtrfCxXd4pRthFs+Rt1w3f1JuTVs+G+J3LwNKNa0SA+jyI/eyyXlmO+Gv46JVcIb4aRq8rYJPgtXHJrytQdsODiB+9PaKLqJWPAgMBAAGjUDBOMB0GA1UdDgQWBBSOYgdV4PgwOW0QeM3aPIo5OGeHtzAfBgNVHSMEGDAWgBSOYgdV4PgwOW0QeM3aPIo5OGeHtzAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBv44QR6by/F1zAp2tdxfS552l2iAbMQYMWDPrhAwaU+khmXN9CS9o37HMDebH/3LBedKoISpratEv037PMXxqZ1C/xX3XqOK7VDMH5l+VAQnwlS7E+adJtzyui9fTk+FNqw+KFnUJwvI7TyPHPac1XFc9bPrIUSNsSUg+WXF+ZFTJwm/eqdh726vzs2XvmIG4ulCc8x54mGlmbPebIAWQhsvKiphXkD5VqY1A8R9tUZNZv0W7BIr4Ug76mV8Yd5dEfI8hELjBSKt2vTr+vNPBZ1ODTePcdtINSReqWYOg/7D3EJ6qwIfnh1qJ7FNKSz2VI25LWa1ED/7PV/JpSTyqV");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kb_center\" password=\"Kb@Center_2019\";");

        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_SSL");
        props.put("ssl.keystore.type", "JKS");
        return props;
    }

    private String getOpType(int opType) {
        switch (opType) {
            case 5:
                return StreamsEnum.OpType.INSERT.getName();
            default:
                return StreamsEnum.OpType.OTHER.getName();

        }
    }
}
