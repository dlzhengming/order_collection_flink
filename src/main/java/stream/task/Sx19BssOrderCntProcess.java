package stream.task;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
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
import stream.protobuf.MessageDb;
import stream.schema.StreamsEnum;
import stream.schema.ZrrKafkaRecord;
import stream.schema.ZrrKafkaSchema;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * STREAM_ORACLE_1902_CRM01 31
 * */
public class Sx19BssOrderCntProcess {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.enableCheckpointing(50000);
        env.registerCachedFile("/home/unicomhdp/app/flink-1.8.3/client-ssl/kafkacenter_client.truststore.jks","truststore");
        env.registerCachedFile("/home/unicomhdp/app/flink-1.8.3/client-ssl/kafkacenter_client.keystore.jks","keystore");
//        env.registerCachedFile("/Users/zhengm/client-ssl/kafkacenter_client.truststore.jks","truststore");
//        env.registerCachedFile("/Users/zhengm/client-ssl/kafkacenter_client.keystore.jks","keystore");
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //* ??????????????????????????????500ms????????????checkpoint??????????????????
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //* ?????????????????????????????????????????????????????????checkpoint??????????????????
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //* ????????????Flink???????????????cancel???????????????Checkpoint???????????????????????????????????????????????????Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // State???????????????taskmanager?????????????????????checkpoint??????????????????state?????????????????????????????????????????????
        env.setStateBackend(new FsStateBackend("hdfs://dn171.hadoop.unicom:8020/flink/checkpoints/sxBss19-order-cnt/"));
        // env.setStateBackend(new FsStateBackend(new URI("file:/home/unicomhdp/app/flink-1.8.3/checkpoints/sxBss19-order-cnt/"), 0));
        // env.setStateBackend(new FsStateBackend(new URI("file:/home/ocdp/app/flink-1.8.3/checkpoints/zrr_kafka_process/"), 0));
        // kafka topic
        List<String> topicList = new ArrayList<>();
        topicList.add("STREAM_ORACLE_1902_CRM01");
        // kafka props
        Properties props = getProperties();
        // kafka consumer
        ZrrFlinkKafkaConsumer<ZrrKafkaRecord> consumer = new ZrrFlinkKafkaConsumer(topicList, new ZrrKafkaSchema(), props);
        consumer.setStartFromGroupOffsets();
        // kafka connector source
        DataStreamSource<ZrrKafkaRecord> zrrDs = env.addSource(consumer);
        // filter TF_B_TRADE/152
        SingleOutputStreamOperator<ZrrKafkaRecord> filterDataStream = zrrDs.filter(new FilterFunction<ZrrKafkaRecord>() {
            @Override
            public boolean filter(ZrrKafkaRecord zrrKafkaRecord){
                String topic = zrrKafkaRecord.getTopic();
                int partition = zrrKafkaRecord.getPartition();
                //* STREAM_ORACLE_1902_CRM01 UCR_CRM4	TF_B_TRADE	31
                if("STREAM_ORACLE_1902_CRM01".equals(topic)&&(partition==31)){
                    return true;
                }else{
                    return false;
                }
            }
        });
        // filter insert order
        SingleOutputStreamOperator<ZrrKafkaRecord> filterInsertDataStream = filterDataStream.filter(new FilterFunction<ZrrKafkaRecord>() {
            @Override
            public boolean filter(ZrrKafkaRecord zrrKafkaRecord){
                try {
                    // protobuf
                    MessageDb.Record msgDbRecord = MessageDb.Record.parseFrom(zrrKafkaRecord.getMesg());
                    if(5==msgDbRecord.getOperationType()){
                        return true;
                    }else{
                        return false;
                    }
                } catch (InvalidProtocolBufferException e) {
                    System.out.println("Protobuf Data conversion exception??????"+zrrKafkaRecord.toString()+"???");
                    return false;
                }
            }
        });
        // map
        SingleOutputStreamOperator<Tuple3<String,String,Long>> mapDataStream = filterInsertDataStream.map(new MapFunction<ZrrKafkaRecord, Tuple3<String,String,Long>>() {
            @Override
            public Tuple3<String,String,Long> map(ZrrKafkaRecord zrrKafkaRecord){
                DateTimeFormatter ftf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                String dt = ftf.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(zrrKafkaRecord.getTimestep()), ZoneId.of("Asia/Shanghai")));
                return new Tuple3<>(zrrKafkaRecord.getTopic(),dt,1L);
            }
        });
        // keyBy sum
        SingleOutputStreamOperator<Tuple3<String, String, Long>> applyStream = mapDataStream.keyBy(0, 1)
                .timeWindow(Time.minutes(3))
                .apply(new RichWindowFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple, TimeWindow>() {

                    private transient MapState<String, Long> topicDayCnt;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<String, Long>(
                                "topic-day-cnt", // ????????????
                                TypeInformation.of(String.class),
                                TypeInformation.of(Long.class)
                        );
                        topicDayCnt = getRuntimeContext().getMapState(descriptor);
                    }

                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<String, String, Long>> iterable, Collector<Tuple3<String, String, Long>> collector) throws Exception {
                        String topic = "";
                        String currentDay = "";
                        Long sum = 0L;

                        for (Tuple3<String, String, Long> element : iterable) {
                            topic = element.f0;
                            currentDay = element.f1;
                            sum += element.f2;
                        }
                        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        LocalDateTime currentDataTime = LocalDateTime.parse(currentDay + " 00:00:00", dtf);
                        LocalDateTime removeDateTime = currentDataTime.minusMonths(1);
                        //LocalDateTime removeDateTime = currentDataTime.minusWeeks(1);
                        DateTimeFormatter ftf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                        String removeKey = ftf.format(removeDateTime);
                        if (topicDayCnt.contains(removeKey)) {
                            System.out.println("removeKey:"+removeKey);
                            topicDayCnt.remove(removeKey);
                        }
                        if (topicDayCnt.contains(currentDay)) {
                            Long currentVal = topicDayCnt.get(currentDay);
                            // ???????????????
                            Long applyVal = sum + currentVal;
                            topicDayCnt.put(currentDay, applyVal);
                            collector.collect(new Tuple3<>(topic, currentDay, applyVal));
                        } else {
                            topicDayCnt.put(currentDay, sum);
                            collector.collect(new Tuple3<>(topic, currentDay, sum));
                        }
                    }
                });
        // sink to hBase
        applyStream.addSink(new SinkFunction<Tuple3<String, String, Long>>() {
            @Override
            public void invoke(Tuple3<String, String, Long> val) throws Exception {
                System.out.println(val.f1+"???SX_BSS19_ORDER_CNT???"+val.f2);
            }
        }).name("sxBss19-order-cnt-sink-hBase");

        try {
            env.execute("sxBss19-order-cnt");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Properties getProperties() throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", "ZRR-PRODUCT-109:9062,ZRR-PRODUCT-110:9062,ZRR-PRODUCT-111:9062,ZRR-PRODUCT-112:9062,ZRR-PRODUCT-113:9062,ZRR-PRODUCT-114:9062,ZRR-PRODUCT-116:9062,ZRR-PRODUCT-117:9062");
        props.put("group.id", "SXBSS19OrderCntProcess_191228");
        props.put("enable.auto.commit", "true");
        props.put("max.partition.fetch.bytes", 51200);
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        //props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("ssl.key.password", "zrr@kafkacenter");
        props.put("ssl.keystore.password", "zrr@kafkacenter");
        props.put("ssl.key.location", "Proc-Type: 4,ENCRYPTEDDEK-Info: DES-EDE3-CBC,C5515E0F1616E33CAwSHDHNko9+FE6SKGUbHZKN3DkOWW2aRWp7XEJQNvirZ/iTrogMMaC7Jj8rSGFDeDclQjXPZjGPnJNGItpIy5yMMGV+1JqW87sUYWF7OQit6aRJeE9/tv2oFuGSWsAnsfW8Kh/S+L1wa7lGi7mnolgT4blVpUmKNMbZtyi17+VdiA1FJzn1CjFuF4Mljuk6igY3y+d7RcBgKtVsBTm9ijeRFHmOupmzKaIbOA3BPsRAUQIDmIOexPvI3kGGGZM9mni35YDhl69wbX/FFQAOT2OtB0+BgbqTlErjfKggIeSKkiw56YnAZn2q0amcouofvCeYLZbpzVZUIjzHE5/lfJ8Xd9bSQkv/T+xDOxbbTPbASKt31Bh+FBck91whWdOuskBntoVvdm5SdyniUW5zdtnZlZvNTzts0TuanzoP7WSIK2ysV0CQ10ujuIQG4DPGC1+rkFKAzp54BFC4L4/LzIrOyywi2qlFo6SzL7HqPt4Km0ovicXb/W/WDXpqXoMVcc0khWBqGKMVf5cWLvI7wo/1QjELxr9fwTENwn40i6gVkje2ISsICypsB664F1PnTnKAL3aulx8AS8hJa9d0IgxfFDg3PlSHOrKGap/mU/ulgY1VasZmX74F3gVeSsc4mZljXKIX83l5XeC/21V4aapweo36V65LJ2EAUQZ1HpPY829W3w/AkXIrG863btDfwdBVhM4RhYamhSJ/N1MUSxK5PS1xmA/AUekv3G3nvYEooowrRKUFLfT6fG0S2NjQp8dIYo4Yf/iPn+1tiCr8pTk6T2LuVLim38fZCkAkcN5CHLbXoEBRPow==");
        props.put("ssl.certificate.location", "MIICtzCCAZ8CCQCiBJfS5IWn2zANBgkqhkiG9w0BAQUFADBwMQswCQYDVQQGEwJOTjELMAkGA1UECAwCTk4xCzAJBgNVBAcMAk5OMQswCQYDVQQKDAJOTjELMAkGA1UECwwCTk4xDDAKBgNVBAMMA3pycjEfMB0GCSqGSIb3DQEJARYQa2Fma2FfY2VudGVyQHpycjAeFw0xODA0MjgwMjIyNDNaFw0xODA1MjgwMjIyNDNaME8xCzAJBgNVBAYTAk5OMQswCQYDVQQIDAJOTjELMAkGA1UEBwwCTk4xCzAJBgNVBAoMAk5OMQswCQYDVQQLDAJOTjEMMAoGA1UEAwwDenJyMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCt8atL9fncmiIUVD0EX1nuxZS9j62C/orhyWdNj9TaOfrMohyZ8YksovPlxCx5NYBhPoOEN6FEmRb+z22Qo8Kxgokm7Tasd/HUzsD2NzpaFEYUoPA1+BcdmJpS3uCyd3d5O8aJcSLYsn9+ETHiB/ptj+J5G25k7UkImzLgA97yPQIDAQABMA0GCSqGSIb3DQEBBQUAA4IBAQAI3b9x9dyhscC8qWvxf9bzNBF9hHfPu9vIMWTmO7dw4ynVlRo+td6oaVlHMO9fCvL4oArr1PlIAQYxPL0sDeaT1NzdC/HeICNfol8xbsFZI7bFwnoFm9faCM5Osa0FA8AQeyIRFj6/BDfbC9Nvb5srTw0ebfiFZdEnWFY4r2RH3CRTpq29gREx3sJ5nU44FZNmPjflm2hb+qHZi0E5Gya3o0MxP/Bkvwy+BctcnrRs/KS80B5+YfzPRwK4wPMFV5kWj5VouRnd37Zvq5oYnlqBn6YF+dLf6RX2HegxwTnVukRtek2BfwvayC7H9GffxqOrUBKzY3BqwYArw+v0pVhe");
        props.put("ssl.ca.location", "MIIDszCCApugAwIBAgIJAIjoMUp17jL6MA0GCSqGSIb3DQEBCwUAMHAxCzAJBgNVBAYTAk5OMQswCQYDVQQIDAJOTjELMAkGA1UEBwwCTk4xCzAJBgNVBAoMAk5OMQswCQYDVQQLDAJOTjEMMAoGA1UEAwwDenJyMR8wHQYJKoZIhvcNAQkBFhBrYWZrYV9jZW50ZXJAenJyMB4XDTE4MDQyODAyMTgyOFoXDTI4MDQyNTAyMTgyOFowcDELMAkGA1UEBhMCTk4xCzAJBgNVBAgMAk5OMQswCQYDVQQHDAJOTjELMAkGA1UECgwCTk4xCzAJBgNVBAsMAk5OMQwwCgYDVQQDDAN6cnIxHzAdBgkqhkiG9w0BCQEWEGthZmthX2NlbnRlckB6cnIwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDIZKUcIIpuE12LCTWTXLhr3snsVKXCUbA4XOpEB535wF1gWB7AkJMyj+syVthHZaobiDpJNgDGxrGa79uvKcu9UfMmR2RQlPxwwyaXx6xBnv4RVkT9P4UkjU4u70nP+jpe+vObVqHmwtYT6L/aoP751SJ3Siqbmw7XpmMJ3Il0eV4wL9QerKo4NOLr0/X4vK4EP6/gb9r/AucYbhiRi1MqswZAUi4RyqTTo7hWonY8B1loBNnP1DVtrfCxXd4pRthFs+Rt1w3f1JuTVs+G+J3LwNKNa0SA+jyI/eyyXlmO+Gv46JVcIb4aRq8rYJPgtXHJrytQdsODiB+9PaKLqJWPAgMBAAGjUDBOMB0GA1UdDgQWBBSOYgdV4PgwOW0QeM3aPIo5OGeHtzAfBgNVHSMEGDAWgBSOYgdV4PgwOW0QeM3aPIo5OGeHtzAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBv44QR6by/F1zAp2tdxfS552l2iAbMQYMWDPrhAwaU+khmXN9CS9o37HMDebH/3LBedKoISpratEv037PMXxqZ1C/xX3XqOK7VDMH5l+VAQnwlS7E+adJtzyui9fTk+FNqw+KFnUJwvI7TyPHPac1XFc9bPrIUSNsSUg+WXF+ZFTJwm/eqdh726vzs2XvmIG4ulCc8x54mGlmbPebIAWQhsvKiphXkD5VqY1A8R9tUZNZv0W7BIr4Ug76mV8Yd5dEfI8hELjBSKt2vTr+vNPBZ1ODTePcdtINSReqWYOg/7D3EJ6qwIfnh1qJ7FNKSz2VI25LWa1ED/7PV/JpSTyqV");

        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_SSL");
        props.put("ssl.keystore.type", "JKS");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"new_sale\" password=\"ns@hrb\";");
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
