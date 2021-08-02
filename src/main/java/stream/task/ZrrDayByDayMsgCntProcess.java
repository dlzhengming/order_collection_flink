package stream.task;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import stream.Zrr.ZrrFlinkKafkaConsumer;
import stream.schema.StreamsEnum;
import stream.schema.ZrrKafkaRecord;
import stream.schema.ZrrKafkaSchema;
import stream.utils.HbaseUtil;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * STREAM_ORACLE_1902_CRM01
 * */
public class ZrrDayByDayMsgCntProcess {
    public static void main(String[] args) throws Exception {
        final String topics;
        final String client_ssl_dir;
        final String ssl_chose;
        final String day_cnt;
        final String window_size;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            topics = params.get("topics");
            client_ssl_dir = params.get("client_ssl_dir");
            ssl_chose = params.get("ssl_chose");
            day_cnt = params.get("day_cnt");
            window_size = params.get("window_size");
        } catch (Exception e) {
            System.err.println("No topics specified. Please run 'ZrrMsgCntProcess --topics <topicA,topicB,topicC> --client_ssl_dir <client_ssl_dir,no> --ssl_chose <SSL_ONE,SSL_TWO,SSL_NO> --day_cnt 7 --window_size 2(minute)'");
            return;
        }

        //String topics = "STREAM_ORACLE_1902_CRM01";
        //String ssl_chose = "SSL_ONE";
        //String day_cnt = "2";
        //String window_size = "2";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000*60*2);
        env.registerCachedFile(client_ssl_dir+"/kafkacenter_client.truststore.jks","truststore");
        env.registerCachedFile(client_ssl_dir+"/kafkacenter_client.keystore.jks","keystore");
        //env.registerCachedFile("/Users/zhengm/client-ssl/kafkacenter_client.truststore.jks","truststore");
        //env.registerCachedFile("/Users/zhengm/client-ssl/kafkacenter_client.keystore.jks","keystore");
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("hdfs://audit-dp02:8020/flink/checkpoints/day_to_day_msg_cnt/"+topics+"/"));
        // kafka topic
        List<String> topicList = new ArrayList<>();
        for(String topic :topics.split(",")){
            topicList.add(topic);
        }
        // kafka props
        Properties props = getProperties(ssl_chose);
        // kafka consumer
        ZrrFlinkKafkaConsumer<ZrrKafkaRecord> consumer = new ZrrFlinkKafkaConsumer(topicList, new ZrrKafkaSchema(), props);
        //consumer.setStartFromGroupOffsets();
        consumer.setStartFromTimestamp(1579017600000L);
        // kafka connector source
        //DataStreamSource<ZrrKafkaRecord> zrrKafkaRecordDataStreamSource = env.addSource(consumer);
        SingleOutputStreamOperator<ZrrKafkaRecord> zrrKafkaRecordDataStreamSource = env.addSource(consumer).uid("SOURCE-"+topics);
        // assignTimestampsAndWatermarks
        SingleOutputStreamOperator<ZrrKafkaRecord> zrrKafkaRecordSingleOutputStreamOperator = zrrKafkaRecordDataStreamSource.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<ZrrKafkaRecord>() {
            Long currentMaxTimestamp = 0L;

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp);
            }

            @Override
            public long extractTimestamp(ZrrKafkaRecord zrrKafkaRecord, long l) {
                currentMaxTimestamp = zrrKafkaRecord.getTimestep();
                return currentMaxTimestamp;
            }
        });
        SingleOutputStreamOperator<Tuple4<String, Integer, String, Long>> mapStream = zrrKafkaRecordSingleOutputStreamOperator.map(new MapFunction<ZrrKafkaRecord, Tuple4<String, Integer, String, Long>>() {
            @Override
            public Tuple4<String, Integer, String, Long> map(ZrrKafkaRecord zrrKafkaRecord) throws Exception {
                DateTimeFormatter dayFtf = DateTimeFormatter.ofPattern("yyyyMMdd");
                String day = dayFtf.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(zrrKafkaRecord.getTimestep()), ZoneId.of("Asia/Shanghai")));
                return new Tuple4<>(zrrKafkaRecord.getTopic(),zrrKafkaRecord.getPartition(),day,1L);
            }
        });
        // timeWindowWindowedStream
        WindowedStream<Tuple4<String, Integer, String, Long>, Tuple, TimeWindow> timeWindowWindowedStream = mapStream.keyBy(0, 1).keyBy(2).timeWindow(Time.minutes(Integer.parseInt(window_size)));
        // 日消息量
        SingleOutputStreamOperator<Tuple3<String, String, String>> dayMsgCntTuple = timeWindowWindowedStream.apply(new RichWindowFunction<Tuple4<String, Integer, String, Long>, Tuple3<String, String, String>, Tuple, TimeWindow>() {

            private transient ValueState<Long> dayMsgCnt;

            @Override
            public void open(Configuration config) {

                ValueStateDescriptor<Long> dayMsgCntDescriptor =
                        new ValueStateDescriptor<>("dayMsgCnt", TypeInformation.of(new TypeHint<Long>() {
                        }), 0L);

                StateTtlConfig dayTtlConfig = StateTtlConfig
                        .newBuilder(org.apache.flink.api.common.time.Time.days(Integer.parseInt(day_cnt) * 2))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .cleanupIncrementally(1, false)
                        .build();

                dayMsgCntDescriptor.enableTimeToLive(dayTtlConfig);

                dayMsgCnt = getRuntimeContext().getState(dayMsgCntDescriptor);
            }

            @Override
            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple4<String, Integer, String, Long>> iterable, Collector<Tuple3<String, String, String>> out) throws Exception {

                DateTimeFormatter minuteFtf = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
                long windowEnd = timeWindow.getEnd();
                LocalDateTime localDateTimeWindowEnd = LocalDateTime.ofInstant(Instant.ofEpochMilli(windowEnd), ZoneId.of("Asia/Shanghai"));
                String endMinute = minuteFtf.format(localDateTimeWindowEnd);

                String topic = "";
                String partition = "";
                String day = "";
                long sum = 0;

                for (Tuple4<String, Integer, String, Long> zrrRecord : iterable) {
                    topic = zrrRecord.f0;
                    partition = String.valueOf(zrrRecord.f1);
                    day = zrrRecord.f2;
                    sum += zrrRecord.f3;
                }
                /** rowKey */
                String rowKey = topic + "-" + partition + "-" + day;
                /** 日消息量 */
                if (dayMsgCnt.value() == null) {
                    dayMsgCnt.update(sum);
                } else {
                    dayMsgCnt.update(dayMsgCnt.value() + sum);
                }
                out.collect(new Tuple3<>(rowKey, endMinute, String.valueOf(dayMsgCnt.value())));
            }
        }).uid("APPLY-"+topics);
        dayMsgCntTuple.addSink(new SinkFunction<Tuple3<String, String, String>>() {
            @Override
            public void invoke(Tuple3<String, String, String> zrrRecord, Context context) throws Exception {
                Map<String,String> dataMap = new HashMap<>();
                dataMap.put(zrrRecord.f1,zrrRecord.f2);
                HbaseUtil.put("DAY_BY_DAY_MSG_CNT",zrrRecord.f0,"F1",dataMap);
            }
        }).name("day-by-day-msg-cnt-sink-hBase");

        try {
            env.execute("day-by-day-msg-cnt");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Properties getProperties(String ssl_chose) throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", "ZRR-PRODUCT-109:9062,ZRR-PRODUCT-110:9062,ZRR-PRODUCT-111:9062,ZRR-PRODUCT-112:9062,ZRR-PRODUCT-113:9062,ZRR-PRODUCT-114:9062,ZRR-PRODUCT-116:9062,ZRR-PRODUCT-117:9062");
        props.put("group.id", "ZrrDayMsgCntProcess_test");
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
        if("SSL_ONE".equals(ssl_chose)){
            props.put("ssl.key.location", "Proc-Type: 4,ENCRYPTEDDEK-Info: DES-EDE3-CBC,C5515E0F1616E33CAwSHDHNko9+FE6SKGUbHZKN3DkOWW2aRWp7XEJQNvirZ/iTrogMMaC7Jj8rSGFDeDclQjXPZjGPnJNGItpIy5yMMGV+1JqW87sUYWF7OQit6aRJeE9/tv2oFuGSWsAnsfW8Kh/S+L1wa7lGi7mnolgT4blVpUmKNMbZtyi17+VdiA1FJzn1CjFuF4Mljuk6igY3y+d7RcBgKtVsBTm9ijeRFHmOupmzKaIbOA3BPsRAUQIDmIOexPvI3kGGGZM9mni35YDhl69wbX/FFQAOT2OtB0+BgbqTlErjfKggIeSKkiw56YnAZn2q0amcouofvCeYLZbpzVZUIjzHE5/lfJ8Xd9bSQkv/T+xDOxbbTPbASKt31Bh+FBck91whWdOuskBntoVvdm5SdyniUW5zdtnZlZvNTzts0TuanzoP7WSIK2ysV0CQ10ujuIQG4DPGC1+rkFKAzp54BFC4L4/LzIrOyywi2qlFo6SzL7HqPt4Km0ovicXb/W/WDXpqXoMVcc0khWBqGKMVf5cWLvI7wo/1QjELxr9fwTENwn40i6gVkje2ISsICypsB664F1PnTnKAL3aulx8AS8hJa9d0IgxfFDg3PlSHOrKGap/mU/ulgY1VasZmX74F3gVeSsc4mZljXKIX83l5XeC/21V4aapweo36V65LJ2EAUQZ1HpPY829W3w/AkXIrG863btDfwdBVhM4RhYamhSJ/N1MUSxK5PS1xmA/AUekv3G3nvYEooowrRKUFLfT6fG0S2NjQp8dIYo4Yf/iPn+1tiCr8pTk6T2LuVLim38fZCkAkcN5CHLbXoEBRPow==");
            props.put("ssl.certificate.location", "MIICtzCCAZ8CCQCiBJfS5IWn2zANBgkqhkiG9w0BAQUFADBwMQswCQYDVQQGEwJOTjELMAkGA1UECAwCTk4xCzAJBgNVBAcMAk5OMQswCQYDVQQKDAJOTjELMAkGA1UECwwCTk4xDDAKBgNVBAMMA3pycjEfMB0GCSqGSIb3DQEJARYQa2Fma2FfY2VudGVyQHpycjAeFw0xODA0MjgwMjIyNDNaFw0xODA1MjgwMjIyNDNaME8xCzAJBgNVBAYTAk5OMQswCQYDVQQIDAJOTjELMAkGA1UEBwwCTk4xCzAJBgNVBAoMAk5OMQswCQYDVQQLDAJOTjEMMAoGA1UEAwwDenJyMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCt8atL9fncmiIUVD0EX1nuxZS9j62C/orhyWdNj9TaOfrMohyZ8YksovPlxCx5NYBhPoOEN6FEmRb+z22Qo8Kxgokm7Tasd/HUzsD2NzpaFEYUoPA1+BcdmJpS3uCyd3d5O8aJcSLYsn9+ETHiB/ptj+J5G25k7UkImzLgA97yPQIDAQABMA0GCSqGSIb3DQEBBQUAA4IBAQAI3b9x9dyhscC8qWvxf9bzNBF9hHfPu9vIMWTmO7dw4ynVlRo+td6oaVlHMO9fCvL4oArr1PlIAQYxPL0sDeaT1NzdC/HeICNfol8xbsFZI7bFwnoFm9faCM5Osa0FA8AQeyIRFj6/BDfbC9Nvb5srTw0ebfiFZdEnWFY4r2RH3CRTpq29gREx3sJ5nU44FZNmPjflm2hb+qHZi0E5Gya3o0MxP/Bkvwy+BctcnrRs/KS80B5+YfzPRwK4wPMFV5kWj5VouRnd37Zvq5oYnlqBn6YF+dLf6RX2HegxwTnVukRtek2BfwvayC7H9GffxqOrUBKzY3BqwYArw+v0pVhe");
            props.put("ssl.ca.location", "MIIDszCCApugAwIBAgIJAIjoMUp17jL6MA0GCSqGSIb3DQEBCwUAMHAxCzAJBgNVBAYTAk5OMQswCQYDVQQIDAJOTjELMAkGA1UEBwwCTk4xCzAJBgNVBAoMAk5OMQswCQYDVQQLDAJOTjEMMAoGA1UEAwwDenJyMR8wHQYJKoZIhvcNAQkBFhBrYWZrYV9jZW50ZXJAenJyMB4XDTE4MDQyODAyMTgyOFoXDTI4MDQyNTAyMTgyOFowcDELMAkGA1UEBhMCTk4xCzAJBgNVBAgMAk5OMQswCQYDVQQHDAJOTjELMAkGA1UECgwCTk4xCzAJBgNVBAsMAk5OMQwwCgYDVQQDDAN6cnIxHzAdBgkqhkiG9w0BCQEWEGthZmthX2NlbnRlckB6cnIwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDIZKUcIIpuE12LCTWTXLhr3snsVKXCUbA4XOpEB535wF1gWB7AkJMyj+syVthHZaobiDpJNgDGxrGa79uvKcu9UfMmR2RQlPxwwyaXx6xBnv4RVkT9P4UkjU4u70nP+jpe+vObVqHmwtYT6L/aoP751SJ3Siqbmw7XpmMJ3Il0eV4wL9QerKo4NOLr0/X4vK4EP6/gb9r/AucYbhiRi1MqswZAUi4RyqTTo7hWonY8B1loBNnP1DVtrfCxXd4pRthFs+Rt1w3f1JuTVs+G+J3LwNKNa0SA+jyI/eyyXlmO+Gv46JVcIb4aRq8rYJPgtXHJrytQdsODiB+9PaKLqJWPAgMBAAGjUDBOMB0GA1UdDgQWBBSOYgdV4PgwOW0QeM3aPIo5OGeHtzAfBgNVHSMEGDAWgBSOYgdV4PgwOW0QeM3aPIo5OGeHtzAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBv44QR6by/F1zAp2tdxfS552l2iAbMQYMWDPrhAwaU+khmXN9CS9o37HMDebH/3LBedKoISpratEv037PMXxqZ1C/xX3XqOK7VDMH5l+VAQnwlS7E+adJtzyui9fTk+FNqw+KFnUJwvI7TyPHPac1XFc9bPrIUSNsSUg+WXF+ZFTJwm/eqdh726vzs2XvmIG4ulCc8x54mGlmbPebIAWQhsvKiphXkD5VqY1A8R9tUZNZv0W7BIr4Ug76mV8Yd5dEfI8hELjBSKt2vTr+vNPBZ1ODTePcdtINSReqWYOg/7D3EJ6qwIfnh1qJ7FNKSz2VI25LWa1ED/7PV/JpSTyqV");
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"new_sale\" password=\"ns@hrb\";");
        }else if("SSL_TWO".equals(ssl_chose)){
            props.put("ssl.key.location", "Proc-Type: 4,ENCRYPTEDDEK-Info: DES-EDE3-CBC,C5515E0F1616E33CAwSHDHNko9+FE6SKGUbHZKN3DkOWW2aRWp7XEJQNvirZ/iTrogMMaC7Jj8rSGFDeDclQjXPZjGPnJNGItpIy5yMMGV+1JqW87sUYWF7OQit6aRJeE9/tv2oFuGSWsAnsfW8Kh/S+L1wa7lGi7mnolgT4blVpUmKNMbZtyi17+VdiA1FJzn1CjFuF4Mljuk6igY3y+d7RcBgKtVsBTm9ijeRFHmOupmzKaIbOA3BPsRAUQIDmIOexPvI3kGGGZM9mni35YDhl69wbX/FFQAOT2OtB0+BgbqTlErjfKggIeSKkiw56YnAZn2q0amcouofvCeYLZbpzVZUIjzHE5/lfJ8Xd9bSQkv/T+xDOxbbTPbASKt31Bh+FBck91whWdOuskBntoVvdm5SdyniUW5zdtnZlZvNTzts0TuanzoP7WSIK2ysV0CQ10ujuIQG4DPGC1+rkFKAzp54BFC4L4/LzIrOyywi2qlFo6SzL7HqPt4Km0ovicXb/W/WDXpqXoMVcc0khWBqGKMVf5cWLvI7wo/1QjELxr9fwTENwn40i6gVkje2ISsICypsB664F1PnTnKAL3aulx8AS8hJa9d0IgxfFDg3PlSHOrKGap/mU/ulgY1VasZmX74F3gVeSsc4mZljXKIX83l5XeC/21V4aapweo36V65LJ2EAUQZ1HpPY829W3w/AkXIrG863btDfwdBVhM4RhYamhSJ/N1MUSxK5PS1xmA/AUekv3G3nvYEooowrRKUFLfT6fG0S2NjQp8dIYo4Yf/iPn+1tiCr8pTk6T2LuVLim38fZCkAkcN5CHLbXoEBRPow==");
            props.put("ssl.certificate.location", "MIICtzCCAZ8CCQCiBJfS5IWn2zANBgkqhkiG9w0BAQUFADBwMQswCQYDVQQGEwJOTjELMAkGA1UECAwCTk4xCzAJBgNVBAcMAk5OMQswCQYDVQQKDAJOTjELMAkGA1UECwwCTk4xDDAKBgNVBAMMA3pycjEfMB0GCSqGSIb3DQEJARYQa2Fma2FfY2VudGVyQHpycjAeFw0xODA0MjgwMjIyNDNaFw0xODA1MjgwMjIyNDNaME8xCzAJBgNVBAYTAk5OMQswCQYDVQQIDAJOTjELMAkGA1UEBwwCTk4xCzAJBgNVBAoMAk5OMQswCQYDVQQLDAJOTjEMMAoGA1UEAwwDenJyMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCt8atL9fncmiIUVD0EX1nuxZS9j62C/orhyWdNj9TaOfrMohyZ8YksovPlxCx5NYBhPoOEN6FEmRb+z22Qo8Kxgokm7Tasd/HUzsD2NzpaFEYUoPA1+BcdmJpS3uCyd3d5O8aJcSLYsn9+ETHiB/ptj+J5G25k7UkImzLgA97yPQIDAQABMA0GCSqGSIb3DQEBBQUAA4IBAQAI3b9x9dyhscC8qWvxf9bzNBF9hHfPu9vIMWTmO7dw4ynVlRo+td6oaVlHMO9fCvL4oArr1PlIAQYxPL0sDeaT1NzdC/HeICNfol8xbsFZI7bFwnoFm9faCM5Osa0FA8AQeyIRFj6/BDfbC9Nvb5srTw0ebfiFZdEnWFY4r2RH3CRTpq29gREx3sJ5nU44FZNmPjflm2hb+qHZi0E5Gya3o0MxP/Bkvwy+BctcnrRs/KS80B5+YfzPRwK4wPMFV5kWj5VouRnd37Zvq5oYnlqBn6YF+dLf6RX2HegxwTnVukRtek2BfwvayC7H9GffxqOrUBKzY3BqwYArw+v0pVhe");
            props.put("ssl.ca.location", "MIIDszCCApugAwIBAgIJAIjoMUp17jL6MA0GCSqGSIb3DQEBCwUAMHAxCzAJBgNVBAYTAk5OMQswCQYDVQQIDAJOTjELMAkGA1UEBwwCTk4xCzAJBgNVBAoMAk5OMQswCQYDVQQLDAJOTjEMMAoGA1UEAwwDenJyMR8wHQYJKoZIhvcNAQkBFhBrYWZrYV9jZW50ZXJAenJyMB4XDTE4MDQyODAyMTgyOFoXDTI4MDQyNTAyMTgyOFowcDELMAkGA1UEBhMCTk4xCzAJBgNVBAgMAk5OMQswCQYDVQQHDAJOTjELMAkGA1UECgwCTk4xCzAJBgNVBAsMAk5OMQwwCgYDVQQDDAN6cnIxHzAdBgkqhkiG9w0BCQEWEGthZmthX2NlbnRlckB6cnIwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDIZKUcIIpuE12LCTWTXLhr3snsVKXCUbA4XOpEB535wF1gWB7AkJMyj+syVthHZaobiDpJNgDGxrGa79uvKcu9UfMmR2RQlPxwwyaXx6xBnv4RVkT9P4UkjU4u70nP+jpe+vObVqHmwtYT6L/aoP751SJ3Siqbmw7XpmMJ3Il0eV4wL9QerKo4NOLr0/X4vK4EP6/gb9r/AucYbhiRi1MqswZAUi4RyqTTo7hWonY8B1loBNnP1DVtrfCxXd4pRthFs+Rt1w3f1JuTVs+G+J3LwNKNa0SA+jyI/eyyXlmO+Gv46JVcIb4aRq8rYJPgtXHJrytQdsODiB+9PaKLqJWPAgMBAAGjUDBOMB0GA1UdDgQWBBSOYgdV4PgwOW0QeM3aPIo5OGeHtzAfBgNVHSMEGDAWgBSOYgdV4PgwOW0QeM3aPIo5OGeHtzAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBv44QR6by/F1zAp2tdxfS552l2iAbMQYMWDPrhAwaU+khmXN9CS9o37HMDebH/3LBedKoISpratEv037PMXxqZ1C/xX3XqOK7VDMH5l+VAQnwlS7E+adJtzyui9fTk+FNqw+KFnUJwvI7TyPHPac1XFc9bPrIUSNsSUg+WXF+ZFTJwm/eqdh726vzs2XvmIG4ulCc8x54mGlmbPebIAWQhsvKiphXkD5VqY1A8R9tUZNZv0W7BIr4Ug76mV8Yd5dEfI8hELjBSKt2vTr+vNPBZ1ODTePcdtINSReqWYOg/7D3EJ6qwIfnh1qJ7FNKSz2VI25LWa1ED/7PV/JpSTyqV");
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kb_center\" password=\"Kb@Center_2019\";");
        }
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
