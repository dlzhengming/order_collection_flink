package stream.Test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import stream.Zrr.ZrrFlinkKafkaConsumer;
import stream.schema.ZrrKafkaRecord;
import stream.schema.ZrrKafkaSchema;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ZrrKafkaProc {
    public static void main(String[] args) throws Exception {
        System.out.println("ZrrKafkaProc Flink Job Start at "+ LocalDateTime.now());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(50000);
        env.registerCachedFile("/home/unicomhdp/app/flink-1.8.3/client-ssl/kafkacenter_client.truststore.jks","truststore");
        env.registerCachedFile("/home/unicomhdp/app/flink-1.8.3/client-ssl/kafkacenter_client.keystore.jks","keystore");
        // kafka topic
        List<String> topicList = new ArrayList<>();
        topicList.add("STREAM_ORACLE_8802_CRM01");
        // kafka props
        Properties props = getProperties();
        // kafka consumer
        ZrrFlinkKafkaConsumer<ZrrKafkaRecord> consumer = new ZrrFlinkKafkaConsumer(topicList, new ZrrKafkaSchema(), props);
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
        String groupId = "zrr_kafka_process_191228";
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

//        props.put("ssl.truststore.location", ZrrKafkaProc.class.getResource("/client-ssl/kafkacenter_client.truststore.jks").toString());
//        props.put("ssl.keystore.location", ZrrKafkaProc.class.getResource("/client-ssl/kafkacenter_client.keystore.jks").toString());

        props.put("ssl.key.location", "Proc-Type: 4,ENCRYPTEDDEK-Info: DES-EDE3-CBC,C5515E0F1616E33CAwSHDHNko9+FE6SKGUbHZKN3DkOWW2aRWp7XEJQNvirZ/iTrogMMaC7Jj8rSGFDeDclQjXPZjGPnJNGItpIy5yMMGV+1JqW87sUYWF7OQit6aRJeE9/tv2oFuGSWsAnsfW8Kh/S+L1wa7lGi7mnolgT4blVpUmKNMbZtyi17+VdiA1FJzn1CjFuF4Mljuk6igY3y+d7RcBgKtVsBTm9ijeRFHmOupmzKaIbOA3BPsRAUQIDmIOexPvI3kGGGZM9mni35YDhl69wbX/FFQAOT2OtB0+BgbqTlErjfKggIeSKkiw56YnAZn2q0amcouofvCeYLZbpzVZUIjzHE5/lfJ8Xd9bSQkv/T+xDOxbbTPbASKt31Bh+FBck91whWdOuskBntoVvdm5SdyniUW5zdtnZlZvNTzts0TuanzoP7WSIK2ysV0CQ10ujuIQG4DPGC1+rkFKAzp54BFC4L4/LzIrOyywi2qlFo6SzL7HqPt4Km0ovicXb/W/WDXpqXoMVcc0khWBqGKMVf5cWLvI7wo/1QjELxr9fwTENwn40i6gVkje2ISsICypsB664F1PnTnKAL3aulx8AS8hJa9d0IgxfFDg3PlSHOrKGap/mU/ulgY1VasZmX74F3gVeSsc4mZljXKIX83l5XeC/21V4aapweo36V65LJ2EAUQZ1HpPY829W3w/AkXIrG863btDfwdBVhM4RhYamhSJ/N1MUSxK5PS1xmA/AUekv3G3nvYEooowrRKUFLfT6fG0S2NjQp8dIYo4Yf/iPn+1tiCr8pTk6T2LuVLim38fZCkAkcN5CHLbXoEBRPow==");
        props.put("ssl.certificate.location", "MIICtzCCAZ8CCQCiBJfS5IWn2zANBgkqhkiG9w0BAQUFADBwMQswCQYDVQQGEwJOTjELMAkGA1UECAwCTk4xCzAJBgNVBAcMAk5OMQswCQYDVQQKDAJOTjELMAkGA1UECwwCTk4xDDAKBgNVBAMMA3pycjEfMB0GCSqGSIb3DQEJARYQa2Fma2FfY2VudGVyQHpycjAeFw0xODA0MjgwMjIyNDNaFw0xODA1MjgwMjIyNDNaME8xCzAJBgNVBAYTAk5OMQswCQYDVQQIDAJOTjELMAkGA1UEBwwCTk4xCzAJBgNVBAoMAk5OMQswCQYDVQQLDAJOTjEMMAoGA1UEAwwDenJyMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCt8atL9fncmiIUVD0EX1nuxZS9j62C/orhyWdNj9TaOfrMohyZ8YksovPlxCx5NYBhPoOEN6FEmRb+z22Qo8Kxgokm7Tasd/HUzsD2NzpaFEYUoPA1+BcdmJpS3uCyd3d5O8aJcSLYsn9+ETHiB/ptj+J5G25k7UkImzLgA97yPQIDAQABMA0GCSqGSIb3DQEBBQUAA4IBAQAI3b9x9dyhscC8qWvxf9bzNBF9hHfPu9vIMWTmO7dw4ynVlRo+td6oaVlHMO9fCvL4oArr1PlIAQYxPL0sDeaT1NzdC/HeICNfol8xbsFZI7bFwnoFm9faCM5Osa0FA8AQeyIRFj6/BDfbC9Nvb5srTw0ebfiFZdEnWFY4r2RH3CRTpq29gREx3sJ5nU44FZNmPjflm2hb+qHZi0E5Gya3o0MxP/Bkvwy+BctcnrRs/KS80B5+YfzPRwK4wPMFV5kWj5VouRnd37Zvq5oYnlqBn6YF+dLf6RX2HegxwTnVukRtek2BfwvayC7H9GffxqOrUBKzY3BqwYArw+v0pVhe");
        props.put("ssl.ca.location", "MIIDszCCApugAwIBAgIJAIjoMUp17jL6MA0GCSqGSIb3DQEBCwUAMHAxCzAJBgNVBAYTAk5OMQswCQYDVQQIDAJOTjELMAkGA1UEBwwCTk4xCzAJBgNVBAoMAk5OMQswCQYDVQQLDAJOTjEMMAoGA1UEAwwDenJyMR8wHQYJKoZIhvcNAQkBFhBrYWZrYV9jZW50ZXJAenJyMB4XDTE4MDQyODAyMTgyOFoXDTI4MDQyNTAyMTgyOFowcDELMAkGA1UEBhMCTk4xCzAJBgNVBAgMAk5OMQswCQYDVQQHDAJOTjELMAkGA1UECgwCTk4xCzAJBgNVBAsMAk5OMQwwCgYDVQQDDAN6cnIxHzAdBgkqhkiG9w0BCQEWEGthZmthX2NlbnRlckB6cnIwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDIZKUcIIpuE12LCTWTXLhr3snsVKXCUbA4XOpEB535wF1gWB7AkJMyj+syVthHZaobiDpJNgDGxrGa79uvKcu9UfMmR2RQlPxwwyaXx6xBnv4RVkT9P4UkjU4u70nP+jpe+vObVqHmwtYT6L/aoP751SJ3Siqbmw7XpmMJ3Il0eV4wL9QerKo4NOLr0/X4vK4EP6/gb9r/AucYbhiRi1MqswZAUi4RyqTTo7hWonY8B1loBNnP1DVtrfCxXd4pRthFs+Rt1w3f1JuTVs+G+J3LwNKNa0SA+jyI/eyyXlmO+Gv46JVcIb4aRq8rYJPgtXHJrytQdsODiB+9PaKLqJWPAgMBAAGjUDBOMB0GA1UdDgQWBBSOYgdV4PgwOW0QeM3aPIo5OGeHtzAfBgNVHSMEGDAWgBSOYgdV4PgwOW0QeM3aPIo5OGeHtzAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBv44QR6by/F1zAp2tdxfS552l2iAbMQYMWDPrhAwaU+khmXN9CS9o37HMDebH/3LBedKoISpratEv037PMXxqZ1C/xX3XqOK7VDMH5l+VAQnwlS7E+adJtzyui9fTk+FNqw+KFnUJwvI7TyPHPac1XFc9bPrIUSNsSUg+WXF+ZFTJwm/eqdh726vzs2XvmIG4ulCc8x54mGlmbPebIAWQhsvKiphXkD5VqY1A8R9tUZNZv0W7BIr4Ug76mV8Yd5dEfI8hELjBSKt2vTr+vNPBZ1ODTePcdtINSReqWYOg/7D3EJ6qwIfnh1qJ7FNKSz2VI25LWa1ED/7PV/JpSTyqV");

        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_SSL");
        props.put("ssl.keystore.type", "JKS");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"new_sale\" password=\"ns@hrb\";");
        return props;
    }
}
