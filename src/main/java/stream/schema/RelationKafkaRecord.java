package stream.schema;

public class RelationKafkaRecord {
    private String topic;
    private int partition;
    private long offset;
    private long timestep;
    private String msg;

    @Override
    public String toString() {
        return "KafkaMsgDTO{" +
                "topic='" + topic + '\'' +
                "timestep='" + timestep + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", mesg='" + msg + '\'' +
                '}';
    }

    public RelationKafkaRecord() {}

    public RelationKafkaRecord(String msg, long timestep, String topic, int partition, long offset) {
        this.msg = msg;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestep = timestep;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getMesg() {
        return msg;
    }

    public void setMesg(String msg) {
        this.msg = msg;
    }

    public long getTimestep() {
        return timestep;
    }

    public void setTimestep(long timestep) {
        this.timestep = timestep;
    }
}
