package stream.Test;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticSearchSinkDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<String> cols = new ArrayList<>();
        cols.add("a,b,c,d,e");

        DataStreamSource<String> colsStream = env.fromCollection(cols);
        //Map<String, String> json = new HashMap<>();

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("MacBookPro", 9200, "http"));
        //httpHosts.add(new HttpHost("localhost", 9200, "http"));

        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {
                        String[] split = element.split(",");
                        Map<String, String> json = new HashMap<>();
                        for(String item : split){
                            json.put(item, item+System.currentTimeMillis());
                        }

                        return Requests.indexRequest()
                                .index("flinkes")
                                .type("_doc")
                                .source(json);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

        //// provide a RestClientFactory for custom configuration on the internally created REST client
        //esSinkBuilder.setRestClientFactory(
        //        restClientBuilder -> {
        //            restClientBuilder.setDefaultHeaders(...)
        //            restClientBuilder.setMaxRetryTimeoutMillis(...)
        //            restClientBuilder.setPathPrefix(...)
        //            restClientBuilder.setHttpClientConfigCallback(...)
        //        }
        //);

        // finally, build and add the sink to the job's pipeline
        colsStream.addSink(esSinkBuilder.build());

        try{
            env.execute("EsSink");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
