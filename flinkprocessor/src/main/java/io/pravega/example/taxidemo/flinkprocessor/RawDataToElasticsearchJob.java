package io.pravega.example.taxidemo.flinkprocessor;

import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.serialization.UTF8StringDeserializationSchema;
import io.pravega.connectors.flink.util.StreamId;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RawDataToElasticsearchJob extends AbstractJob {

    private static Logger log = LoggerFactory.getLogger(RawDataToElasticsearchJob.class);

    public RawDataToElasticsearchJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() throws Exception {

        final String jobName = AppConfiguration.RUN_MODE_RAW_DATA_TO_ELASTICSEARCH;

        if (appConfiguration.getElasticSearch().isSinkResults()) {
            new ElasticSetup(appConfiguration.getElasticSearch()).run();
        }

        StreamId inputStreamId = pravegaArgs.inputStream;
        log.info("inputStreamId={}", inputStreamId);
//        createStream(inputStreamId);

        // Configure the Flink job environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set parallelism, etc.
        int parallelism = appConfiguration.getParallelism();
        if (parallelism > 0) {
            env.setParallelism(parallelism);
        }
        if (appConfiguration.isDisableOperatorChaining()) {
            env.disableOperatorChaining();
        }
        if(!appConfiguration.isDisableCheckpoint()) {
            long checkpointInterval = appConfiguration.getCheckpointInterval();
            env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        }
        log.info("Parallelism={}, MaxParallelism={}", env.getParallelism(), env.getMaxParallelism());

        long startTime = 0;
        FlinkPravegaReader<String> flinkPravegaReader = flinkPravegaParams.newReader(
                inputStreamId,
                startTime,
                new UTF8StringDeserializationSchema());

        DataStream<String> events = env
                .addSource(flinkPravegaReader)
                .name("EventReader");

        if (appConfiguration.isEnableRebalance()) {
            events = events.rebalance();
            log.info("Rebalancing events");
        }

        events.printToErr();

        if (appConfiguration.getElasticSearch().isSinkResults()) {
            ElasticsearchSink<String> elasticSink = sinkToElasticSearch();
            events.addSink(elasticSink).name("Write to ElasticSearch");
        }

        log.info("Executing {} job", jobName);
        env.execute(jobName);
    }

    private ElasticsearchSink sinkToElasticSearch() throws Exception {
        String host = appConfiguration.getElasticSearch().getHost();
        int port = appConfiguration.getElasticSearch().getPort();
        String cluster = appConfiguration.getElasticSearch().getCluster();
        String index = appConfiguration.getElasticSearch().getIndex();
        String type = appConfiguration.getElasticSearch().getType();

        Map<String, String> config = new HashMap<>();
        config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", cluster);
        config.put("client.transport.sniff", "false");

        List<InetSocketAddress> transports = new ArrayList<>();
        transports.add(new InetSocketAddress(InetAddress.getByName(host), port));

        return new ElasticsearchSink(config, transports, new ResultSinkFunction(index, type, appConfiguration.getElasticSearch()));
    }

    public static class ResultSinkFunction implements ElasticsearchSinkFunction<String> {
        private static final Logger LOG = LoggerFactory.getLogger(ResultSinkFunction.class);

        private final String index;
        private final String type;

        public ResultSinkFunction(String index, String type, AppConfiguration.ElasticSearch elasticConfig) {
            this.index = index;
            this.type = type;
        }

        @Override
        public void process(String event, RuntimeContext ctx, RequestIndexer indexer) {
            indexer.add(createIndexRequest(event));
        }

        private IndexRequest createIndexRequest(String event) {
            return Requests.indexRequest()
                .index(index)
                .type(type)
//                .id(id)
                .source(event);
        }
    }
}
