package io.pravega.example.taxidemo.flinkprocessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.serialization.JsonDeserializationSchema;
import io.pravega.connectors.flink.util.StreamId;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
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

public class ExtractStatisticsJob extends AbstractJob {

    private static Logger log = LoggerFactory.getLogger(ExtractStatisticsJob.class);

    public ExtractStatisticsJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() throws Exception {

        final String jobName = AppConfiguration.RUN_MODE_EXTRACT_STATISTICS;

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

        // We can't use MemoryStateBackend because it can't store our large state.
        if (env.getStateBackend() == null || env.getStateBackend() instanceof MemoryStateBackend) {
            log.warn("Using FsStateBackend");
            env.setStateBackend(new FsStateBackend("file:///tmp/flink-state", true));
        }

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        long startTime = 0;
        FlinkPravegaReader<RawData> flinkPravegaReader = flinkPravegaParams.newReader(
                inputStreamId,
                startTime,
                new JsonDeserializationSchema<>(RawData.class));

        DataStream<RawData> events = env
                .addSource(flinkPravegaReader)
                .name("EventReader");

        if (appConfiguration.isEnableRebalance()) {
            events = events.rebalance();
            log.info("Rebalancing events");
        }

        events = events.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<RawData>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(RawData element) {
                return element.timestamp;
            }
        });

        events = events.filter((FilterFunction<RawData>) value -> {
//            return value.trip_id.equals("517da645-9856-4592-9035-a04ddda00040");
//            return value.trip_duration_minutes > 10.0;
            return true;
//            return value.event_type.equals("dropoff") && value.tip_amount > 0.0;
        });

//        events.printToErr();

        DataStream<RawDataAggregator.Result> aggEvents = events
            .keyBy("trip_id")
            .window(GlobalWindows.create())
            .trigger(CountTrigger.of(1))
            .aggregate(new RawDataAggregator.AggregateFunction());

//        aggEvents.printToErr();

        aggEvents = aggEvents.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<RawDataAggregator.Result>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(RawDataAggregator.Result element) {
                        return element.timestamp;
                    }
                });

        DataStream<StatisticsAggregator.Result> aggEvents2 = aggEvents
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(15)))
            .aggregate(new StatisticsAggregator.AggregateFunction(), new StatisticsAggregator.AllWindowFunction());
        aggEvents2.printToErr();

        if (appConfiguration.getElasticSearch().isSinkResults()) {
            ElasticsearchSink<StatisticsAggregator.Result> elasticSink = sinkToElasticSearch();
            aggEvents2.addSink(elasticSink).name("Write to ElasticSearch");
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

    public static class ResultSinkFunction implements ElasticsearchSinkFunction<StatisticsAggregator.Result> {
        private static final Logger LOG = LoggerFactory.getLogger(ResultSinkFunction.class);

        private final String index;
        private final String type;
        private final ObjectMapper objectMapper = new ObjectMapper();

        public ResultSinkFunction(String index, String type, AppConfiguration.ElasticSearch elasticConfig) {
            this.index = index;
            this.type = type;
        }

        @Override
        public void process(StatisticsAggregator.Result event, RuntimeContext ctx, RequestIndexer indexer) {
            indexer.add(createIndexRequest(event));
        }

        private IndexRequest createIndexRequest(StatisticsAggregator.Result event) {
            String source = null;
            try {
                source = objectMapper.writeValueAsString(event);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                return null;
            }
            String id = String.format("%d", event.timestamp);
            return Requests.indexRequest()
                .index(index)
                .type(type)
                .id(id)
                .source(source);
        }
    }
}
