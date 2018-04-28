package io.pravega.example.taxidemo.flinkprocessor;

import io.pravega.connectors.flink.util.FlinkPravegaParams;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationMain {

    private static Logger log = LoggerFactory.getLogger( ApplicationMain.class );
    private static final AppConfiguration appConfiguration = new AppConfiguration();

    public static void main(String... args) throws Exception {
        log.info("NETTY={}", io.netty.util.Version.identify());

        parseConfigurations(args);

        String runMode = appConfiguration.getRunMode();
        switch (runMode) {
            case AppConfiguration.RUN_MODE_RAW_DATA_TO_ELASTICSEARCH: {
                RawDataToElasticsearchJob job = new RawDataToElasticsearchJob(appConfiguration);
                job.run();
                break;
            }
            case AppConfiguration.RUN_MODE_EXTRACT_STATISTICS: {
                ExtractStatisticsJob job = new ExtractStatisticsJob(appConfiguration);
                job.run();
                break;
            }
            default: {
                printUsage();
                throw new IllegalArgumentException("invalid runMode");
            }
        }
    }

    private static void parseConfigurations(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        log.info("Parameter Tool: {}", params.toMap());

        appConfiguration.setRunMode(params.get("runMode", AppConfiguration.RUN_MODE_RAW_DATA_TO_ELASTICSEARCH));
        appConfiguration.setParallelism(params.getInt("job.parallelism", 1));
        appConfiguration.setCheckpointInterval(params.getLong("job.checkpointInterval", 10000));     // milliseconds
        appConfiguration.setDisableCheckpoint(params.getBoolean("job.disableCheckpoint", false));
        appConfiguration.setDisableOperatorChaining(params.getBoolean("job.disableOperatorChaining", false));
        appConfiguration.setEnableRebalance(params.getBoolean("rebalance", false));

        FlinkPravegaParams flinkPravegaParams = new FlinkPravegaParams(ParameterTool.fromArgs(args));
        appConfiguration.setFlinkPravegaParams(flinkPravegaParams);

        String scope = params.get("scope", "taxidemo");
        appConfiguration.getPravegaArgs().inputStream =
                flinkPravegaParams.getStreamFromParam("input.stream", scope + "/rawdata");

        appConfiguration.getPravegaArgs().targetRate = params.getInt("scaling.targetRate", 100000);  // Data rate in KB/sec
        appConfiguration.getPravegaArgs().scaleFactor = params.getInt("scaling.scaleFactor", 2);
        appConfiguration.getPravegaArgs().minNumSegments = params.getInt("scaling.minNumSegments", 12);

        AppConfiguration.ElasticSearch elasticSearch = appConfiguration.getElasticSearch();

        // elastic-sink: Whether to sink the results to Elastic Search or not.
        elasticSearch.setSinkResults(params.getBoolean("elastic-sink", false));

        elasticSearch.setDeleteIndex(params.getBoolean("elastic-delete-index", false));

        // elastic-host: Host of the Elastic instance to sink to.
        elasticSearch.setHost(params.get("elastic-host", "master.elastic.l4lb.thisdcos.directory"));

        // elastic-port: Port of the Elastic instance to sink to.
        elasticSearch.setPort(params.getInt("elastic-port", 9300));

        // elastic-cluster: The name of the Elastic cluster to sink to.
        elasticSearch.setCluster(params.get("elastic-cluster", "elastic"));

        // elastic-index: The name of the Elastic index to sink to.
        elasticSearch.setIndex(params.get("elastic-index", "taxidemo-rawdata"));

        // elastic-type: The name of the type to sink.
        elasticSearch.setType(params.get("elastic-type", "event"));
    }

    private static void printUsage() {
        final String usage = "java ApplicationMain " +
                "--runMode <annotate|decode|averagespeed|train|predict> " +
                "--controller <tcp://PRAVEGA_CONTROLLER_IP:9091/>";
        log.warn("Usage: {}", usage);
    }
}
