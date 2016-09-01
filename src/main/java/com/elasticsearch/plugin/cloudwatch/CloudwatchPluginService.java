package com.elasticsearch.plugin.cloudwatch;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.internal.Profile;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.IndexService;

import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.jvm.JvmStats.GarbageCollector;
import org.elasticsearch.monitor.jvm.JvmStats.Mem;
import org.elasticsearch.monitor.jvm.JvmStats.Threads;
import org.elasticsearch.node.service.NodeService;
import com.amazonaws.util.EC2MetadataUtils;

import java.lang.reflect.Array;
import java.util.*;
import java.security.PrivilegedAction;
import java.security.AccessController;

public class CloudwatchPluginService extends AbstractLifecycleComponent<CloudwatchPluginService>  {
    private TimeValue frequency;
    private final IndicesService indicesService;
    private String clusterName;
    private Client client;
    private volatile Thread cloudwatchThread;
    private volatile boolean stopped;
    private NodeService nodeService;
    private AmazonCloudWatch cloudwatch;
    private boolean indexStatsEnabled;
    private boolean canStart = true;
    private String nodeName;
    private Date now;
    private AWSCredentialsProviderChain awsCredentialChain;
    private AWSCredentials awsCredentials;

    private Dimension clusterDimension;
    private ArrayList<Dimension> clusterDimensions = new ArrayList<Dimension>();

    private Dimension nodeDimension;
    private ArrayList<Dimension> nodeDimensions = new ArrayList<Dimension>();


    @Inject
    public CloudwatchPluginService(Settings settings, Client client,
                                   IndicesService indicesService, NodeService nodeService) {
        super(settings);

        this.client = client;
        this.nodeService = nodeService;
        this.indicesService = indicesService;

        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            public Void run() {
                initialize();
                return null;
            }
        });

    }

    private void initialize() {
        this.nodeName = nodeService.nodeName();
        logger.info("Using [{}] as the node name", this.nodeName);

        this.indexStatsEnabled = settings.getAsBoolean("metrics.cloudwatch.index_stats_enabled", false);
        logger.info("Reporting Index stats to CloudWatch [{}]", this.indexStatsEnabled);

        this.frequency = settings.getAsTime("metrics.cloudwatch.frequency", TimeValue.timeValueSeconds(20));
        logger.info("Cloudwatch reporting frequency [{}]", this.frequency);

        this.clusterName = settings.get("cluster.name");
        logger.info("Using cluster name [{}]", this.clusterName);

        this.clusterDimension = new Dimension().withName("Cluster").withValue(clusterName);
        this.clusterDimensions.add(clusterDimension);

        this.nodeDimension = new Dimension().withName("Node").withValue(nodeName);
        this.nodeDimensions.add(this.clusterDimension);
        this.nodeDimensions.add(this.nodeDimension);

        this.awsCredentialChain = new DefaultAWSCredentialsProviderChain();
        this.cloudwatch = new AmazonCloudWatchClient(this.awsCredentialChain);

        try {
            String regionName = EC2MetadataUtils.getEC2InstanceRegion();
            Region region  = Region.getRegion(Regions.fromName(regionName));
            logger.info("Detected CloudWatch region [{}]", region.getName());
            this.cloudwatch.setRegion(region);
        } catch (Exception e) {
            String regionName = settings.get("metrics.cloudwatch.region");
            Region region  = Region.getRegion(Regions.fromName(regionName));
            if (region == null) {
                logger.info("Using default region us-east-1");
                this.cloudwatch.setRegion(Region.getRegion(Regions.US_EAST_1));
            } else {
                logger.info("Using CloudWatch region from settings [{}]",regionName);
                this.cloudwatch.setRegion(region);
            }

        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        cloudwatchThread = EsExecutors.daemonThreadFactory(settings, "cloudwatch_poster")
                .newThread(new CloudwatchPoster());
        cloudwatchThread.start();
        logger.info("Sending metrics to CloudWatch every [{}]", frequency);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        if (stopped) {
            return;
        }
        if (cloudwatchThread != null) {
            cloudwatchThread.interrupt();
        }
        stopped = true;
        logger.info("Stopped sending CloudWatch metrics");
    }


    public class CloudwatchPoster implements Runnable {

        public void run() {
            while (!stopped) {
                if (canStart) {
                    try {
                        Thread.sleep(frequency.millis());
                    } catch (InterruptedException e1) {
                        continue;
                    }

                    logger.debug("Running CloudWatch plugin");

                    now = new Date();

                    sendClusterHealth();

                    NodeIndicesStats nodeIndicesStats = indicesService.stats(false);

                    NodeStats nodeStats = nodeService.stats(new CommonStatsFlags(), true, true, true, true, true, true, true, true, true);

                    //Node Index Stats
                    NodeIndicesStats indexStats = nodeStats.getIndices();
                    sendIndexingStats(indexStats.getIndexing().getTotal(), new ArrayList<Dimension>());
                    sendGetStats(indexStats.getGet(), new ArrayList<Dimension>());
                    sendSearchStats(indexStats.getSearch().getTotal(), new ArrayList<Dimension>());
                    sendMergeStats(indexStats.getMerge(), new ArrayList<Dimension>());
                    sendFieldDataStats(indexStats.getFieldData(), new ArrayList<Dimension>());
                    sendSegmentStats(indexStats.getSegments(), new ArrayList<Dimension>());
                    sendQueryCacheStats(indexStats.getQueryCache(), new ArrayList<Dimension>());
                    sendFlushStats(indexStats.getFlush(), new ArrayList<Dimension>());
                    sendStoreStats(indexStats.getStore(), new ArrayList<Dimension>() );

                    //Node JVM Stats
                    sendJVMStats(nodeStats.getJvm());

                    //Node Document Stats
                    sendDocsStats(nodeIndicesStats.getDocs());


                    if (indexStatsEnabled) {
                        sendIndexStats();
                    }

                } else {
                    logger.error("CloudWatch plugin not running due to previous error");
                    stopped = true;
                }

            }

        }

        private PutMetricDataRequest createMetricsRequest() {
            PutMetricDataRequest request = new PutMetricDataRequest();
            String namespace = settings.get("metrics.cloudwatch.namespace", "Elasticsearch");
            request.setNamespace(namespace);
            return request;
        }

        private void sendIndexingStats(IndexingStats.Stats stats, List<Dimension> dimensions) {
            try {
                PutMetricDataRequest request = createMetricsRequest();

                List<MetricDatum> data = new ArrayList<MetricDatum>();
                data.add(nodeDatum("IndexTotal",(double) stats.getIndexCount(),StandardUnit.Count,dimensions));
                data.add(nodeDatum("IndexTime",(double) stats.getIndexTimeInMillis(),StandardUnit.Milliseconds,dimensions));
                data.add(nodeDatum("IndexCurrent",(double) stats.getIndexCurrent(),StandardUnit.Count,dimensions));
                data.add(nodeDatum("DeleteTotal",(double) stats.getDeleteCount(),StandardUnit.Count,dimensions));
                data.add(nodeDatum("DeleteTime",(double) stats.getDeleteTimeInMillis(),StandardUnit.Milliseconds,dimensions));
                data.add(nodeDatum("DeleteCurrent",(double) stats.getIndexCount(),StandardUnit.Count,dimensions));
                request.setMetricData(data);
                cloudwatch.putMetricData(request);
            } catch (AmazonClientException e) {
                logger.error("Error sending index stats for node", e);
            }
        }

        private void sendGetStats(GetStats stats, List<Dimension> dimensions) {
            try {
                PutMetricDataRequest request = createMetricsRequest();

                List<MetricDatum> data = new ArrayList<MetricDatum>();
                data.add(nodeDatum("GetTotal",(double) stats.getCount(),StandardUnit.Count,dimensions));
                data.add(nodeDatum("GetTime",(double) stats.getTimeInMillis(),StandardUnit.Milliseconds,dimensions));
                data.add(nodeDatum("GetExistsTotal",(double) stats.getExistsCount(),StandardUnit.Count,dimensions));
                data.add(nodeDatum("GetExistsTime",(double) stats.getExistsTimeInMillis(),StandardUnit.Milliseconds,dimensions));
                data.add(nodeDatum("GetMissingTotal",(double) stats.getMissingCount(),StandardUnit.Count,dimensions));
                data.add(nodeDatum("GetMissingTime",(double) stats.getMissingTimeInMillis(),StandardUnit.Milliseconds,dimensions));
                data.add(nodeDatum("GetCurrent",(double) stats.current(),StandardUnit.Count,dimensions));

                request.setMetricData(data);
                cloudwatch.putMetricData(request);
            } catch (AmazonClientException e) {
                logger.error("Error sending get stats", e);
            }
        }

        private void sendSearchStats(SearchStats.Stats stats, List<Dimension> dimensions) {
            try {
                PutMetricDataRequest request = createMetricsRequest();

                List<MetricDatum> data = new ArrayList<MetricDatum>();
                data.add(nodeDatum("SearchQueryTotal",(double) stats.getQueryCount(),StandardUnit.Count, dimensions));
                data.add(nodeDatum("SearchQueryTime",(double) stats.getQueryTimeInMillis(),StandardUnit.Milliseconds, dimensions));
                data.add(nodeDatum("SearchQueryCurrent",(double) stats.getQueryCurrent(),StandardUnit.Count, dimensions));
                data.add(nodeDatum("SearchFetchTotal",(double) stats.getFetchCount(),StandardUnit.Count, dimensions));
                data.add(nodeDatum("SearchFetchTime",(double) stats.getFetchTimeInMillis(),StandardUnit.Milliseconds, dimensions));
                data.add(nodeDatum("SearchCurrent",(double) stats.getFetchCurrent(),StandardUnit.Count, dimensions));
                data.add(nodeDatum("SearchScrollTotal",(double) stats.getScrollCount(),StandardUnit.Count, dimensions));
                data.add(nodeDatum("SearchScrollTime",(double) stats.getScrollTimeInMillis(),StandardUnit.Milliseconds, dimensions));
                data.add(nodeDatum("SearchScrollCurrent",(double) stats.getScrollCurrent(),StandardUnit.Count, dimensions));

                request.setMetricData(data);
                cloudwatch.putMetricData(request);
            } catch (AmazonClientException e) {
                logger.error("Error sending search stats", e);
            }
        }

        private void sendMergeStats(MergeStats stats, List<Dimension> dimensions) {
            try {
                PutMetricDataRequest request = createMetricsRequest();

                List<MetricDatum> data = new ArrayList<MetricDatum>();
                data.add(nodeDatum("MergeCurrent",(double) stats.getCurrent(),StandardUnit.Count, dimensions));
                data.add(nodeDatum("MergeCurrentDocs",(double) stats.getCurrentNumDocs(),StandardUnit.Count, dimensions));
                data.add(nodeDatum("MergeCurrentSize",(double) stats.getCurrentSizeInBytes(),StandardUnit.Bytes, dimensions));
                data.add(nodeDatum("MergeTotal",(double) stats.getTotal(),StandardUnit.Count, dimensions));
                data.add(nodeDatum("MergeTime",(double) stats.getTotalTimeInMillis(),StandardUnit.Milliseconds, dimensions));
                data.add(nodeDatum("MergeDocs",(double) stats.getTotalNumDocs(),StandardUnit.Count, dimensions));
                data.add(nodeDatum("MergeSize",(double) stats.getTotalSizeInBytes(),StandardUnit.Bytes, dimensions));

                request.setMetricData(data);
                cloudwatch.putMetricData(request);
            } catch (AmazonClientException e) {
                logger.error("Error sending merge stats", e);
            }
        }

        private void sendFieldDataStats(FieldDataStats stats, List<Dimension> dimensions) {
            try {
                PutMetricDataRequest request = createMetricsRequest();

                List<MetricDatum> data = new ArrayList<MetricDatum>();
                data.add(nodeDatum("FieldDataEviction",(double) stats.getEvictions(),StandardUnit.Count,dimensions));
                data.add(nodeDatum("FieldDataMemory",(double) stats.getMemorySizeInBytes(),StandardUnit.Bytes, dimensions));

                request.setMetricData(data);
                cloudwatch.putMetricData(request);
            } catch (AmazonClientException e) {
                logger.error("Error sending field data stats", e);
            }
        }

        private void sendSegmentStats(SegmentsStats stats, List<Dimension> dimensions) {
            try {
                PutMetricDataRequest request = createMetricsRequest();

                List<MetricDatum> data = new ArrayList<MetricDatum>();
                data.add(nodeDatum("SegementCount",(double) stats.getCount(),StandardUnit.Count, dimensions));
                data.add(nodeDatum("SegmentBitsetMemory",(double) stats.getBitsetMemoryInBytes(),StandardUnit.Bytes, dimensions));
                data.add(nodeDatum("SegmentDocValuesMemory",(double) stats.getDocValuesMemoryInBytes(),StandardUnit.Bytes, dimensions));
                data.add(nodeDatum("SegmentIndexWriterMemory",(double) stats.getIndexWriterMemoryInBytes(),StandardUnit.Bytes, dimensions));
                data.add(nodeDatum("SegmentNormsMemory",(double) stats.getNormsMemoryInBytes(),StandardUnit.Bytes, dimensions));
                data.add(nodeDatum("SegmentStoredFieldsMemory",(double) stats.getStoredFieldsMemoryInBytes(),StandardUnit.Bytes, dimensions));
                data.add(nodeDatum("SegmentTermMemory",(double) stats.getTermsMemoryInBytes(),StandardUnit.Bytes, dimensions));
                data.add(nodeDatum("SegmentVectorsMemory",(double) stats.getTermVectorsMemoryInBytes(),StandardUnit.Bytes, dimensions));
                data.add(nodeDatum("SegmentVersionMemory",(double) stats.getVersionMapMemoryInBytes(),StandardUnit.Bytes, dimensions));
                data.add(nodeDatum("SegmentMemory",(double) stats.getMemoryInBytes(),StandardUnit.Bytes, dimensions));

                request.setMetricData(data);
                cloudwatch.putMetricData(request);
            } catch (AmazonClientException e) {
                logger.error("Error sending segment stats", e);
            }
        }

        private void sendQueryCacheStats(QueryCacheStats stats, List<Dimension> dimensions) {
            try {
                PutMetricDataRequest request = createMetricsRequest();

                List<MetricDatum> data = new ArrayList<MetricDatum>();
                data.add(nodeDatum("QueryCacheMemory",(double) stats.getMemorySizeInBytes(),StandardUnit.Count, dimensions));
                data.add(nodeDatum("QueryCacheEvictions",(double) stats.getEvictions(),StandardUnit.Count, dimensions));
                data.add(nodeDatum("QueryCacheCount",(double) stats.getTotalCount(),StandardUnit.Count, dimensions));
                data.add(nodeDatum("QueryCacheHitCount",(double) stats.getHitCount(),StandardUnit.Count, dimensions));
                data.add(nodeDatum("QueryCacheMissCount",(double) stats.getMissCount(),StandardUnit.Count, dimensions));

                request.setMetricData(data);
                cloudwatch.putMetricData(request);
            } catch (AmazonClientException e) {
                logger.error("Error sending query cache stats", e);
            }
        }

        private void sendFlushStats(FlushStats stats, List<Dimension> dimensions) {
            try {
                PutMetricDataRequest request = createMetricsRequest();

                List<MetricDatum> data = new ArrayList<MetricDatum>();
                data.add(nodeDatum("FlushTotal",(double) stats.getTotal(),StandardUnit.Count));
                data.add(nodeDatum("FlushTime",(double) stats.getTotalTimeInMillis(),StandardUnit.Milliseconds));

                request.setMetricData(data);
                cloudwatch.putMetricData(request);
            } catch (AmazonClientException e) {
                logger.error("Error sending flush stats", e);
            }
        }

        private void sendStoreStats(StoreStats stats, List<Dimension> dimensions) {
            try {
                PutMetricDataRequest request = createMetricsRequest();

                List<MetricDatum> data = new ArrayList<MetricDatum>();
                data.add(nodeDatum("StoreSize",(double) stats.getSizeInBytes(),StandardUnit.Bytes, dimensions));

                request.setMetricData(data);
                cloudwatch.putMetricData(request);
            } catch (AmazonClientException e) {
                logger.error("Error sending store stats", e);
            }
        }

        private void sendClusterHealth() {
            org.elasticsearch.client.ClusterAdminClient cluster = client.admin().cluster();
            cluster.health(new ClusterHealthRequest(), new ActionListener<ClusterHealthResponse>() {
                public void onResponse(ClusterHealthResponse healthResponse) {

                    PutMetricDataRequest request = createMetricsRequest();

                    List<MetricDatum> data = new ArrayList<MetricDatum>();
                    data.add(clusterDatum("ActivePrimaryShards", (double) healthResponse.getActivePrimaryShards(),StandardUnit.Count));
                    data.add(clusterDatum("ActiveShards", (double) healthResponse.getActiveShards(),StandardUnit.Count));
                    data.add(clusterDatum("InitializingShards", (double) healthResponse.getInitializingShards(),StandardUnit.Count));
                    data.add(clusterDatum("DataNodes", (double) healthResponse.getNumberOfDataNodes(),StandardUnit.Count));
                    data.add(clusterDatum("Nodes", (double) healthResponse.getNumberOfNodes(),StandardUnit.Count));
                    data.add(clusterDatum("PendingTasks", (double) healthResponse.getNumberOfPendingTasks(),StandardUnit.Count));
                    data.add(clusterDatum("RelocatingShards", (double) healthResponse.getRelocatingShards(),StandardUnit.Count));
                    data.add(clusterDatum("UnassignedShards", (double) healthResponse.getUnassignedShards(),StandardUnit.Count));
                    data.add(clusterDatum("InFlightFetch", (double) healthResponse.getNumberOfInFlightFetch(),StandardUnit.Count));

                    request.setMetricData(data);
                    cloudwatch.putMetricData(request);

                }

                public void onFailure(Throwable e) {
                    logger.error("Asking for cluster health failed.", e);
                }
            });
        }

        private void sendDocsStats(DocsStats docStats) {
            try {
                PutMetricDataRequest request = createMetricsRequest();

                List<MetricDatum> data = new ArrayList<MetricDatum>();
                data.add(nodeDatum("DocsCount", docStats.getCount(), StandardUnit.Count));
                data.add(nodeDatum("DocsDeleted", docStats.getDeleted(), StandardUnit.Count));

                request.setMetricData(data);
                cloudwatch.putMetricData(request);
            } catch (AmazonClientException e) {
                logger.error("Exception thrown by amazon while sending DocsStats", e);
            }
        }

        private void sendJVMStats( JvmStats jvmStats) {

            try {
                PutMetricDataRequest request = createMetricsRequest();

                List<MetricDatum> data = new ArrayList<MetricDatum>();
                data.add(nodeDatum("Uptime", jvmStats.getUptime().seconds(), StandardUnit.Seconds));

                Mem mem = jvmStats.getMem();
                data.add(nodeDatum("JVMHeapCommittedBytes", mem.getHeapCommitted().bytes(), StandardUnit.Bytes));
                data.add(nodeDatum("JVMHeapUsedBytes", mem.getHeapUsed().bytes(), StandardUnit.Bytes));
                data.add(nodeDatum("JVMNonHeapCommittedBytes", mem.getNonHeapCommitted().bytes(), StandardUnit.Bytes));
                data.add(nodeDatum("JVMNonHeapUsedBytes", mem.getNonHeapUsed().bytes(), StandardUnit.Bytes));
                data.add(nodeDatum("JVMHeapUsedPercent", mem.getHeapUsedPercent(), StandardUnit.Percent));


                Threads threads = jvmStats.getThreads();
                data.add(nodeDatum("JVMThreads", threads.getCount(), StandardUnit.Count));
                data.add(nodeDatum("JVMThreadsPeak", threads.getPeakCount(), StandardUnit.Count));

                // garbage collectors
                Iterator<GarbageCollector> gcs = jvmStats.getGc().iterator();
                long collectionCount = 0;
                long collectionTime = 0;
                while (gcs.hasNext()) {
                    GarbageCollector gc = gcs.next();
                    String name = gc.getName();
                    name = name.substring(0, 1).toUpperCase(Locale.ENGLISH) + name.substring(1);
                    data.add(nodeDatum("GCCollection" + name + "Count", collectionCount, StandardUnit.Count));
                    data.add(nodeDatum("GCCollection" + name + "Time", collectionTime, StandardUnit.Seconds));

                    collectionCount += gc.getCollectionCount();
                    collectionTime += gc.getCollectionTime().seconds();
                }

                data.add(nodeDatum("GCCollectionTotalCount", collectionCount, StandardUnit.Count));
                data.add(nodeDatum("GCCollectionTotalTime", collectionTime, StandardUnit.Seconds));

                request.setMetricData(data);
                cloudwatch.putMetricData(request);
            } catch (AmazonClientException e) {
                logger.error("Error sending JVM Stats", e);
            }
        }

        private void sendIndexStats() {
                PutMetricDataRequest request = createMetricsRequest();

                for( IndexService indexService : indicesService) {
                    for( IndexShard indexShard : indexService) {
                        List<Dimension> dimensions = new ArrayList<Dimension>();
                        dimensions.add(new Dimension().withName("IndexName").withValue(indexShard.shardId().index().name()));
                        dimensions.add(new Dimension().withName("ShardId").withValue(indexShard.shardId().id() + ""));
                        sendStoreStats(indexShard.storeStats(), dimensions);
                        sendMergeStats(indexShard.mergeStats(),dimensions);
                        sendIndexingStats(indexShard.indexingStats().getTotal(),dimensions);
                        sendGetStats(indexShard.getStats(),dimensions);
                        sendSearchStats(indexShard.searchStats().getTotal(),dimensions);
                        sendFieldDataStats(indexShard.fieldDataStats(),dimensions);
                        sendSegmentStats(indexShard.segmentStats(), dimensions);
                        sendQueryCacheStats(indexShard.queryCacheStats(), dimensions);
                        sendFlushStats(indexShard.flushStats(), dimensions);
                    }

                }


        }

        private MetricDatum nodeDatum(String metricName, double metricValue, StandardUnit unit, List<Dimension> dimensions) {
            List<Dimension> currDim;
            currDim = new ArrayList<Dimension>(nodeDimensions);
            currDim.addAll(dimensions);

            return datum(metricName,metricValue,unit,currDim);
        }

        private MetricDatum nodeDatum(String metricName, double metricValue, StandardUnit unit) {

            return nodeDatum(metricName, metricValue, unit, new ArrayList<Dimension>());
        }

        private MetricDatum clusterDatum(String metricName, double metricValue, StandardUnit unit) {
            return datum(metricName,metricValue,unit,clusterDimensions);
        }

        private MetricDatum datum(String metricName, double metricValue, StandardUnit unit, List<Dimension> dimensions ) {
            MetricDatum datum = new MetricDatum();
            datum.setDimensions(dimensions);
            datum.setMetricName(metricName);
            datum.setTimestamp(now);
            datum.setValue(metricValue);
            datum.setUnit(unit);
            return datum;
        }


    }
}
