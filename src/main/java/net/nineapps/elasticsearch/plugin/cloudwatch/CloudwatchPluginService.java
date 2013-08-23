package net.nineapps.elasticsearch.plugin.cloudwatch;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.status.DocsStatus;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.node.service.NodeService;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;

public class CloudwatchPluginService extends AbstractLifecycleComponent<CloudwatchPluginService> {
	private Client client;
    private volatile Thread cloudwatchThread;
    private volatile boolean stopped;
    private final TimeValue frequency;
    private final IndicesService indicesService;
    private NodeService nodeService;
    private AWSCredentials awsCredentials;
    private AmazonCloudWatch cloudwatch;
    private final String clusterName;
    private boolean indexStatsEnabled;
	private boolean osStatsEnabled;
	private boolean jvmStatsEnabled;
	
	@Inject
	public CloudwatchPluginService(Settings settings, Client client,
			IndicesService indicesService, NodeService nodeService) {
		super(settings);
		this.client = client;
        this.nodeService = nodeService;
        this.indicesService = indicesService;
        String accessKey = settings.get("metrics.cloudwatch.aws.access_key");
        String secretKey = settings.get("metrics.cloudwatch.aws.secret_key");
        awsCredentials = new BasicAWSCredentials(accessKey, secretKey);

        indexStatsEnabled = settings.getAsBoolean("metrics.cloudwatch.index_stats_enabled", false);
        osStatsEnabled = settings.getAsBoolean("metrics.cloudwatch.os_stats_enabled", false);
        jvmStatsEnabled = settings.getAsBoolean("metrics.cloudwatch.jvm_stats_enabled", true);
        
        String region = settings.get("metrics.cloudwatch.aws.region");
        logger.info("configured region is [{}]",region);
        cloudwatch = cloudwatchClient(region);
        
        frequency = settings.getAsTime("metrics.cloudwatch.frequency", TimeValue.timeValueMinutes(1));

        clusterName = settings.get("cluster.name");
//        logger.info("cluster name is [{}]", clusterName);
	}

	@Override
	protected void doClose() throws ElasticSearchException {		
	}

	@Override
	protected void doStart() throws ElasticSearchException {
        cloudwatchThread = EsExecutors.daemonThreadFactory(settings, "cloudwatch_poster")
        		.newThread(new CloudwatchPoster());
        cloudwatchThread.start();
        logger.info("Cloudwatch reporting triggered every [{}]", frequency);
	}

	@Override
	protected void doStop() throws ElasticSearchException {
        if (stopped) {
            return;
        }
        if (cloudwatchThread != null) {
            cloudwatchThread.interrupt();
        }
        stopped = true;
        logger.info("Cloudwatch poster stopped");
	}
	
	public class CloudwatchPoster implements Runnable {

		public void run() {
            while (!stopped) {
            	
            	logger.info("Running Cloudwatch plugin tasks");

            	final Date now = new Date();
            	
            	NodeIndicesStats nodeIndicesStats = indicesService.stats(false);
            	
            	client.admin().cluster().health(new ClusterHealthRequest(), new ActionListener<ClusterHealthResponse>() {
					public void onResponse(ClusterHealthResponse healthResponse) {

						logger.info("cluster name is [{}]", healthResponse.getClusterName());
						
						PutMetricDataRequest request = new PutMetricDataRequest();
						request.setNamespace("9apps/Elasticsearch");
						
						List<MetricDatum> data = Lists.newArrayList(); 
						data.add(clusterDatum(now, "ClusterStatus", (double) healthResponse.getStatus().value()));
						data.add(clusterDatum(now, "NumberOfNodes", (double) healthResponse.getNumberOfNodes()));
						data.add(clusterDatum(now, "NumberOfDataNodes", (double) healthResponse.getNumberOfDataNodes()));
						data.add(clusterDatum(now, "ActivePrimaryShards", (double) healthResponse.getActivePrimaryShards()));
						data.add(clusterDatum(now, "ActiveShards", (double) healthResponse.getActiveShards()));
						data.add(clusterDatum(now, "RelocatingShards", (double) healthResponse.getRelocatingShards()));
						data.add(clusterDatum(now, "InitializingShards", (double) healthResponse.getInitializingShards()));
						data.add(clusterDatum(now, "UnassignedShards", (double) healthResponse.getUnassignedShards()));

						request.setMetricData(data);
						cloudwatch.putMetricData(request);
						
					}
					
					public void onFailure(Throwable e) {
						logger.error("Asking for cluster health failed.", e);
					}
				});

//                logger.info("node attributes is [{}]", nodeService.attributes());
                NodeStats nodeStats = nodeService.stats(new CommonStatsFlags().clear(), true, false, true, false, false, false, false, false);

                String nodeAddress = nodeService.attributes().get("http_address");
                if (nodeAddress != null) {
                	// it might take a little time until http_address is added to the attributes
                	// if it's still not there, we skip the node metrics this time
//	                logger.info("node name is [{}]", nodeAddress);
	                
                	if(osStatsEnabled){
                		sendOsStats(now, nodeStats, nodeAddress);
                	}

                	if(jvmStatsEnabled){
                		sendJVMStats(now, nodeStats, nodeAddress);
                	}

	    			sendDocsStats(now, nodeAddress, nodeIndicesStats);

	    			if (indexStatsEnabled) {
	                    sendIndexStats(now, nodeAddress);
	    		    }
	    			
	    			
	    			// Most stats we copied from this plugin, selecting the ones that make sense for us: https://github.com/spinscale/elasticsearch-graphite-plugin/blob/master/src/main/java/org/elasticsearch/service/graphite/GraphiteService.java
	    			
                } else {
                	logger.warn("Node attribute http_address still not set, skipping node metrics.");
                }
            	
                try {
                    Thread.sleep(frequency.millis());
                } catch (InterruptedException e1) {
                    continue;
                }
            }
		}

		private void sendDocsStats(final Date now, String nodeAddress, NodeIndicesStats nodeIndicesStats) {
			try {
				PutMetricDataRequest request = new PutMetricDataRequest();
				request.setNamespace("9apps/Elasticsearch");
				List<MetricDatum> docsData = Lists.newArrayList();
				DocsStats docsStats = nodeIndicesStats.getDocs();
				long count = ( docsStats != null ? docsStats.getCount() : 0 );
				long deleted = ( docsStats != null ? docsStats.getDeleted() : 0 );
				docsData.add(nodeDatum(now, nodeAddress, "DocsCount", count, StandardUnit.Count));
				docsData.add(nodeDatum(now, nodeAddress, "DocsDeleted", deleted, StandardUnit.Count));
				
				request.setMetricData(docsData);
				cloudwatch.putMetricData(request);
    		} catch (AmazonClientException e) {
    			logger.error("Exception thrown by amazon while sending DocsStats", e);
    		}
		}

		private void sendJVMStats(final Date now, NodeStats nodeStats,
				String nodeAddress) {

			try {
				PutMetricDataRequest request = new PutMetricDataRequest();
				request.setNamespace("9apps/Elasticsearch");
	
				JvmStats jvmStats = nodeStats.getJvm();
				List<MetricDatum> jvmData = Lists.newArrayList();
				jvmData.add(nodeDatum(now, nodeAddress, "JVMUptime", jvmStats.uptime().seconds(), StandardUnit.Seconds));
	
				// mem
				jvmData.add(nodeDatum(now, nodeAddress, "JVMMemHeapCommitted", jvmStats.mem().heapCommitted().bytes(), StandardUnit.Bytes));
				jvmData.add(nodeDatum(now, nodeAddress, "JVMMemHeapUsed", jvmStats.mem().heapUsed().bytes(), StandardUnit.Bytes));
				jvmData.add(nodeDatum(now, nodeAddress, "JVMMemNonHeapCommitted", jvmStats.mem().nonHeapCommitted().bytes(), StandardUnit.Bytes));
				jvmData.add(nodeDatum(now, nodeAddress, "JVMMemNonHeapUsed", jvmStats.mem().nonHeapUsed().bytes(), StandardUnit.Bytes));
	
				// threads
				jvmData.add(nodeDatum(now, nodeAddress, "JVMThreads", jvmStats.threads().count(), StandardUnit.Count));
				jvmData.add(nodeDatum(now, nodeAddress, "JVMThreadPeak", jvmStats.threads().peakCount(), StandardUnit.Count));
	
				// garbage collectors
				jvmData.add(nodeDatum(now, nodeAddress, "JVMGCCollectionCount", jvmStats.gc().collectionCount(), StandardUnit.Count));
				jvmData.add(nodeDatum(now, nodeAddress, "JVMGCCollectionTime", jvmStats.gc().collectionTime().seconds(), StandardUnit.Seconds));

				request.setMetricData(jvmData);
				cloudwatch.putMetricData(request);
    		} catch (AmazonClientException e) {
    			logger.error("Exception thrown by amazon while sending JVMStats", e);
    		}
		}

		private void sendOsStats(final Date now,
				NodeStats nodeStats, String nodeAddress) {
			
			try {
				PutMetricDataRequest request = new PutMetricDataRequest();
				request.setNamespace("9apps/Elasticsearch");
	
				OsStats osStats = nodeStats.getOs();
				
				List<MetricDatum> osData = Lists.newArrayList();
				
//				logger.info("Getting os stats for this node");
				osData.add(nodeDatum(now, nodeAddress, "OsCpuSys", osStats.cpu().sys(), StandardUnit.Percent));
				osData.add(nodeDatum(now, nodeAddress, "OsCpuIdle", osStats.cpu().idle(), StandardUnit.Percent));
				osData.add(nodeDatum(now, nodeAddress, "OsCpuUser", osStats.cpu().user(), StandardUnit.Percent));
	
				osData.add(nodeDatum(now, nodeAddress, "OsMemFreeBytes", osStats.mem().free().bytes(), StandardUnit.Bytes));
				osData.add(nodeDatum(now, nodeAddress, "OsMemUsedBytes", osStats.mem().used().bytes(), StandardUnit.Bytes));
				osData.add(nodeDatum(now, nodeAddress, "OsMemFreePercent", osStats.mem().freePercent(), StandardUnit.Percent));
				osData.add(nodeDatum(now, nodeAddress, "OsMemUsedPercent", osStats.mem().usedPercent(), StandardUnit.Percent));
				osData.add(nodeDatum(now, nodeAddress, "OsMemActualFreeBytes", osStats.mem().actualFree().bytes(), StandardUnit.Bytes));
				osData.add(nodeDatum(now, nodeAddress, "OsMemActualUsedBytes", osStats.mem().actualUsed().bytes(), StandardUnit.Bytes));
				    
				osData.add(nodeDatum(now, nodeAddress, "OsSwapFreeBytes", osStats.swap().free().bytes(), StandardUnit.Bytes));
				osData.add(nodeDatum(now, nodeAddress, "OsSwapUsedBytes", osStats.swap().used().bytes(), StandardUnit.Bytes));
	      
				request.setMetricData(osData);
				cloudwatch.putMetricData(request);
    		} catch (AmazonClientException e) {
    			logger.error("Exception thrown by amazon while sending OsStats", e);
    		}
		}
		private void sendIndexStats(final Date now, String nodeAddress) {
			try {
				List<MetricDatum> data = Lists.newArrayList();
				Set<String> indices = indicesService.indices();
				for(String index : indices){
					//Reset request per shard
					PutMetricDataRequest request = new PutMetricDataRequest();
					request.setNamespace("9apps/Elasticsearch");
				
					IndicesStatusResponse response = client.admin().indices().prepareStatus(index).setSnapshot(true).execute().actionGet();
					
					List<Dimension> dimensions = new ArrayList<Dimension>();
				    dimensions.add(new Dimension().withName("IndexName").withValue(index));
				    
				    data.add(nodeDatum(now, nodeAddress, "FailedShards", response.getFailedShards(), StandardUnit.Count, dimensions));
				    data.add(nodeDatum(now, nodeAddress, "SuccessfulShards", response.getSuccessfulShards(), StandardUnit.Count, dimensions));
				    data.add(nodeDatum(now, nodeAddress, "TotalShards", response.getTotalShards(), StandardUnit.Count, dimensions));
				    DocsStatus docsStats = response.getIndex(index).getDocs();
				    long count = ( docsStats != null ? docsStats.getNumDocs() : 0 );
				    data.add(nodeDatum(now, nodeAddress, "DocsCount", count, StandardUnit.Count, dimensions));
					long deleted = ( docsStats != null ? docsStats.getDeletedDocs() : 0 );
				    data.add(nodeDatum(now, nodeAddress, "DeletedDocsCount", deleted, StandardUnit.Count, dimensions));
					request.setMetricData(data);
					cloudwatch.putMetricData(request);
				}
	    	} catch (AmazonClientException e) {
	    			logger.error("Exception thrown by amazon while sending IndexStats", e);
	    	}
		}

		private void sendIndexStatsPerShard(final Date now, String nodeAddress) {
			try {
				List<MetricDatum> data = Lists.newArrayList();
				List<IndexShard> indexShards = getIndexShards(indicesService);
				for (IndexShard indexShard : indexShards) {
					//Reset request per shard
					PutMetricDataRequest request = new PutMetricDataRequest();
					request.setNamespace("9apps/Elasticsearch");
					
					List<Dimension> dimensions = new ArrayList<Dimension>();
				    dimensions.add(new Dimension().withName("IndexName").withValue(indexShard.shardId().index().name()));
				    dimensions.add(new Dimension().withName("ShardId").withValue(indexShard.shardId().id() + ""));
				    
					// docs stats
				    DocsStats docsStats = indexShard.docStats();
					long count = ( docsStats != null ? docsStats.getCount() : 0 );
				    data.add(nodeDatum(now, nodeAddress, "DocsCount", count, StandardUnit.Count, dimensions));
				    
					long deleted = ( docsStats != null ? docsStats.getDeleted() : 0 );
	    	        data.add(nodeDatum(now, nodeAddress, "DocsDeleted", deleted, StandardUnit.Count, dimensions));
		
	    	        // store stats
	    	        StoreStats storeStats = indexShard.storeStats();
	    	        
	    	        data.add(nodeDatum(now, nodeAddress, "StoreSize", storeStats.sizeInBytes(), StandardUnit.Bytes, dimensions));
	    	        data.add(nodeDatum(now, nodeAddress, "StoreThrottleTimeInNanos", storeStats.throttleTime().getNanos(), StandardUnit.None, dimensions));

					request.setMetricData(data);
					cloudwatch.putMetricData(request);
				}
	    	} catch (AmazonClientException e) {
	    			logger.error("Exception thrown by amazon while sending IndexStats", e);
	    	}
		}

		private MetricDatum nodeDatum(final Date timestamp, String nodeAddress, 
				String metricName, double metricValue, StandardUnit unit, List<Dimension> dimensions) {
			MetricDatum datum = new MetricDatum();
			Dimension clusterNameDimension = new Dimension();
			clusterNameDimension.setName("ClusterName");
			clusterNameDimension.setValue(clusterName);
			Dimension nodeNameDimension = new Dimension();
			nodeNameDimension.setName("NodeName");
			nodeNameDimension.setValue(nodeAddress);
			datum.setDimensions(Lists.newArrayList(clusterNameDimension, nodeNameDimension));
			datum.getDimensions().addAll(dimensions);
			datum.setMetricName(metricName);
			datum.setTimestamp(timestamp);
			datum.setValue(metricValue);
			datum.setUnit(unit);
			return datum;
		}

		private MetricDatum nodeDatum(final Date timestamp, String nodeAddress, 
				String metricName, double metricValue, StandardUnit unit) {
			
			return nodeDatum(timestamp, nodeAddress, metricName, metricValue, unit, new ArrayList<Dimension>());
		}
		
		private MetricDatum clusterDatum(final Date now, String metricName, double metricValue) {
			MetricDatum datum = new MetricDatum();
			Dimension dimension = new Dimension();
			dimension.setName("ClusterName");
			dimension.setValue(clusterName);
			datum.setDimensions(Lists.newArrayList(dimension));
			datum.setMetricName(metricName);
			datum.setTimestamp(now);
			datum.setValue(metricValue);
			datum.setUnit(StandardUnit.Count);
			return datum;
		}
		
	}
	
	private String cloudwatchEndpoint(String region) {
		if ("us-east-1".equals(region) || "us-west-2".equals(region)
				|| "us-west-1".equals(region) || "eu-west-1".equals(region)
				|| "ap-southeast-1".equals(region) || "ap-southeast-2".equals(region)
				|| "ap-northeast-1".equals(region) || "sa-east-1".equals(region)) {
			return "monitoring." + region + ".amazonaws.com";
		} else {
			logger.warn("Unrecognized region [{}], using the default, us-east-1", region);
			return "monitoring.us-east-1.amazonaws.com";
		}
	}

	private AmazonCloudWatch cloudwatchClient(String region) {
		String cloudwatchEndpoint = cloudwatchEndpoint(region);
		final AmazonCloudWatch cloudwatch = new AmazonCloudWatchClient(awsCredentials);
		cloudwatch.setEndpoint(cloudwatchEndpoint);
		return cloudwatch;
	}

    private List<IndexShard> getIndexShards(IndicesService indicesService) {
        List<IndexShard> indexShards = Lists.newArrayList();
        String[] indices = indicesService.indices().toArray(new String[]{});
        for (String indexName : indices) {
            IndexService indexService = indicesService.indexServiceSafe(indexName);
            for (int shardId : indexService.shardIds()) {
                indexShards.add(indexService.shard(shardId));
            }
        }
        return indexShards;
    }
}
