package net.redborder.storm.metrics;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by andresgomez on 23/06/14.
 */
public class KafkaConsumerMonitorMetrics implements IMetricsConsumer {

    CuratorFramework client;



    @Override
    public void prepare(Map map, Object conf, TopologyContext topologyContext, IErrorReporter iErrorReporter) {
        Map<String, Object> config = (Map<String, Object>) conf;
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient(config.get("zookeeper").toString(), retryPolicy);
        client.start();


        try {
            if (client.checkExists().forPath("/consumers/rb-storm") == null) {
                client.create().creatingParentsIfNeeded().forPath("/consumers/rb-storm");
                System.out.println("Creating /consumers/rb-storm path ...");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {

        for (Metric metric : dataPointsToMetrics(taskInfo, dataPoints)) {
            try {
                report(metric, metric.value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    String clean(String s) {
        return s.replace('.', '_').replace('/', '_');
    }

    List<Metric> dataPointsToMetrics(TaskInfo taskInfo,
                                     Collection<DataPoint> dataPoints) {
        List<Metric> res = new LinkedList<Metric>();

        String component = clean(taskInfo.srcComponentId);
        String worker = clean(taskInfo.srcWorkerHost);
        Integer port = taskInfo.srcWorkerPort;
        Integer taskId = taskInfo.srcTaskId;


        for (DataPoint p : dataPoints) {

            res.add(new Metric(p.name, worker, port, component, taskId, p.value));
        }
        return res;
    }

    public void report(Metric metric, Object value) throws Exception {

        if (metric.name.equals("kafkaOffset")) {

            Map<String, Object> jsonInfo = (Map<String, Object>) value;


            /*
                    Owners
             */

            String ownersPath = "/consumers/rb-storm/owners/" + jsonInfo.get("topic");

            if (client.checkExists().forPath(ownersPath) == null)
                client.create().creatingParentsIfNeeded().forPath(ownersPath);

            ownersPath = ownersPath + "/" + partitionToNumber(jsonInfo.get("partition").toString());

            String owners = "rb-storm_" + metric.worker + ":" + metric.port + ":" + metric.component + ":" + metric.taskId;

            if (client.checkExists().forPath(ownersPath) != null)
                client.setData().forPath(ownersPath, owners.getBytes());
            else
                client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(ownersPath, owners.getBytes());


            /*
                    Offsets
             */

            String offsetsPath = "/consumers/rb-storm/offsets/" + jsonInfo.get("topic") ;

            if (client.checkExists().forPath(offsetsPath) == null)
                client.create().creatingParentsIfNeeded().forPath(offsetsPath);

            offsetsPath = offsetsPath + "/" + partitionToNumber(jsonInfo.get("partition").toString());

            if (client.checkExists().forPath(offsetsPath) != null)
                client.setData().forPath(offsetsPath, jsonInfo.get("offsets").toString().getBytes());
            else
                client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(offsetsPath, jsonInfo.get("offsets").toString().getBytes());


            /*
                    Ids
             */

            String idsPath = "/consumers/rb-storm/ids";

            if (client.checkExists().forPath(idsPath) == null)
                client.create().creatingParentsIfNeeded().forPath(idsPath);


            idsPath = idsPath + "/" + owners;

            String valuePath = "{ \"pattern\":\"static\", \"subscription\":{ \" " + jsonInfo.get("topic") + "\": 1 }, \"timestamp\":\"" + System.currentTimeMillis() + "\", \"version\":1 }";

            if (client.checkExists().forPath(idsPath) != null)
                client.setData().forPath(idsPath, valuePath.getBytes());
            else
                client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(idsPath, valuePath.getBytes());

            System.out.println("Updating kafka consumer monitor metrics ... ");
        }

    }

    String partitionToNumber(String partition) {
        String number = partition.substring(partition.indexOf("_") + 1, partition.length());

        return number;
    }


    @Override
    public void cleanup() {
        client.close();
    }
}
