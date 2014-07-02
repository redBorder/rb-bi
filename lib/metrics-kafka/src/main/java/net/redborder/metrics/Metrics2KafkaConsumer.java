package net.redborder.metrics;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;

/**
 * Created by andresgomez on 23/06/14.
 */
public class Metrics2KafkaConsumer implements IMetricsConsumer {

    Producer<String, String> producer;
    org.codehaus.jackson.map.ObjectMapper _mapper;
    String metricsJSON;
    List<String> metrics;
    String _topic;
    long timestamp=0;
    String _brokerList = new String();



    public static final Logger LOG = LoggerFactory.getLogger(Metrics2KafkaConsumer.class);

    @Override
    public void prepare(Map map, Object conf, TopologyContext topologyContext, IErrorReporter iErrorReporter) {
        Map<String, Object> config = (Map<String, Object>) conf;

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(config.get("zookeeper").toString(), retryPolicy);
        client.start();

        List<String> ids = null;
        boolean first = true;

        try {
            ids = client.getChildren().forPath("/brokers/ids");
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(Metrics2KafkaConsumer.class.getName()).log(Level.SEVERE, null, ex);
        }

        for (String id : ids) {
            String jsonString = null;

            try {
                jsonString = new String(client.getData().forPath("/brokers/ids/" + id), "UTF-8");
            } catch (Exception ex) {
                java.util.logging.Logger.getLogger(Metrics2KafkaConsumer.class.getName()).log(Level.SEVERE, null, ex);
            }

            if (jsonString != null) {
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> json = null;

                try {
                    json = mapper.readValue(jsonString, Map.class);

                    if (first) {
                        _brokerList = _brokerList.concat(json.get("host") + ":" + json.get("port"));
                        first = false;
                    } else {
                        _brokerList = _brokerList.concat("," + json.get("host") + ":" + json.get("port"));
                    }
                } catch (NullPointerException ex) {
                    java.util.logging.Logger.getLogger(Metrics2KafkaConsumer.class.getName()).log(Level.SEVERE, "Failed converting a JSON tuple to a Map class", ex);
                } catch (JsonMappingException e) {
                    e.printStackTrace();
                } catch (JsonParseException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        Properties props = new Properties();
        props.put("metadata.broker.list", _brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "net.redborder.metrics.SimplePartitioner");
        props.put("request.required.acks", "1");
        props.put("message.send.max.retries", "10");

        ProducerConfig configKafka = new ProducerConfig(props);

        metrics = (List<String>) config.get("metrics");

        _topic = config.get("topic").toString();


        System.out.println("Metrics to kafka: " + metrics.toString());

        producer = new Producer<String, String>(configKafka);

        _mapper = new org.codehaus.jackson.map.ObjectMapper();


    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {

        for (Metric metric : dataPointsToMetrics(taskInfo, dataPoints)) {
            report(metric, metric.value);
        }
    }

    @Override
    public void cleanup() {
        producer.close();
    }



    List<Metric> dataPointsToMetrics(TaskInfo taskInfo,
                                     Collection<DataPoint> dataPoints) {
        List<Metric> res = new LinkedList<Metric>();

        String component = clean(taskInfo.srcComponentId);
        String worker = clean(taskInfo.srcWorkerHost);


        for (DataPoint p : dataPoints) {

            res.add(new Metric(p.name, worker, taskInfo.srcWorkerPort , component, taskInfo.srcTaskId, p.value));
        }
        return res;
    }

    String clean(String s) {
        return s.replace('.', '_').replace('/', '_');
    }

    public void report(Metric metric, Object value) {


        if(metric.name.contains("throughput_")) {

            Map<String, Object> map = (Map<String, Object>) value;

            System.out.println("Sending metric : " + metric.name + " ...");

            map.put("timestamp", System.currentTimeMillis()/1000);
            map.put("monitor", metric.name);
            map.put("sensor_name", metric.worker);


            try {
                metricsJSON = _mapper.writeValueAsString(map);
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(Metrics2KafkaConsumer.class.getName()).log(Level.SEVERE, null, ex);
            }

            KeyedMessage<String, String> data = new KeyedMessage<String, String>(_topic, metricsJSON);

            producer.send(data);
        }

    }
}



