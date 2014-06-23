package net.redborder.storm.metrics;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;

/**
 * Created by andresgomez on 23/06/14.
 */
public class KafkaMetrics implements IMetricsConsumer {

    Producer<String, String> producer;
    ObjectMapper _mapper;
    String metricsJSON;
    List<String> metrics;


    public static final Logger LOG = LoggerFactory.getLogger(KafkaMetrics.class);

    @Override
    public void prepare(Map map, Object o, TopologyContext topologyContext, IErrorReporter iErrorReporter) {

        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.101.204:9092, 192.168.101.205:9092, 192.168.101.206:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "net.redborder.storm.metrics.SimplePartitioner");
        props.put("request.required.acks", "0");
        props.put("message.send.max.retries", "10");

        ProducerConfig config = new ProducerConfig(props);

        metrics = (List<String>) o;

        System.out.println("Metrics to kafka: " + metrics.toString());

        producer = new Producer<String, String>(config);

        _mapper = new ObjectMapper();


    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {

        for (Metric metric : dataPointsToMetrics(taskInfo, dataPoints)) {
            report(metric, metric.value);
        }
    }

    @Override
    public void cleanup() {

    }




    List<Metric> dataPointsToMetrics(TaskInfo taskInfo,
                                     Collection<DataPoint> dataPoints) {
        List<Metric> res = new LinkedList<Metric>();

        String component = clean(taskInfo.srcComponentId);
        String worker = clean(taskInfo.srcWorkerHost);


        for (DataPoint p : dataPoints) {

            res.add(new Metric(p.name, worker,taskInfo.srcWorkerPort , component, taskInfo.srcTaskId, p.value));
        }
        return res;
    }

    String clean(String s) {
        return s.replace('.', '_').replace('/', '_');
    }

    public void report(Metric metric, Object value) {


        if(metrics.contains(metric.name)) {

            Map<String, Object> map = (Map<String, Object>) value;
            map.put("metric_name", metric.name);
            map.put("worker", metric.worker);
            map.put("componentId", metric.component);

            try {
                metricsJSON = _mapper.writeValueAsString(map);
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(KafkaMetrics.class.getName()).log(Level.SEVERE, null, ex);
            }

            KeyedMessage<String, String> data = new KeyedMessage<String, String>("rb_metrics", metricsJSON);

            producer.send(data);
        }

    }
}



