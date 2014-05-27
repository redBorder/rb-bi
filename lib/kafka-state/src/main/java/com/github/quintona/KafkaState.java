package com.github.quintona;

import backtype.storm.task.IMetricsContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

public class KafkaState<T> implements State {

    List<KeyedMessage<String, String>> messages = new ArrayList<>();
    String _brokerList;
    String _topic;
    private Options options;
    private String topic;
    Producer<String, String> producer;

    public static class Options implements Serializable {

        public String serializerClass = "kafka.serializer.StringEncoder";

    }

    public static StateFactory nonTransactional(String zookeeper, String topic, Options options) {
        return new Factory(zookeeper, topic, options);
    }

    protected static class Factory implements StateFactory {

        private Options options;
        private String topic;
        private String zookeeper;

        public Factory(String zookeeper ,String topic, Options options) {
            this.options = options;
            this.topic = topic;
            this.zookeeper = zookeeper;
        }

        @Override
        public State makeState(Map conf, IMetricsContext metrics,
                int partitionIndex, int numPartitions) {
            return new KafkaState(zookeeper,topic, options);
        }

    }

    public KafkaState(String zookeeper ,String topic, Options options) {
        this.topic = topic;
        this.options = options;
        
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeper, retryPolicy);
        _brokerList = new String();
        client.start();

        List<String> ids = null;
        boolean first = true;
        _topic = topic;

        try {
            ids = client.getChildren().forPath("/brokers/ids");
        } catch (Exception ex) {
            Logger.getLogger(KafkaState.class.getName()).log(Level.SEVERE, null, ex);
        }

        for (String id : ids) {
            String jsonString = null;

            try {
                jsonString = new String(client.getData().forPath("/brokers/ids/" + id), "UTF-8");
            } catch (Exception ex) {
                Logger.getLogger(KafkaState.class.getName()).log(Level.SEVERE, null, ex);
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
                } catch (IOException | NullPointerException ex) {
                    Logger.getLogger(KafkaState.class.getName()).log(Level.SEVERE, "Failed converting a JSON tuple to a Map class", ex);
                }
            }
        }
        
        Properties props = new Properties();
        props.put("metadata.broker.list", _brokerList);
        props.put("serializer.class", options.serializerClass);
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }

    @Override
    public void beginCommit(Long txid) {
        if (messages.size() > 0) {
            throw new RuntimeException("Kafka State is invalid, the previous transaction didn't flush");
        }
    }

    public void enqueue(KeyedMessage<String, String> message) {
        messages.add(message);
    }

    private void sendMessage() {
        producer.send(messages);
    }

    @Override
    public void commit(Long txid) {
        sendMessage();
        messages.clear();
    }

}
