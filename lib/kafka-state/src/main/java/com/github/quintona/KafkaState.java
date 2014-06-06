package com.github.quintona;

import backtype.storm.task.IMetricsContext;
import org.boon.json.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.boon.json.JsonFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

public class KafkaState<T> implements State {
    List<KeyedMessage<String, String>> messages = new ArrayList<>();
    String _brokerList = new String();
    Producer<String, String> producer;

    public static StateFactory nonTransactional(String zookeeper) {
        return new Factory(zookeeper);
    }

    protected static class Factory implements StateFactory {
        private final String zookeeper;

        public Factory(String zookeeper) {
            this.zookeeper = zookeeper;
        }

        @Override
        public State makeState(Map conf, IMetricsContext metrics,
                int partitionIndex, int numPartitions) {
            return new KafkaState(zookeeper);
        }
    }

    public KafkaState(String zookeeper) {        
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeper, retryPolicy);
        client.start();

        List<String> ids = null;
        boolean first = true;

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
                ObjectMapper mapper = JsonFactory.create();
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
                    Logger.getLogger(KafkaState.class.getName()).log(Level.SEVERE, "Failed converting a JSON tuple to a Map class", ex);
                }
            }
        }
        
        Properties props = new Properties();
        props.put("metadata.broker.list", _brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<>(config);
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
