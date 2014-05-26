/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import net.redborder.storm.util.KafkaConfigFile;
import org.apache.curator.*;
import org.apache.curator.framework.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.codehaus.jackson.map.ObjectMapper;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class ProducerKafkaFunction extends BaseFunction {

    Producer<String, String> _producer;
    String _brokerList;
    String _topic;
    Map<Long, Integer> _count;

    public ProducerKafkaFunction(KafkaConfigFile config, String topic) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(config.getZkHost(), retryPolicy);
        _brokerList = new String();
        client.start();

        List<String> ids = null;
        boolean first = true;
        _topic = topic;

        try {
            ids = client.getChildren().forPath("/brokers/ids");
        } catch (Exception ex) {
            Logger.getLogger(ProducerKafkaFunction.class.getName()).log(Level.SEVERE, null, ex);
        }

        for (String id : ids) {
            String jsonString = null;

            try {
                jsonString = new String(client.getData().forPath("/brokers/ids/" + id), "UTF-8");
            } catch (Exception ex) {
                Logger.getLogger(ProducerKafkaFunction.class.getName()).log(Level.SEVERE, null, ex);
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
                    Logger.getLogger(MapperFunction.class.getName()).log(Level.SEVERE, "Failed converting a JSON tuple to a Map class", ex);
                }
            }
        }
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String msg = tuple.getString(0);
        KeyedMessage<String, String> data = new KeyedMessage<>(_topic, msg);
        _producer.send(data);

        Long timestamp = System.currentTimeMillis() / 1000;

        if (_count.containsKey(timestamp)) {
            Integer count = _count.get(timestamp);
            _count.put(timestamp, count + 1);
        } else {
            if (_count.containsKey(timestamp - 1)) {
                System.out.println(_count.get(timestamp - 1));
            }

            _count.put(timestamp, 1);
        }

    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        Properties props;
        props = new Properties();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        props.put("partitioner.class", "net.redborder.storm.util.SimplePartitioner");
        props.put("metadata.broker.list", _brokerList);
        ProducerConfig config = new ProducerConfig(props);

        _producer = new Producer<>(config);
        _count = new HashMap<>();

    }

    @Override
    public void cleanup() {
        _producer.close();
    }

}
