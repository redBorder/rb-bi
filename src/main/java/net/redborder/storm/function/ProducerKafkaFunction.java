/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import java.util.Map;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class ProducerKafkaFunction extends BaseFunction {

    Properties _props;
    Producer<String, String> _producer;
    String _topic;

    public ProducerKafkaFunction(Properties props, String topic) {
        _props = props;
        _topic = topic;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String msg = tuple.getString(0);
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(_topic, msg);
        _producer.send(data);
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

        _props.put("partitioner.class", "com.redborder.storm.util.SimplePartitioner");
        ProducerConfig config = new ProducerConfig(_props);

        _producer = new Producer<String, String>(config);

    }

    @Override
    public void cleanup() {
        _producer.close();
    }

}
