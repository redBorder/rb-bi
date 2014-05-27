package com.github.quintona;

import backtype.storm.task.IMetricsContext;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

public class KafkaState<T> implements State {
	
	List<KeyedMessage<String, String>> messages = new ArrayList<>();

	public static class Options implements Serializable {
		public String zookeeperHost = "127.0.0.1";
		public int zookeeperPort = 2181;
		public String serializerClass = "kafka.serializer.StringEncoder";
		
		public Options(){}

		public Options(String zookeeperHost, int zookeeperPort, String serializerClass, String topicName) {
			this.zookeeperHost = zookeeperHost;
			this.zookeeperPort = zookeeperPort;
			this.serializerClass = serializerClass;
		}
	}
	
	public static StateFactory transactional(String topic, Options options) {
        return new Factory(topic, options, true);
    }
	
	public static StateFactory nonTransactional(String topic, Options options) {
        return new Factory(topic, options, false);
    }

	protected static class Factory implements StateFactory {
		
		private Options options;
		private String topic;
		boolean transactional;
		
		public Factory(String topic, Options options, boolean transactional){
			this.options = options;
			this.topic = topic;
			this.transactional = transactional;
		}

		@Override
		public State makeState(Map conf, IMetricsContext metrics,
				int partitionIndex, int numPartitions) {
			return new KafkaState(topic, options, transactional);
		}

	}
	
	private Options options;
	private String topic;
	Producer<String, String> producer;
	private boolean transactional;
	
	public KafkaState(String topic, Options options, boolean transactional){
		this.topic = topic;
		this.options = options;
		this.transactional = transactional;
		Properties props = new Properties();
		props.put("metadata.broker.list", options.zookeeperHost + ":" + Integer.toString(options.zookeeperPort));
		props.put("serializer.class", options.serializerClass);
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}

	@Override
	public void beginCommit(Long txid) {
		if(messages.size() > 0)
			throw new RuntimeException("Kafka State is invalid, the previous transaction didn't flush");
	}
	
	public void enqueue(KeyedMessage<String, String> message){
                messages.add(message);
	}
	
	private void sendMessage(){
		producer.send(messages);
	}

	@Override
	public void commit(Long txid) {
		sendMessage();
                messages.clear();
	}

}
