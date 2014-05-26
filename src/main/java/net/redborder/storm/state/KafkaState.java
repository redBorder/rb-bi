/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.redborder.storm.state;
 
import backtype.storm.task.IMetricsContext;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.Encoder;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;
 
public class KafkaState implements State {
  protected String kafkaTopic;
  protected String zookeeperHosts;
  protected Producer<String, String> producer;
 
  public static class Factory implements StateFactory {
    protected String kafkaTopic;
    protected String zookeeperHosts;
      
    public Factory(String kafkaTopic, String zookeeperHosts) {
      this.kafkaTopic = kafkaTopic;
      this.zookeeperHosts = zookeeperHosts;
    }
 
    public State makeState(Map conf,
                           int partitionIndex,
                           int numPartitions) {
       return makeState(conf, null, partitionIndex, numPartitions);
     }
 
    public State makeState(Map conf,
                           IMetricsContext context,
                           int partitionIndex,
                           int numPartitions) {
      return new KafkaState(kafkaTopic, zookeeperHosts);
    }
  }
 
  public static class Updater extends BaseStateUpdater<KafkaState> {
    @Override
    public void updateState(KafkaState state, List<TridentTuple> tuples, TridentCollector collector) {
      state.setBulk(tuples);
    }
  }
 
  public KafkaState(String kafkaTopic, String zookeeperHosts) {
    this.kafkaTopic     = kafkaTopic;
    this.zookeeperHosts = zookeeperHosts;
  }
 
  @Override
  public void beginCommit(Long txid) {
 
  }
 
  @Override
  public void commit(Long txid) {
 
  }
 
 
  public void setBulk(List<TridentTuple> tuples) {
    Properties props = new Properties();
    props.put("metadata.broker.list", zookeeperHosts);
    props.put("serializer.class", "net.redborder.storm.util.SimplePartitioner");
    System.out.println("zk.connect: " + props.get("metadata.broker.list"));
 
    ProducerConfig config = new ProducerConfig(props);
    producer = new Producer<String, String>(config);
 
    KeyedMessage<String, String> producerData;
    List<KeyedMessage<String, String>> batchData = new ArrayList<KeyedMessage<String, String>>();
 
    for (TridentTuple tuple : tuples) {
      producerData = new KeyedMessage<String, String>(kafkaTopic, tuple.getString(0));
      batchData.add(producerData);
    }
    System.out.println("Writing " + tuples.size() + " records to Kafka topic \"" + kafkaTopic + "\"");
    producer.send(batchData);
  }
 
  //----------------------------------------------------------------------------
 
}