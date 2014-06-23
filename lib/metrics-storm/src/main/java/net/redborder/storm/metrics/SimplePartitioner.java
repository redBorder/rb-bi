package net.redborder.storm.metrics;

/**
 * Created by andresgomez on 23/06/14.
 */
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
/**
 *
 * @author andresgomez
 */
public class SimplePartitioner implements Partitioner<String> {
    public SimplePartitioner (VerifiableProperties props) {

    }

    public int partition(String key, int a_numPartitions) {
        int partition = 0;
        int offset = key.lastIndexOf('.');
        if (offset > 0) {
            partition = Integer.parseInt( key.substring(offset+1)) % a_numPartitions;
        }
        return partition;
    }
}
