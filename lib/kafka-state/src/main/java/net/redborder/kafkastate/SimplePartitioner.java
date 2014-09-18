package net.redborder.kafkastate;

/**
 * Created by andresgomez on 21/08/14.
 */
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {
    public SimplePartitioner(VerifiableProperties props) {

    }

    @Override
    public int partition(Object key, int numPartitions) {
        int partition = 0;
        int offset = key.toString().lastIndexOf('.');
        if (offset > 0) {
            partition = Integer.parseInt(key.toString().substring(offset + 1)) % numPartitions;
        }

        return partition;
    }
}
