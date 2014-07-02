package net.redborder.storm.state;

import backtype.storm.task.IMetricsContext;
import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.DefaultRetrier;
import com.basho.riak.client.query.MultiFetchFuture;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import storm.trident.state.State;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.NonTransactionalMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class RiakState<T> implements IBackingMap<T> {

    private final Class<T> clazz;
    private final Bucket bucket;

    public RiakState(String bucketName, List<String> hosts, int port, Class<T> clazz) {

        this.clazz = clazz;

        try {
            PBClusterConfig clusterConfig = new PBClusterConfig(5);

            PBClientConfig clientConfig = new PBClientConfig.Builder()
                    .withPort(port)
                    .build();

            clusterConfig.addHosts(clientConfig, hosts.toArray(new String[hosts.size()]));

            IRiakClient riakClient = RiakFactory.newClient(clusterConfig);

            bucket = riakClient.createBucket(bucketName)
                    .withRetrier(DefaultRetrier.attempts(3))
                    .execute();

        } catch (Exception e) {
            throw new RuntimeException("Exception while talking to Riak!", e);
        }
    }

    @Override
    public List<T> multiGet(List<List<Object>> lists) {
        try {
            ArrayList<String> keys = new ArrayList<>();
            for (List<Object> keyList : lists) {
                keys.add(keyList.get(0).toString());
            }

            List<MultiFetchFuture<T>> futureResults = bucket.multiFetch(keys, clazz)
                    .withRetrier(DefaultRetrier.attempts(3))
                    .execute();

            ArrayList<T> results = new ArrayList<>();
            for (MultiFetchFuture<T> result : futureResults) {
                results.add(result.get());
            }

            return results;
        } catch (Exception e) {
            throw new RuntimeException("Exception while talking to Riak!", e);
        }
    }

    @Override
    public void multiPut(List<List<Object>> lists, List<T> ts) {

        Iterator<List<Object>> keyIterator = lists.iterator();
        Iterator<T> valueIterator = ts.iterator();

        try {

            while (keyIterator.hasNext() && valueIterator.hasNext()) {
                String key = keyIterator.next().get(0).toString();
                T value = valueIterator.next();

                bucket.store(key, value)
                        .withRetrier(DefaultRetrier.attempts(3))
                        .execute();
            }
        } catch (Exception e) {
            throw new RuntimeException("Exception while talking to Riak!", e);
        }
    }

    /**
     * Creates a nontransactional state using Riak as the backing store.
     *
     * This is nontransactional for two reasons: it makes it friendlier to users of the HTTP client
     */
    public static class Factory<T> implements storm.trident.state.StateFactory {

        private final String bucket;
        private final List<String> hosts;
        private final int port;
        private final Class<T> clazz;

        public Factory(String bucket, List<String> hosts, int port, Class<T> clazz) {
            this.bucket = bucket;
            this.hosts = hosts;
            this.port = port;
            this.clazz = clazz;
        }

        @Override public State makeState(Map map, IMetricsContext iMetricsContext, int i, int i2) {
            return NonTransactionalMap.build(new RiakState<>(bucket, hosts, port, clazz));
        }
    }
}
