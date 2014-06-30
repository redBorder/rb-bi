import backtype.storm.task.IMetricsContext;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.NonTransactionalMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by andresgomez on 28/06/14.
 */
public class HazelcastStateFactory<T> implements StateFactory {

    HazelcastOptions _options;

    public HazelcastStateFactory(HazelcastOptions options) {
        _options = options;

    }


    @Override
    public State makeState(Map map, IMetricsContext iMetricsContext, int i, int i2) {

        if (_options.existsHazelcastCluster) {
            ClientConfig configClient = new ClientConfig();
            configClient.getGroupConfig().setName(_options.groupName).setPassword(_options.groupPassword);
            configClient.getNetworkConfig().setAddresses(_options.addressHazelcastNodes);

            System.out.println("Connecting to hazelcast cluster  ... ");
            System.out.println("[ Name: " + _options.groupName + ", Pass: " + _options.groupPassword + " ]");
            System.out.println("Connect to: " + _options.addressHazelcastNodes.toString());


            return NonTransactionalMap.build(new HazelcastState<T>(_options, HazelcastClient.newHazelcastClient(configClient)));
        } else {

            Config config = new Config();
            config.setGroupConfig(new GroupConfig().setName(_options.groupName).setPassword(_options.groupPassword));

            Map configMap = new HashMap<String, MapConfig>();

            System.out.println("Configuring map: " +_options.mapName);

            configMap.put(_options.mapName,
                    new MapConfig()
                            .setName(_options.mapName)
                            .setBackupCount(_options.backupCount)
                            .setAsyncBackupCount(_options.AsynbackupCount)
                            .setInMemoryFormat(InMemoryFormat.valueOf(_options.memoryFormat))
            );

            config.setMapConfigs(configMap);

            System.out.println("Creating hazelcast instance ...");
            System.out.println("[ Name: " + _options.groupName + ", Pass: " + _options.groupPassword + " ]");

            return NonTransactionalMap.build(new HazelcastState<T>(_options, Hazelcast.newHazelcastInstance(config)));
        }
    }
}
