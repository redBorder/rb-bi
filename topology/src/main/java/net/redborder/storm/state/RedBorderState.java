package net.redborder.storm.state;

import net.redborder.state.gridgain.GridGainFactory;
import net.redborder.storm.util.ConfigData;
import storm.trident.state.StateFactory;
import java.util.Map;

/**
 * Created by andresgomez on 25/08/14.
 */
public class RedBorderState {

    public static StateFactory getStateFactory(ConfigData config, String cacheName) throws CacheNotValidException {
        if(config.getCacheType().equals("gridgain")){
            return new GridGainFactory(cacheName, config.getEnrichs(),  config.getGridGainConfig());
        }else if(config.getCacheType().equals("riak")){
            return new RiakState.Factory<>("rbbi:" + cacheName, config.getRiakServers(), 8087, Map.class);
        } else if (config.getCacheType().equals("memcached")) {
            MemcachedState.Options memcachedOpts = new MemcachedState.Options();
            memcachedOpts.expiration = 0;
            return MemcachedState.transactional(config.getMemcachedServers(), memcachedOpts);
        }else {
            throw new CacheNotValidException("Not cache backend found: " + config.getCacheType());
        }
    }
}
