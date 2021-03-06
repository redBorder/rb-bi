package net.redborder.storm.state;

import net.redborder.storm.state.gridgain.GridGainUpdater;
import net.redborder.storm.state.memcached.MemcachedUpdater;
import net.redborder.storm.state.riak.RiakUpdater;
import net.redborder.storm.util.ConfigData;
import storm.trident.state.BaseStateUpdater;

/**
 * Created by andresgomez on 25/08/14.
 */
public class StateUpdater {

    public static BaseStateUpdater getStateUpdater(ConfigData config, String key, String value, String bucket) throws CacheNotValidException {
        if (config.getCacheType().equals("gridgain")) {
            return new GridGainUpdater(key, value);
        } else if (config.getCacheType().equals("riak")) {
            return new RiakUpdater(key, value);
        } else if (config.getCacheType().equals("memcached") || config.getCacheType().equals("memory")) {
            return new MemcachedUpdater(key, value, bucket);
        } else {
            throw new CacheNotValidException("Not cache backend found: " + config.getCacheType());
        }
    }
}
