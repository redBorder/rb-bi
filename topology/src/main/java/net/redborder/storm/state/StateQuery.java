package net.redborder.storm.state;

import net.redborder.storm.state.gridgain.GridGainLocationQuery;
import net.redborder.storm.state.gridgain.GridGainNmspMeasureQuery;
import net.redborder.storm.state.gridgain.GridGainQuery;
import net.redborder.storm.state.gridgain.GridGainTrapQuery;
import net.redborder.storm.state.memcached.MemcachedLocationQuery;
import net.redborder.storm.state.memcached.MemcachedNmspMeasureQuery;
import net.redborder.storm.state.memcached.MemcachedQuery;
import net.redborder.storm.state.memcached.MemcachedTrapQuery;
import net.redborder.storm.state.riak.RiakLocationQuery;
import net.redborder.storm.state.riak.RiakNmspMeasureQuery;
import net.redborder.storm.state.riak.RiakQuery;
import net.redborder.storm.state.riak.RiakTrapQuery;
import net.redborder.storm.util.ConfigData;
import storm.trident.state.BaseQueryFunction;

/**
 * Created by andresgomez on 25/08/14.
 */
public class StateQuery {

    public static BaseQueryFunction getStateQuery(ConfigData config, String key, String bucket) throws CacheNotValidException{
        if(config.getCacheType().equals("gridgain")){
            return new GridGainQuery(key);
        }else if(config.getCacheType().equals("riak")){
            return new RiakQuery(key);
        } else if(config.getCacheType().equals("memcached")){
            return new MemcachedQuery(key, bucket);
        }else{
            throw new CacheNotValidException("Not cache backend found: " + config.getCacheType());
        }
    }

    public static BaseQueryFunction getStateLocationQuery(ConfigData config) throws CacheNotValidException {
        if(config.getCacheType().equals("gridgain")){
            return new GridGainLocationQuery("client_mac");
        }else if(config.getCacheType().equals("riak")){
            return new RiakLocationQuery("client_mac");
        }else if(config.getCacheType().equals("memcached")){
            return new MemcachedLocationQuery("client_mac", "location");
        } else {
            throw new CacheNotValidException("Not cache backend found: " + config.getCacheType());
        }
    }

    public static BaseQueryFunction getStateTrapQuery(ConfigData config) throws CacheNotValidException {
        if(config.getCacheType().equals("gridgain")){
            return new GridGainTrapQuery("client_mac");
        }else if(config.getCacheType().equals("riak")){
            return new RiakTrapQuery("client_mac");
        }else if(config.getCacheType().equals("memcached")){
            return new MemcachedTrapQuery("client_mac", "trap");
        } else {
            throw new CacheNotValidException("Not cache backend found: " + config.getCacheType());
        }
    }

    public static BaseQueryFunction getStateNmspMeasureQuery(ConfigData config) throws CacheNotValidException {
        if(config.getCacheType().equals("gridgain")){
            return new GridGainNmspMeasureQuery("client_mac");
        }else if(config.getCacheType().equals("riak")){
            return new RiakNmspMeasureQuery("client_mac");
        }else if(config.getCacheType().equals("memcached")){
            return new MemcachedNmspMeasureQuery("client_mac", "nmsp");
        } else {
            throw new CacheNotValidException("Not cache backend found: " + config.getCacheType());
        }
    }
}
