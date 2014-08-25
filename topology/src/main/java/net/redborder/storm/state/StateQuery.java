package net.redborder.storm.state;

import net.redborder.storm.util.ConfigData;
import storm.trident.state.BaseQueryFunction;

/**
 * Created by andresgomez on 25/08/14.
 */
public class StateQuery {

    public static BaseQueryFunction getStateQuery(ConfigData config, String key){
        if(config.getCacheType().equals("gridgain")){
            return new GridGainQuery(key);
        }else{
            return new RiakQuery(key);
        }
    }

    public static BaseQueryFunction getStateLocationQuery(ConfigData config) throws CacheNotValidException {
        if(config.getCacheType().equals("gridgain")){
            return new GridGainLocationQuery("client_mac");
        }else if(config.getCacheType().equals("riak")){
            return new RiakLocationQuery("client_mac");
        } else {
            throw new CacheNotValidException("Not cache backend found: " + config.getCacheType());
        }
    }
}
