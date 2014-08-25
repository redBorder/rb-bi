package net.redborder.storm.state;

import net.redborder.storm.util.ConfigData;
import storm.trident.state.BaseStateUpdater;

/**
 * Created by andresgomez on 25/08/14.
 */
public class StateUpdater {

    public static BaseStateUpdater getStateUpdater(ConfigData config, String key, String value) throws CacheNotValidException {
        if(config.getCacheType().equals("gridgain")){
            return new GridGainUpdater(key, value);
        }else if(config.getCacheType().equals("riak")){
            return new RiakUpdater(key, value);
        } else {
            throw new CacheNotValidException("Not cache backend found: " + config.getCacheType());
        }
    }
}
