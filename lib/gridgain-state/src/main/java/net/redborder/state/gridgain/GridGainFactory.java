package net.redborder.state.gridgain;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.NonTransactionalMap;

import java.util.*;

/**
 * Created by andresgomez on 30/06/14.
 */
public class GridGainFactory implements StateFactory {
    String _cacheName;
    List<String> _topics;
    Map<String, Object> _gridGainConfig;

    public GridGainFactory(String cacheName, List<String> topics, Map<String, Object> gridGainConfig) {
        _cacheName = cacheName;
        _topics = topics;
        _gridGainConfig = gridGainConfig;
    }

    @Override
    public State makeState(Map configStorm, IMetricsContext iMetricsContext, int i, int i2) {
        GridGainManager.init(_topics, _gridGainConfig);
        return NonTransactionalMap.build(new GridGainStormState(_cacheName));
    }
}
