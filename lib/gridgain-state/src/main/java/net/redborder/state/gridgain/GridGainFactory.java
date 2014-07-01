package net.redborder.state.gridgain;

import backtype.storm.task.IMetricsContext;
import org.gridgain.grid.Grid;
import org.gridgain.grid.GridConfiguration;
import org.gridgain.grid.GridException;
import org.gridgain.grid.GridGain;
import org.gridgain.grid.cache.GridCache;
import org.gridgain.grid.cache.GridCacheConfiguration;
import org.gridgain.grid.cache.GridCacheDistributionMode;
import org.gridgain.grid.cache.GridCacheMode;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.NonTransactionalMap;

import java.util.Map;

/**
 * Created by andresgomez on 30/06/14.
 */
public class GridGainFactory<K, V> implements StateFactory {

    String _cacheName;

    public GridGainFactory(String cacheName){
        _cacheName=cacheName;
    }


    @Override
    public State makeState(Map configStorm, IMetricsContext iMetricsContext, int i, int i2) {

        Grid _grid = GridGain.grid();

        GridCache<K,V> map = _grid.cache(_cacheName);
        return NonTransactionalMap.build(new GridGainState<K, V>(map));
    }
}
