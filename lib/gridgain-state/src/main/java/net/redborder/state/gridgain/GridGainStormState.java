package net.redborder.state.gridgain;

import org.gridgain.grid.Grid;
import org.gridgain.grid.GridException;
import org.gridgain.grid.GridTopologyException;
import org.gridgain.grid.cache.GridCache;
import org.gridgain.grid.cache.GridCacheAtomicUpdateTimeoutException;
import storm.trident.state.map.IBackingMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 30/06/14.
 */
public class GridGainStormState implements IBackingMap<Map<String, Map<String, Object>>> {

    GridCache<String, Map<String, Object>> _map;
    String _cacheName;

    public GridGainStormState(String cacheName) {
        _map = GridGainManager.getGrid().cache(cacheName);
        _cacheName = cacheName;
    }

    @Override
    public List<Map<String, Map<String, Object>>> multiGet(List<List<Object>> lists) {
        List<Map<String, Map<String, Object>>> values = new ArrayList<>();
        List<String> keys = new ArrayList<>();

        for (List key : lists) {
            keys.add((String) key.get(0));
        }

        try {

            Map<String, Map<String, Object>> cache = _map.getAll(keys);
            values.add(cache);

        } catch (Exception e) {
            if(e instanceof GridTopologyException) {
                Logger.getLogger(GridGainStormState.class.getName()).log(Level.SEVERE, "All gridgain nodes are shutdown!!!", e);
                GridGainManager.reconnect();
            }else if(e instanceof GridCacheAtomicUpdateTimeoutException) {
                Logger.getLogger(GridGainStormState.class.getName()).log(Level.SEVERE, "Network timeout, storm will try reconnect to gridgain cluster ...", e);
                GridGainManager.reconnect();
            }else if(e instanceof  IllegalStateException) {
                Logger.getLogger(GridGainStormState.class.getName()).log(Level.SEVERE, "Other storm state instance initiate gridgain client before, try to repare the connection ...", e);
            } else {
                Logger.getLogger(GridGainStormState.class.getName()).log(Level.SEVERE, "Storm will try reconnect to gridgain cluster ...", e);
                GridGainManager.reconnect();
            }

            _map = GridGainManager.getGrid().cache(_cacheName);
            Map<String, Map<String, Object>> errorCache = new HashMap<>();
            values.add(errorCache);
        }

        return values;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<Map<String, Map<String, Object>>> values) {

        try {
            _map.putAll(values.get(0));
        } catch (Exception e) {
            if(e instanceof GridTopologyException) {
                Logger.getLogger(GridGainStormState.class.getName()).log(Level.SEVERE, "All gridgain nodes are shutdown!!!", e);
                GridGainManager.reconnect();
            }else if(e instanceof GridCacheAtomicUpdateTimeoutException) {
                Logger.getLogger(GridGainStormState.class.getName()).log(Level.SEVERE, "Network timeout, storm will try reconnect to gridgain cluster ...", e);
                GridGainManager.reconnect();
            }else if(e instanceof  IllegalStateException) {
                Logger.getLogger(GridGainStormState.class.getName()).log(Level.SEVERE, "Other storm state instance initiate gridgain client before, try to repare the connection ...", e);
            } else {
                Logger.getLogger(GridGainStormState.class.getName()).log(Level.SEVERE, "Storm will try reconnect to gridgain cluster ...", e);
                GridGainManager.reconnect();
            }
            _map = GridGainManager.getGrid().cache(_cacheName);
        }
    }
}
