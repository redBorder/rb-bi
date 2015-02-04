package net.redborder.state.gridgain;

import org.gridgain.grid.Grid;
import org.gridgain.grid.GridException;
import org.gridgain.grid.GridTopologyException;
import org.gridgain.grid.cache.GridCache;
import storm.trident.state.map.IBackingMap;

import java.util.ArrayList;
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
        List<String> keys = new ArrayList<>();

        for (List key : lists) {
            keys.add((String) key.get(0));
        }

        try {

            List<Map<String, Map<String, Object>>> values = new ArrayList<>();

            values.add(_map.getAll(keys));

            return values;
        } catch (Exception e) {
            if(e instanceof GridTopologyException) {
                Logger.getLogger(GridGainStormState.class.getName()).log(Level.SEVERE, "All gridgain nodes are shutdown!!!", e);
            }else {
                e.printStackTrace();
            }
            // GridGainManager.close();
           // _map = GridGainManager.getGrid().cache(_cacheName);
        }

        return null;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<Map<String, Map<String, Object>>> values) {

        try {
            _map.putAll(values.get(0));
        } catch (Exception e) {
            if(e instanceof GridTopologyException) {
                Logger.getLogger(GridGainStormState.class.getName()).log(Level.SEVERE, "All gridgain nodes are shutdown!!!", e);
            }else {
                e.printStackTrace();
            }
           // GridGainManager.close();
            //map = GridGainManager.getGrid().cache(_cacheName);
        }
    }
}
