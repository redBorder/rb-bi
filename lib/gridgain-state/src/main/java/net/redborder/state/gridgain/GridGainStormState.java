package net.redborder.state.gridgain;

import storm.trident.state.map.IBackingMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by andresgomez on 30/06/14.
 */
public class GridGainStormState implements IBackingMap<Map<String, Map<String, Object>>> {
    String _cacheName;

    public GridGainStormState(String cacheName) {
        _cacheName = cacheName;
    }

    @Override
    public List<Map<String, Map<String, Object>>> multiGet(List<List<Object>> lists) {
        IGridGainStateCache<String, Map<String, Object>> map = GridGainManager.cache(_cacheName);
        List<Map<String, Map<String, Object>>> values = new ArrayList<>();
        List<String> keys = new ArrayList<>();

        for (List key : lists) {
            keys.add((String) key.get(0));
        }

        Map<String, Map<String, Object>> cache = map.getAll(keys);
        values.add(cache);

        return values;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<Map<String, Map<String, Object>>> values) {
        IGridGainStateCache<String, Map<String, Object>> map = GridGainManager.cache(_cacheName);
        map.putAll(values.get(0));
    }
}
