package net.redborder.state.gridgain;

import org.gridgain.grid.GridException;
import org.gridgain.grid.cache.GridCache;
import storm.trident.state.map.IBackingMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by andresgomez on 30/06/14.
 */
public class GridGainState implements IBackingMap<Map<String, Map<String, Object>>> {

    GridCache<String, Map<String, Object>> _map;

    public GridGainState(GridCache<String, Map<String, Object>> map){
        _map=map;

    }

    @Override
    public List<Map<String, Map<String, Object>>> multiGet(List<List<Object>> lists){
        List<String> keys = new ArrayList<>();

        for(List key : lists){
            keys.add((String) key.get(0));
        }

        try {

            List<Map<String, Map<String, Object>>> values = new ArrayList<>();

            values.add(_map.getAll(keys));

            return values;
        } catch (GridException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<Map<String, Map<String, Object>>> values) {

        try {
            _map.putAll(values.get(0));
        } catch (GridException e) {
            e.printStackTrace();
        }

    }
}
