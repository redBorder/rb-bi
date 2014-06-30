package net.redborder.state.gridgain;

import org.gridgain.grid.GridException;
import org.gridgain.grid.cache.GridCache;
import storm.trident.state.map.IBackingMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by andresgomez on 30/06/14.
 */
public class GridGainState<K,V> implements IBackingMap<V> {

    GridCache<K, V> _map;

    public GridGainState(GridCache<K, V> map){
        _map=map;

    }

    @Override
    public List<V> multiGet(List<List<Object>> lists) {

        List<K> keys = new ArrayList<K>();

        for(List key : lists){
            keys.add((K) key.get(0));
        }

        try {
            return new ArrayList<V>(_map.getAll(keys).values());
        } catch (GridException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<V> values) {

        Map<K, V> mapToSave = new HashMap<K, V>();

        for(int i=0; i<keys.size(); i++){

            mapToSave.put((K) keys.get(i).get(0), values.get(i));

        }

        try {
            _map.putAll(mapToSave);
        } catch (GridException e) {
            e.printStackTrace();
        }

    }
}
