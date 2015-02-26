package net.redborder.storm.filters;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * Created by andresgomez on 14/1/15.
 */
public class FieldFilter extends BaseFilter {
    String _field;
    String _value;

    public FieldFilter(String field, String value) {
        _field = field;
        _value = value;
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        Map<String, Object> tupleMap = (Map<String, Object>) tuple.get(0);
        String fieldStr = (String) tupleMap.get(_field);
        return (fieldStr != null && fieldStr.equals(_value));
    }
}
