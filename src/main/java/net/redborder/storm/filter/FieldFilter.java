/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author carlosrdrz
 */
public class FieldFilter extends BaseFilter {

    List<String> _fields;

    public FieldFilter(String... fields) {
        _fields = new ArrayList<>();
        _fields.addAll(Arrays.asList(fields));
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        Map<String, Object> event = (Map<String, Object>) tuple.get(0);
        boolean isKeep = true;

        for (String field : _fields) {
            if (!event.containsKey(field)) {
                isKeep = false;
            }
        }

        return isKeep;
    }

}
