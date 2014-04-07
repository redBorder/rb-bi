/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.filter;

import java.util.Map;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class MSEenrichedFilter extends BaseFilter {

    String _log;

    public MSEenrichedFilter() {
        _log = "";

    }

    public MSEenrichedFilter(String log) {
        _log = log;
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        Map<String, Object> event = (Map<String, Object>) tuple.get(0);
        System.out.println(_log+" "+"Filter: " + event);

        return event != null;
    }

}
