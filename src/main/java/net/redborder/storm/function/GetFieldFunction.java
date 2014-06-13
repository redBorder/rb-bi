/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import java.util.Map;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class GetFieldFunction extends BaseFunction {

    String _field;
    boolean debug;

    public GetFieldFunction(String field, boolean debug) {
        _field = field;
        this.debug=debug;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);
        if (event.containsKey(_field)) {
            String field = event.get(_field).toString();
            collector.emit(new Values(field));
        } else {
            collector.emit(new Values("null"));
        }
    }

}
