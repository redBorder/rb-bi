package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * Created by andresgomez on 11/07/14.
 */
public class CheckTimestampFunction extends BaseFunction{
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);
        if(!event.containsKey("timestamp")){
            event.put("timestamp", System.currentTimeMillis()/1000);
            collector.emit(new Values(event));
        }else{
            collector.emit(new Values(event));
        }

    }
}
