package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * <p>This function check if the event contains the timestamp. If the event does not contain it creates it from the current time.</p>
 * @author Andres Gomez
 */
public class CheckTimestampFunction extends BaseFunction{

    /**
     * <p>This function check if the event contains the timestamp. If the event does not contain it creates it from the current time.</p>
     */
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
