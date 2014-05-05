/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class SeparateLongTimeFlowFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);
        List<Map<String, Object>> listToSend = new ArrayList<>();
        
        if (event.containsKey("first_switched") && event.containsKey("last_switched")) {
            DateTime start = new DateTime(Long.parseLong(event.get("first_switched").toString()));
            DateTime end = new DateTime(Long.parseLong(event.get("last_switched").toString()));
            Seconds diff = Seconds.secondsBetween(start, end);
            int bytes = Integer.parseInt(event.get("bytes").toString());
            int pkts = Integer.parseInt(event.get("pkts").toString());
            
            if (diff.getSeconds() < 60) {
                int min_start = start.getMinuteOfDay();
                int min_end = end.getMillisOfDay();
                
                if (min_start == min_end) {
                    // No dividir
                    event.put("timestamp", event.get("last_switched"));
                    listToSend.add(event);
                } else {
                    // Dividir en dos paquetes
                    int secs_start = 60 - start.getSecondOfMinute();
                    int secs_end = end.getSecondOfMinute();
                    int secs_sum = secs_start + secs_end;
                    
                    Map<String, Object> first = new HashMap<>();
                    first.putAll(event);
                    first.put("timestamp", start.getMillis() / 1000 - start.getSecondOfMinute());
                    first.put("bytes", secs_start * bytes / secs_sum);
                    first.put("pkts", secs_start * pkts / secs_sum);
                    listToSend.add(first);
                    
                    Map<String, Object> second = new HashMap<>();
                    second.putAll(event);
                    second.put("timestamp", end.getMillis() / 1000);
                    second.put("bytes", secs_end * bytes / secs_sum);
                    second.put("pkts", secs_end * pkts / secs_sum);
                    listToSend.add(second);
                }
            }
        }
        
        for (Map<String, Object> e : listToSend) {
            collector.emit(new Values(e));
        }
    }

}
