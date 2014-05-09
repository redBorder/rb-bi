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
import java.util.logging.Level;
import java.util.logging.Logger;
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
    
    public final int DELAYED_REALTIME_TIME = 15;
    
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);
        List<Map<String, Object>> generatedPackets = new ArrayList<>();
        
        if (event.containsKey("first_switched") && event.containsKey("last_switched")) {
            DateTime packet_start = new DateTime(Long.parseLong(event.get("first_switched").toString()) * 1000);
            DateTime packet_end = new DateTime(Long.parseLong(event.get("last_switched").toString()) * 1000);
            DateTime limit = new DateTime().withMinuteOfHour(0);
            DateTime now = new DateTime();
            int now_hour = now.getHourOfDay();
            int packet_end_hour = packet_end.getHourOfDay();
            
            //System.out.println("Separation packet " + event);
            
            if (packet_end.isAfter(now)) {
                Logger.getLogger(SeparateLongTimeFlowFunction.class.getName()).log(Level.WARNING, 
                    "Dropped packet {0} because it ended in the future.", event);
                return;
            } else if (now_hour != packet_end_hour) {
                int now_minutes = now.getMinuteOfHour();
                if (now_minutes > DELAYED_REALTIME_TIME) {
                    Logger.getLogger(SeparateLongTimeFlowFunction.class.getName()).log(Level.WARNING, 
                        "Dropped packet {0} because its realtime processor is already shutdown.", event);
                    return;
                }
            }
            
            if (packet_start.isBefore(limit)) {
                packet_start = limit;
            }
            
            DateTime this_start;
            DateTime this_end = packet_start;
            int bytes = Integer.parseInt(event.get("bytes").toString());
            int pkts = Integer.parseInt(event.get("pkts").toString());
            int totalDiff = Seconds.secondsBetween(packet_start, packet_end).getSeconds();
            int diff, this_bytes, this_pkts;
            int bytes_count = 0;
            int pkts_count = 0;
            
            do {
                this_start = this_end;
                this_end = this_start.plusSeconds(60 - this_start.getSecondOfMinute());
                if (this_end.isAfter(packet_end)) this_end = packet_end;
                diff = Seconds.secondsBetween(this_start, this_end).getSeconds();
                this_bytes = (int) Math.ceil(bytes * diff / totalDiff);
                if (this_bytes < 1) this_bytes = 1;
                this_pkts = (int) Math.ceil(pkts * diff / totalDiff);
                if (this_pkts < 1) this_pkts = 1;
                bytes_count += this_bytes;
                pkts_count += this_pkts;
                
                Map<String, Object> to_send = new HashMap<>();
                to_send.putAll(event);
                to_send.put("timestamp", this_end.getMillis() / 1000);
                to_send.put("bytes", this_bytes);
                to_send.put("pkts", this_pkts);
                generatedPackets.add(to_send);
            } while (this_end.isBefore(packet_end));
            
            if (bytes != bytes_count || pkts != pkts_count) {
                int last_index = generatedPackets.size() - 1;
                Map<String, Object> last = generatedPackets.get(last_index);
                int new_pkts = ((int) last.get("pkts")) + (pkts - pkts_count);
                int new_bytes = ((int) last.get("bytes")) + (bytes - bytes_count);
                
                if (new_pkts > 0) last.put("pkts", new_pkts);
                if (new_bytes > 0) last.put("bytes", new_pkts);
                
                generatedPackets.set(last_index, last);
            }
            
            //System.out.println("-------------------------------");
            for (Map<String, Object> e : generatedPackets) {
                collector.emit(new Values(e));
                //System.out.println(e);
            }
            //System.out.println("-------------------------------");
        } else {
            collector.emit(new Values(event));
        }
    }
}
