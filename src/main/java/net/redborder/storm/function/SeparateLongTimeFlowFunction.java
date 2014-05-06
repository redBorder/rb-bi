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

    private class PacketData {
        long first_timestamp;
        long second_timestamp;
        int secs_first;
        int secs_second;
        int secs_total;
        int first_bytes;
        int first_pkts;
        int second_bytes;
        int second_pkts;
    };
    
    private List<Map<String, Object>> dividePkts(PacketData info, Map<String, Object> event) {
        List<Map<String, Object>> ret = new ArrayList<>();
        
        Map<String, Object> first = new HashMap<>();
        first.putAll(event);
        first.put("timestamp", info.first_timestamp);
        first.put("bytes", info.first_bytes);
        first.put("pkts", info.first_pkts);
        ret.add(first);
                    
        Map<String, Object> second = new HashMap<>();
        second.putAll(event);
        second.put("timestamp", info.second_timestamp);
        second.put("bytes", info.second_bytes);
        second.put("pkts", info.second_pkts);
        ret.add(second);
        
        return ret;
    }
    
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
            
            PacketData info = new PacketData();
            info.first_timestamp = start.getMillis() / 1000;
            info.second_timestamp = end.getMillis() / 1000;
            info.secs_first = 60 - start.getSecondOfMinute();
            info.secs_second = end.getSecondOfMinute();
            info.secs_total = info.secs_first + info.secs_second;
            info.first_bytes = info.secs_first * bytes / info.secs_total;
            info.first_pkts = info.secs_first * pkts / info.secs_total;
            info.second_bytes = bytes - info.first_bytes;
            info.second_pkts = pkts - info.first_pkts;
            
            if (diff.getSeconds() < 60) {
                int min_start = start.getMinuteOfDay();
                int min_end = end.getMinuteOfDay();
                
                if (min_start == min_end) {
                    // No dividir
                    event.put("timestamp", event.get("last_switched"));
                    listToSend.add(event);
                    System.out.println("This packet wasnt divided" + event);
                } else {
                    // Dividir en dos paquetes            
                    listToSend.addAll(dividePkts(info, event));
                    System.out.println("This packet was divided into two: " + event);
                }
            } else {
                int intervals = diff.getSeconds() / 60;
                int total_bytes = bytes - info.first_bytes - info.second_bytes;
                int total_pkts = pkts - info.first_pkts - info.second_pkts;
                int remain_bytes = total_bytes;
                int remain_pkts = total_pkts;
                int bytes_per_interval = total_bytes / 60;
                int pkts_per_interval = total_pkts / 60;
                                
                for (int i = 0; i < intervals; i++) {
                    remain_bytes -= bytes_per_interval;
                    remain_pkts -= pkts_per_interval;
                    
                    Map<String, Object> to_send = new HashMap<>();
                    to_send.putAll(event);
                    to_send.put("timestamp", info.first_timestamp + i * 60);
                    to_send.put("bytes", bytes_per_interval);
                    to_send.put("pkts", pkts_per_interval);
                    listToSend.add(to_send);
                }
                
                if (remain_bytes > 0) info.second_bytes += remain_bytes;
                if (remain_pkts > 0) info.second_pkts += remain_pkts;
                
                listToSend.addAll(dividePkts(info, event));
                System.out.println("This packet was divided into " + listToSend.size() + " packets: " + event);
            }
            
            System.out.println("-------------------------------");
            for (Map<String, Object> e : listToSend) {
                collector.emit(new Values(e));
                System.out.println(e);
            }
            System.out.println("-------------------------------");
        } else {
            collector.emit(new Values(event));
        }
    }

}
