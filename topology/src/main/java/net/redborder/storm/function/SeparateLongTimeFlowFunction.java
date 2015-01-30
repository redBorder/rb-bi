/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import net.redborder.metrics.CountMetric;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author andresgomez
 */
public class SeparateLongTimeFlowFunction extends BaseFunction {

    boolean _debug;
    CountMetric _metric;
    long logMark;


    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _debug = (boolean) conf.get("rbDebug");
        _metric = context.registerMetric("throughput_" + "rb_flow_output", new CountMetric(), 50);
        logMark = System.currentTimeMillis();
    }

    public final int DELAYED_REALTIME_TIME = 15;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);
        List<Map<String, Object>> generatedPackets = new ArrayList<>();

        // last_switched is timestamp now
        if (event.containsKey("first_switched") && event.containsKey("timestamp")) {
            DateTime packet_start = new DateTime(Long.parseLong(event.get("first_switched").toString()) * 1000);
            DateTime packet_end = new DateTime(Long.parseLong(event.get("timestamp").toString()) * 1000);
            DateTime now = new DateTime();
            int now_hour = now.getHourOfDay();
            int packet_end_hour = packet_end.getHourOfDay();

            // Get the lower limit date time that a packet can have
            DateTime limit;
            if (now.getMinuteOfHour() < DELAYED_REALTIME_TIME) {
                limit = new DateTime().minusHours(1).withMinuteOfHour(0);
            } else {
                limit = new DateTime().withMinuteOfHour(0);
            }

            // Discard too old events
            if ((packet_end_hour == now_hour - 1 && now.getMinuteOfHour() > DELAYED_REALTIME_TIME) ||
                    (now.getMillis() - packet_end.getMillis() > 1000 * 60 * 60)) {
                if ((logMark+300000)>System.currentTimeMillis()) {
                    Logger.getLogger(SeparateLongTimeFlowFunction.class.getName()).log(Level.WARNING,
                            "Dropped packet {0} because its realtime processor is already shutdown.", event);
                    logMark = System.currentTimeMillis();
                }
                return;
            } else if (packet_start.isBefore(limit)) {
                // If the lower limit date time is overpassed, correct it
                if ((logMark+300000)>System.currentTimeMillis()) {
                    Logger.getLogger(SeparateLongTimeFlowFunction.class.getName()).log(Level.WARNING,
                            "Packet {0} first switched was corrected because it overpassed the lower limit (event too old).", event);
                    logMark = System.currentTimeMillis();
                }

                packet_start = limit;
                event.put("first_switched", limit.getMillis() / 1000);
            }

            // Correct events in the future
            if (packet_end.isAfter(now) && ((packet_end.getHourOfDay() != packet_start.getHourOfDay()) ||
                    (packet_end.getMillis() - now.getMillis() > 1000 * 60 * 60))) {

                if ((logMark+300000)>System.currentTimeMillis()) {
                    Logger.getLogger(SeparateLongTimeFlowFunction.class.getName()).log(Level.WARNING,
                            "Packet {0} ended in a future segment and I modified its last and/or first switched values.", event);
                    logMark = System.currentTimeMillis();
                }
                event.put("timestamp", now.getMillis() / 1000);
                packet_end = now;

                if (!packet_end.isAfter(packet_start)) {
                    event.put("first_switched", now.getMillis() / 1000);
                    packet_start = now;
                }
            }

            DateTime this_start;
            DateTime this_end = packet_start;

            long bytes = 0;
            long pkts = 0;

            try {
                if (event.containsKey("bytes"))
                    bytes = Integer.parseInt(event.get("bytes").toString());
            } catch (NumberFormatException e) {
                Logger.getLogger(SeparateLongTimeFlowFunction.class.getName()).log(Level.WARNING,
                        "Invalid number of bytes in packet {0}.", event);
                return;
            }

            try {
                if (event.containsKey("pkts"))
                    pkts = Integer.parseInt(event.get("pkts").toString());
            } catch (NumberFormatException e) {
                Logger.getLogger(SeparateLongTimeFlowFunction.class.getName()).log(Level.WARNING,
                        "Invalid number of packets in packet {0}.", event);
                return;
            }

            long totalDiff = Seconds.secondsBetween(packet_start, packet_end).getSeconds();
            long diff, this_bytes, this_pkts;
            long bytes_count = 0;
            long pkts_count = 0;

            do {
                this_start = this_end;
                this_end = this_start.plusSeconds(60 - this_start.getSecondOfMinute());
                if (this_end.isAfter(packet_end)) this_end = packet_end;
                diff = Seconds.secondsBetween(this_start, this_end).getSeconds();

                if (totalDiff == 0) this_bytes = bytes;
                else this_bytes = (long) Math.ceil(bytes * diff / totalDiff);

                if (totalDiff == 0) this_pkts = pkts;
                else this_pkts = (long) Math.ceil(pkts * diff / totalDiff);

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
                long new_pkts = ((long) last.get("pkts")) + (pkts - pkts_count);
                long new_bytes = ((long) last.get("bytes")) + (bytes - bytes_count);

                if (new_pkts > 0) last.put("pkts", new_pkts);
                if (new_bytes > 0) last.put("bytes", new_bytes);

                generatedPackets.set(last_index, last);
            }

            for (Map<String, Object> e : generatedPackets) {
                e.remove("first_switched");
                _metric.incrEvent();
                collector.emit(new Values(e));
            }
        } else if (event.containsKey("timestamp")) {
            Long bytes = Long.parseLong(event.get("bytes").toString());
            event.put("bytes", bytes);
            _metric.incrEvent();
            collector.emit(new Values(event));
        } else {
            Long bytes = Long.parseLong(event.get("bytes").toString());
            event.put("bytes", bytes);
            Logger.getLogger(SeparateLongTimeFlowFunction.class.getName()).log(Level.WARNING,
                    "Packet without timestamp -> {0}.", event);
            _metric.incrEvent();
            collector.emit(new Values(event));
        }
    }
}
