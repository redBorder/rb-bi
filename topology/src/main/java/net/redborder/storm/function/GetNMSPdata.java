package net.redborder.storm.function;


import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by andresgomez on 4/12/14.
 */
public class GetNMSPdata extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> nmspEvent = (Map<String, Object>) tuple.get(0);
        String nmspType = (String) nmspEvent.get("type");

        if (nmspType.toLowerCase().equals("measure")) {
            List<Map<String, Object>> datas = (List<Map<String, Object>>) nmspEvent.get("data");
            for (Map<String, Object> data : datas) {
                System.out.println("Sending MEAS: " + data.toString());
                collector.emit(new Values(data, null));
            }
        } else if (nmspType.toLowerCase().equals("info")) {
            List<Map<String, Object>> datas = (List<Map<String, Object>>) nmspEvent.get("data");
            for (Map<String, Object> data : datas) {
                System.out.println("Sending INFO: " + data.toString());
                collector.emit(new Values(null, data));
            }
        } else {
            System.out.println("NMSP TYPE NOT SUPPORTED: " + nmspType);
        }


    }
}
