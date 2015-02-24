package net.redborder.storm.function;


import backtype.storm.tuple.Values;
import net.redborder.storm.util.logger.RbLogger;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 4/12/14.
 */
public class GetNMSPdata extends BaseFunction {

    Logger logger;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        logger = RbLogger.getLogger(GetNMSPdata.class.getName());
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> nmspEvent = (Map<String, Object>) tuple.get(0);
        String sensor_name = (String) nmspEvent.get("sensor_name");
        Map<String, Object> enrichment = (Map<String, Object>) nmspEvent.get("enrichment");
        String nmspType = (String) nmspEvent.get("type");

        if (nmspType.toLowerCase().equals("measure")) {
            List<Map<String, Object>> datas = (List<Map<String, Object>>) nmspEvent.get("data");
            logger.severe("Sending nmsp events [measure]: " + datas.size());
            for (Map<String, Object> data : datas) {
                data.put("sensor_name", sensor_name);
                if (enrichment != null)
                    data.put("enrichment", enrichment);
                collector.emit(new Values(data, null));
            }
        } else if (nmspType.toLowerCase().equals("info")) {
            List<Map<String, Object>> datas = (List<Map<String, Object>>) nmspEvent.get("data");
            logger.severe("Sending nmsp events [info]: " + datas.size());
            for (Map<String, Object> data : datas) {
                data.put("sensor_name", sensor_name);
                if (enrichment != null)
                    data.put("enrichment", enrichment);
                collector.emit(new Values(null, data));
            }
        } else {
            logger.warning("NMSP TYPE NOT SUPPORTED: " + nmspType);
        }


    }
}
