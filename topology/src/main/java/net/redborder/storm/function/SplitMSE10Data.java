package net.redborder.storm.function;


import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 16/2/15.
 */
public class SplitMSE10Data extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String , Object> mseData = (Map<String, Object>) tuple.get(0);
        List<Map<String, Object>> notifications = (List<Map<String, Object>>) mseData.get("notifications");

        for (Map<String, Object> notification : notifications){
            if(notification.get("notificationType").equals("association")){
                collector.emit(new Values(notification, null));
            }else if(notification.get("notificationType").equals("locationupdate")){
                collector.emit(new Values(null, notification));
            }else{
                Logger.getLogger(SplitMSE10Data.class.getName()).log(Level.WARNING, "MSE version 10 notificationType is unknown: " + notification.get("notificationType"));
            }
        }
    }
}
