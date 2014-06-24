package net.redborder.storm.metrics;

import backtype.storm.metric.api.IMetric;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by andresgomez on 23/06/14.
 */
public class CountMetric implements IMetric {

    Map<String, Object> metrics;
    long events;

    public CountMetric(){
        metrics = new HashMap<String, Object>();
        events=0;
    }

    @Override
    public Object getValueAndReset() {

        metrics.put("value", events/50);

        events = 0;

        return metrics;
    }

    public void incrEvent(){
        events++;
    }


}
