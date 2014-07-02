package net.redborder.metrics;

import backtype.storm.metric.api.IMetric;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by andresgomez on 23/06/14.
 */
public class CountMetric implements IMetric {

    Map<String, Object> metrics;
    Double events;

    public CountMetric(){
        metrics = new HashMap<String, Object>();
        events=0.000;
    }

    @Override
    public Object getValueAndReset() {

        metrics.put("value", String.valueOf(events/50));

        events = 0.000;

        return metrics;
    }

    public void incrEvent(){
        events++;
    }


}
