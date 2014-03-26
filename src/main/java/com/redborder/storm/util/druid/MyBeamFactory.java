/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.redborder.storm.util.druid;

import backtype.storm.task.IMetricsContext;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.Beam;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.druid.DruidEnvironment;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.druid.DruidRollup;
import com.metamx.tranquility.storm.BeamFactory;
import com.metamx.tranquility.typeclass.Timestamper;
import com.redborder.storm.util.GetKafkaConfig;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.MaxAggregatorFactory;
import io.druid.query.aggregation.MinAggregatorFactory;
import java.io.IOException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author andresgomez
 */
public class MyBeamFactory implements BeamFactory<String> {

    String _zkConnect;
    String _topic;
    
    public MyBeamFactory(GetKafkaConfig zkConfig){
        _zkConnect=zkConfig.getZkConnect();
        _topic=zkConfig.getTopic();
    }
    
    @Override
    public Beam<String> makeBeam(Map<?, ?> map, IMetricsContext imc) {
        try {

            final CuratorFramework curator = CuratorFrameworkFactory.newClient(
                    _zkConnect, new  BoundedExponentialBackoffRetry(100, 1000, 5));
            curator.start();

            final String dataSource = _topic;
            final List<String> dimensions = ImmutableList.of("sensor_id","sensor_name", "monitor", "value");
            final List<AggregatorFactory> aggregators = ImmutableList.<AggregatorFactory>of(
                    new CountAggregatorFactory("events"),
                    new DoubleSumAggregatorFactory("sum_value", "value"),
                    new MaxAggregatorFactory("max_value", "value"),
                    new MinAggregatorFactory("min_value", "value")
            );
            
            final DruidBeams.Builder<String> builder = DruidBeams
                    .builder(
                            new Timestamper<String>() {
                                @Override
                                public DateTime timestamp(String theMap) {
                                    ObjectMapper mapper = new ObjectMapper();
                                    Map<String, Object> map = null;
                                    try {
                                        map = mapper.readValue(theMap, Map.class);
                                    } catch (IOException ex) {
                                        Logger.getLogger(MyBeamFactory.class.getName()).log(Level.SEVERE, null, ex);
                                    }

                                    return new DateTime(map.get("timestamp"));
                                }
                            }
                    )
                    .curator(curator)
                    .discoveryPath("/druid/discovery")
                    .location(
                            new DruidLocation(
                                    new DruidEnvironment(
                                            "druid:overlord",
                                            "druid:prod:local"
                                    ), dataSource
                            )
                    )
                    .rollup(DruidRollup.create(dimensions, aggregators, QueryGranularity.NONE))
                    .tuning(ClusteredBeamTuning.create(Granularity.HOUR, new Period("PT0M"), new Period("PT10M"), 2, 2));

            final Beam<String> beam = builder.buildBeam();

            return beam;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
