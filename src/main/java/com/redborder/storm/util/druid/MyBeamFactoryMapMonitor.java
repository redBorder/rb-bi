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
import com.metamx.tranquility.druid.DruidDimensions;
import com.metamx.tranquility.druid.DruidEnvironment;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.druid.DruidRollup;
import com.metamx.tranquility.storm.BeamFactory;
import com.metamx.tranquility.typeclass.Timestamper;
import com.redborder.storm.util.GetKafkaConfig;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.MaxAggregatorFactory;
import io.druid.query.aggregation.MinAggregatorFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.List;
import java.util.Map;
import org.apache.curator.retry.RetryOneTime;

/**
 * BeamFactory is used to make the BeamStateMonitor to Tranquility.
 * @author andresgomez
 */
public class MyBeamFactoryMapMonitor implements BeamFactory<Map<String, Object>> {

    String _zkConnect;
    String _topic;

    /**
     * Consturctor.
     *
     * @param zkConfig Class GetKafkaConfig with the selected topic.
     */
    public MyBeamFactoryMapMonitor(GetKafkaConfig zkConfig) {
        _zkConnect = zkConfig.getZkConnect();
        _topic = zkConfig.getTopic();

    }

    @Override
    public Beam<Map<String, Object>> makeBeam(Map<?, ?> conf, IMetricsContext metrics) {
        try {
           // final CuratorFramework curator = CuratorFrameworkFactory.newClient(
           //         _zkConnect, new BoundedExponentialBackoffRetry(100, 1000, 5));
            final CuratorFramework curator = CuratorFrameworkFactory
                    .builder()
                    .connectString(_zkConnect)
                    .retryPolicy(new RetryOneTime(1000))
                    .build();
            
            curator.start();

            final String dataSource = _topic;
            final List<String> dimensions = ImmutableList.of(
                    "sensor_name", "monitor", "value", "type", "unit");
            final List<String> exclusions = ImmutableList.of(
            "unit", "type");
            final List<AggregatorFactory> aggregators = ImmutableList.<AggregatorFactory>of(
                    new CountAggregatorFactory("events"),
                    new DoubleSumAggregatorFactory("sum_value", "value"),
                    new MaxAggregatorFactory("max_value", "value"),
                    new MinAggregatorFactory("min_value", "value"));

            final DruidBeams.Builder<Map<String, Object>> builder = DruidBeams
                    .builder(
                            new Timestamper<Map<String, Object>>() {
                                @Override
                                public DateTime timestamp(Map<String, Object> theMap) {
                                    Long date = Long.parseLong(theMap.get("timestamp").toString());
                                    date = date * 1000;
                                    return new DateTime(date.longValue());   
                                }
                            }
                    )
                    .curator(curator)
                    .discoveryPath("/druid/discoveryPath")
                    .location(
                            new DruidLocation(
                                    new DruidEnvironment(
                                            "overlord",
                                            "druid:local:firehose:%s"
                                    ), dataSource
                            )
                    )
                    .rollup(DruidRollup.create(DruidDimensions.schemalessWithExclusions(exclusions), aggregators, QueryGranularity.MINUTE))
                    .tuning(ClusteredBeamTuning.create(Granularity.HOUR, new Period("PT0M"), new Period("PT30M"), 1, 1))
                    .timestampSpec(new TimestampSpec("timestamp", "posix"));

            final Beam<Map<String, Object>> beam = builder.buildBeam();

            return beam;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
