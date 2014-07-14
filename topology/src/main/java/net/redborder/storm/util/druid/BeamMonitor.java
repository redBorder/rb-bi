/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.util.druid;

import backtype.storm.task.IMetricsContext;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.Beam;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.druid.*;
import com.metamx.tranquility.storm.BeamFactory;
import com.metamx.tranquility.typeclass.Timestamper;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.List;
import java.util.Map;

/**
 * BeamFactory is used to make the BeamStateMonitor to Tranquility.
 * @author andresgomez
 */
public class BeamMonitor implements BeamFactory<Map<String, Object>> {

    int partitions;
    int replicas;
    String zk;
    
    public BeamMonitor(int partitions, int replicas, String zk) {
        this.partitions = partitions;
        this.replicas = replicas;
        this.zk = zk;
    }
    
    @Override
    public Beam<Map<String, Object>> makeBeam(Map<?, ?> conf, IMetricsContext metrics) {
        try {
            final CuratorFramework curator = CuratorFrameworkFactory
                    .builder()
                    .connectString(zk)
                    .retryPolicy(new RetryOneTime(1000))
                    .build();
            
            curator.start();

            final String dataSource = "rb_monitor";
            final List<String> exclusions = ImmutableList.of("unit", "type");
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
                    .tuning(ClusteredBeamTuning.create(Granularity.HOUR, new Period("PT0M"), new Period("PT30M"), partitions, replicas))
                    .timestampSpec(new TimestampSpec("timestamp", "posix"));

            final Beam<Map<String, Object>> beam = builder.buildBeam();

            return beam;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
