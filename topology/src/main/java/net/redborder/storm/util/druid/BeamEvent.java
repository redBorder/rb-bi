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
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.List;
import java.util.Map;

/**
 * BeamFactory is used to make the BeamStateMonitor to Tranquility.
 *
 * @author andresgomez
 */
public class BeamEvent implements BeamFactory<Map<String, Object>> {

    int partitions;
    int replicas;
    String zk;

    public BeamEvent(int partitions, int replicas, String zk) {
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

            final String dataSource = "rb_event";
            final List<String> dimensions = ImmutableList.of("action", "classification", "conversation", "domain_name",
                    "ethlength_range", "group_name", "sig_generator", "icmptype",
                    "iplen_range", "l4_proto", "rev", "sensor_name",
                    "priority", "msg", "sig_id", "scatterplot", "ethsrc",
                    "ethsrc_vendor", "src", "src_country_code",
                    "src_net_name", "src_port", "src_as_name",
                    "src_map", "ethdst", "ethdst_vendor", "dst",
                    "dst_country_code", "dst_net_name",
                    "dst_port", "dst_as_name", "dst_map", "tos",
                    "ttl", "vlan", "darklist_score_name", "darklist_category",
                    "darklist_protocol", "darklist_direction", "darklist_score");
            final List<AggregatorFactory> aggregators = ImmutableList.<AggregatorFactory>of(
                    new CountAggregatorFactory("events"));

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
                    .rollup(DruidRollup.create(DruidDimensions.specific(dimensions), aggregators, QueryGranularity.MINUTE))
                    .druidTuning(DruidTuning.create(80000, new Period("PT10M"), 3))
                    .tuning(ClusteredBeamTuning.builder()
                            .partitions(partitions)
                            .replicants(replicas)
                            .segmentGranularity(Granularity.HOUR)
                            .warmingPeriod(new Period("PT0M"))
                            .windowPeriod(new Period("PT15M"))
                            .build())
                    .timestampSpec(new TimestampSpec("timestamp", "posix"));

            final Beam<Map<String, Object>> beam = builder.buildBeam();

            return beam;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
