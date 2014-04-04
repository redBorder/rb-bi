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
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.MaxAggregatorFactory;
import io.druid.query.aggregation.MinAggregatorFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.List;
import java.util.Map;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.retry.RetryOneTime;

/**
 * BeamFactory is used to make the BeamStateMonitor to Tranquility.
 *
 * @author andresgomez
 */
public class MyBeamFactoryMapFlow implements BeamFactory<Map<String, Object>> {

    String _zkConnect;
    String _topic;

    /**
     * Consturctor.
     *
     * @param zkConfig Class GetKafkaConfig with the selected topic.
     */
    public MyBeamFactoryMapFlow(GetKafkaConfig zkConfig) {
        _zkConnect = zkConfig.getZkConnect();
        _topic = zkConfig.getTopic();

    }

    @Override
    public Beam<Map<String, Object>> makeBeam(Map<?, ?> conf, IMetricsContext metrics) {
        try {
            final CuratorFramework curator = CuratorFrameworkFactory
                    .builder()
                    .connectString(_zkConnect)
                    .retryPolicy(new RetryOneTime(1000))
                    .build();
            
            curator.start();

            final String dataSource = _topic;
            final List<String> exclusions = ImmutableList.of(
                    "http_url", "http_user_agent", "first_switched", "transaction_id",
                    "flow_end_reason", "flow_sampler_id", "src_name", "dst_name",
                    "vlan_name", "src_port_name", "dst_port_name", "l4_proto_name",
                    "tcp_flags", "srv_port_name", "type", "src_country", "dst_country",
                    "sta_mac_address_unit", "application_id", "engine_id", "src_as",
                    "dst_as", "second", "bytes", "pkts", "sta_mac_address_lat",
                    "sta_mac_address_long", "src_net", "dst_net");
            final List<AggregatorFactory> aggregators = ImmutableList.<AggregatorFactory>of(
                    new CountAggregatorFactory("events"),
                    new LongSumAggregatorFactory("sum_bytes", "bytes"),
                    new LongSumAggregatorFactory("sum_pkts", "pkts")
            );

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
                    .rollup(DruidRollup.create(DruidDimensions.schemalessWithExclusions(exclusions), aggregators, QueryGranularity.NONE))
                    .tuning(ClusteredBeamTuning.create(Granularity.HOUR, new Period("PT0M"), new Period("PT30M"), 1, 1));

            final Beam<Map<String, Object>> beam = builder.buildBeam();

            return beam;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
