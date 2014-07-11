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
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.druid.DruidDimensions;
import com.metamx.tranquility.druid.DruidEnvironment;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.druid.DruidRollup;
import com.metamx.tranquility.storm.BeamFactory;
import com.metamx.tranquility.typeclass.Timestamper;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.List;
import java.util.Map;
import org.apache.curator.retry.RetryOneTime;

/**
 * BeamFactory is used to make the BeamStateMonitor to Tranquility.
 *
 * @author andresgomez
 */
public class BeamFlow implements BeamFactory<Map<String, Object>> {
    
    int partitions;
    int replicas;
    String zk;
    
    public BeamFlow(int partitions, int replicas, String zk){
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

            final String dataSource = "rb_flow";
            final List<String> dimesions = ImmutableList.of(
                    "application_id_name", "biflow_direction", "conversation", "direction",
                    "engine_id_name", "http_user_agent_os", "http_host", "http_social_media",
                    "http_social_user", "http_referer_l1", "l4_proto", "ip_protocol_version",
                    "sensor_name", "scatterplot", "src", "src_country_code", "src_net_name",
                    "src_port", "src_as_name", "client_mac", "client_id", "client_mac_vendor",
                    "dot11_status", "src_vlan", "src_map", "srv_port", "dst",
                    "dst_country_code", "dst_net_name", "dst_port", "dst_as_name",
                    "dst_vlan", "dst_map", "input_snmp", "output_snmp", "tos",
                    "client_latlong", "coordinates_map", "client_campus",
                    "client_building", "client_floor", "wireless_id", "client_rssi",
                    "wireless_station", "hnblocation", "hnbgeolocation", "rat",
                    "darklist_score_name", "darklist_category", "darklist_protocol",
                    "darklist_direction", "darklist_score");
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
                    .rollup(DruidRollup.create(DruidDimensions.specific(dimesions), aggregators, QueryGranularity.MINUTE))
                    .tuning(ClusteredBeamTuning.create(Granularity.HOUR, new Period("PT0M"), new Period("PT15M"), partitions, replicas))
                    .timestampSpec(new TimestampSpec("timestamp", "posix"));
            
            final Beam<Map<String, Object>> beam = builder.buildBeam();

            return beam;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
