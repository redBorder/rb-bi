/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.redborder.storm.util.scheme;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author andresgomez
 */
public class SchemeEvent_rb_flow {
    @JsonProperty("bytes")
    public String bytes = null;
    @JsonProperty("system_id")
    public String system_id = null;
    @JsonProperty("pkts")
    public String pkts = null;
    @JsonProperty("interface_id")
    public String interface_id = null;
    @JsonProperty("flows")
    public String flows = null;
    @JsonProperty("line_card")
    public String line_card = null;
    @JsonProperty("l4_proto")
    public String l4_proto = null;
    @JsonProperty("l4_proto_name")
    public String l4_proto_name = null;
    @JsonProperty("netflow_cache")
    public String netflow_cache = null;
    @JsonProperty("tos")
    public String tos = null;
    @JsonProperty("template_id")
    public String template_id = null;
    @JsonProperty("tcp_flags")
    public String tcp_flags = null;
    @JsonProperty("src_port")
    public String src_port = null;
    @JsonProperty("src_port_name")
    public String src_port_name = null;
    @JsonProperty("src_name")
    public String src_name = null;
    @JsonProperty("src_net_name")
    public String src_net_name = null;
    @JsonProperty("src_net")
    public String src_net = null;
    @JsonProperty("ipv4_src_mask")
    public String ipv4_src_mask = null;
    @JsonProperty("input_snmp")
    public String input_snmp = null;
    @JsonProperty("dst_port")
    public String dst_port = null;
    @JsonProperty("dst_port_name")
    public String dst_port_name = null;
    @JsonProperty("srv_port")
    public String srv_port = null;
    @JsonProperty("srv_port_map")
    public String srv_port_map = null;
    @JsonProperty("dst")
    public String dst = null;
    @JsonProperty("dst_asnum")
    public String dst_asnum = null;
    @JsonProperty("dst_name")
    public String dst_name = null;
    @JsonProperty("dst_net")
    public String dst_net = null;
    @JsonProperty("ipv4_dst_mask")
    public String ipv4_dst_mask = null;
    @JsonProperty("output_snmp")
    public String output_snmp = null;
    @JsonProperty("ipv4_next_hop")
    public String ipv4_next_hop = null;
    @JsonProperty("src_as")
    public String src_as = null;
    @JsonProperty("dst_as")
    public String dst_as = null;
    @JsonProperty("src_as_name")
    public String src_as_name = null;
    @JsonProperty("dst_as_name")
    public String dst_as_name = null;
    @JsonProperty("timestamp")
    public String timestamp = null;
    @JsonProperty("first_switched")
    public String first_switched = null;
    @JsonProperty("out_bytes")
    public String out_bytes = null;
    @JsonProperty("out_pkts")
    public String out_pkts = null;
    @JsonProperty("in_out_bytes")
    public String in_out_bytes = null;
    @JsonProperty("in_out_pkts")
    public String in_out_pkts = null;
    @JsonProperty("ipv6_src_addr")
    public String ipv6_src_addr = null;
    @JsonProperty("ipv6_dst_addr")
    public String ipv6_dst_addr = null;
    @JsonProperty("ipv6_src_mask")
    public String ipv6_src_mask = null;
    @JsonProperty("ipv6_dst_mask")
    public String ipv6_dst_mask = null;
    @JsonProperty("src_asnum")
    public String src_asnum = null;
    @JsonProperty("dst_net_name")
    public String dst_net_name = null;
    @JsonProperty("icmp_type")
    public String icmp_type = null;
    @JsonProperty("sampling_interval")
    public String sampling_interval = null;
    @JsonProperty("sampling_algorithm")
    public String sampling_algorithm = null;
    @JsonProperty("flow_active_timeout")
    public String flow_active_timeout = null;
    @JsonProperty("flow_inactive_timeout")
    public String flow_inactive_timeout = null;
    @JsonProperty("engine_type")
    public String engine_type = null;
    @JsonProperty("engine_id")
    public String engine_id = null;
    @JsonProperty("engine_id_name")
    public String engine_id_name = null;
    @JsonProperty("total_bytes_exp")
    public String total_bytes_exp = null;
    @JsonProperty("total_pkts_exp")
    public String total_pkts_exp = null;
    @JsonProperty("total_flows_exp")
    public String total_flows_exp = null;
    @JsonProperty("flow_sampler_id")
    public String flow_sampler_id = null;
    @JsonProperty("min_ttl")
    public String min_ttl = null;
    @JsonProperty("max_ttl")
    public String max_ttl = null;
    @JsonProperty("in_src_mac")
    public String in_src_mac = null;
    @JsonProperty("out_src_mac")
    public String out_src_mac = null;
    @JsonProperty("in_src_mac_name")
    public String in_src_mac_name = null;
    @JsonProperty("out_src_mac_name")
    public String out_src_mac_name = null;
    @JsonProperty("in_src_mac_vendor")
    public String in_src_mac_vendor = null;
    @JsonProperty("out_src_mac_vendor")
    public String out_src_mac_vendor = null;
    @JsonProperty("cisco_src_vlan")
    public String cisco_src_vlan = null;
    @JsonProperty("cisco_dst_vlan")
    public String cisco_dst_vlan = null;
    @JsonProperty("cisco_src_vlan_name")
    public String cisco_src_vlan_name = null;
    @JsonProperty("cisco_dst_vlan_name")
    public String cisco_dst_vlan_name = null;
    @JsonProperty("src_vlan")
    public String src_vlan = null;
    @JsonProperty("dst_vlan")
    public String dst_vlan = null;
    @JsonProperty("src_vlan_name")
    public String src_vlan_name = null;
    @JsonProperty("dst_vlan_name")
    public String dst_vlan_name = null;
    @JsonProperty("ip_protocol_version")
    public String ip_protocol_version = null;
    @JsonProperty("direction")
    public String direction = null;
    @JsonProperty("ipv6_next_hop")
    public String ipv6_next_hop = null;
    @JsonProperty("mpls_label_1")
    public String mpls_label_1 = null;
    @JsonProperty("mpls_label_2")
    public String mpls_label_2 = null;
    @JsonProperty("mpls_label_3")
    public String mpls_label_3 = null;
    @JsonProperty("mpls_label_4")
    public String mpls_label_4 = null;
    @JsonProperty("mpls_label_5")
    public String mpls_label_5 = null;
    @JsonProperty("mpls_label_6")
    public String mpls_label_6 = null;
    @JsonProperty("mpls_label_7")
    public String mpls_label_7 = null;
    @JsonProperty("mpls_label_8")
    public String mpls_label_8 = null;
    @JsonProperty("mpls_label_9")
    public String mpls_label_9 = null;
    @JsonProperty("mpls_label_10")
    public String mpls_label_10 = null;
    @JsonProperty("in_dst_mac")
    public String in_dst_mac = null;
    @JsonProperty("in_dst_mac_name")
    public String in_dst_mac_name = null;
    @JsonProperty("in_dst_mac_vendor")
    public String in_dst_mac_vendor = null;
    @JsonProperty("out_dst_mac")
    public String out_dst_mac = null;
    @JsonProperty("out_dst_mac_name")
    public String out_dst_mac_name = null;
    @JsonProperty("out_dst_mac_vendor")
    public String out_dst_mac_vendor = null;
    @JsonProperty("application_id")
    public String application_id = null;
    @JsonProperty("application_id_name")
    public String application_id_name = null;
    @JsonProperty("packet_section_offset")
    public String packet_section_offset = null;
    @JsonProperty("sampled_packet_size")
    public String sampled_packet_size = null;
    @JsonProperty("sampled_packet_id")
    public String sampled_packet_id = null;
    @JsonProperty("sensor_ip")
    public String sensor_ip = null;
    @JsonProperty("sensor_name")
    public String sensor_name = null;
    @JsonProperty("exporter_ipv6_address")
    public String exporter_ipv6_address = null;
    @JsonProperty("flow_end_reason")
    public String flow_end_reason = null;
    @JsonProperty("flow_id")
    public String flow_id = null;
    @JsonProperty("flow_start_sec")
    public String flow_start_sec = null;
    @JsonProperty("flow_end_sec")
    public String flow_end_sec = null;
    @JsonProperty("flow_start_milliseconds")
    public String flow_start_milliseconds = null;
    @JsonProperty("flow_end_milliseconds")
    public String flow_end_milliseconds = null;
    @JsonProperty("biflow_direction")
    public String biflow_direction = null;
    @JsonProperty("observation_point_type")
    public String observation_point_type = null;
    @JsonProperty("transaction_id")
    public String transaction_id = null;
    @JsonProperty("observation_point_id")
    public String observation_point_id = null;
    @JsonProperty("selector_id")
    public String selector_id = null;
    @JsonProperty("ipfix_sampling_algorithm")
    public String ipfix_sampling_algorithm = null;
    @JsonProperty("sampling_size")
    public String sampling_size = null;
    @JsonProperty("sampling_population")
    public String sampling_population = null;
    @JsonProperty("frame_length")
    public String frame_length = null;
    @JsonProperty("packets_observed")
    public String packets_observed = null;
    @JsonProperty("packets_selected")
    public String packets_selected = null;
    @JsonProperty("selector_name")
    public String selector_name = null;
    @JsonProperty("fragments")
    public String fragments = null;
    @JsonProperty("client_nw_delay_sec")
    public String client_nw_delay_sec = null;
    @JsonProperty("client_nw_delay_usec")
    public String client_nw_delay_usec = null;
    @JsonProperty("client_nw_delay_ms")
    public String client_nw_delay_ms = null;
    @JsonProperty("server_nw_delay_sec")
    public String server_nw_delay_sec = null;
    @JsonProperty("server_nw_delay_usec")
    public String server_nw_delay_usec = null;
    @JsonProperty("server_nw_delay_ms")
    public String server_nw_delay_ms = null;
    @JsonProperty("appl_latency_sec")
    public String appl_latency_sec = null;
    @JsonProperty("appl_latency_usec")
    public String appl_latency_usec = null;
    @JsonProperty("appl_latency_ms")
    public String appl_latency_ms = null;
    @JsonProperty("num_pkts_up_to_128_bytes")
    public String num_pkts_up_to_128_bytes = null;
    @JsonProperty("num_pkts_128_to_256_bytes")
    public String num_pkts_128_to_256_bytes = null;
    @JsonProperty("num_pkts_256_to_512_bytes")
    public String num_pkts_256_to_512_bytes = null;
    @JsonProperty("num_pkts_512_to_1024_bytes")
    public String num_pkts_512_to_1024_bytes = null;
    @JsonProperty("num_pkts_1024_to_1514_bytes")
    public String num_pkts_1024_to_1514_bytes = null;
    @JsonProperty("num_pkts_over_1514_bytes")
    public String num_pkts_over_1514_bytes = null;
    @JsonProperty("cumulative_icmp_type")
    public String cumulative_icmp_type = null;
    @JsonProperty("src_country_code")
    public String src_country_code = null;
    @JsonProperty("src_country")
    public String src_country = null;
    @JsonProperty("src_city")
    public String src_city = null;
    @JsonProperty("dst_country_code")
    public String dst_country_code = null;
    @JsonProperty("dst_country")
    public String dst_country = null;
    @JsonProperty("dst_city")
    public String dst_city = null;
    @JsonProperty("flow_proto_port")
    public String flow_proto_port = null;
    @JsonProperty("upstream_tunnel_id")
    public String upstream_tunnel_id = null;
    @JsonProperty("longest_flow_pkt")
    public String longest_flow_pkt = null;
    @JsonProperty("shortest_flow_pkt")
    public String shortest_flow_pkt = null;
    @JsonProperty("retransmitted_in_pkts")
    public String retransmitted_in_pkts = null;
    @JsonProperty("retransmitted_out_pkts")
    public String retransmitted_out_pkts = null;
    @JsonProperty("ooorder_in_pkts")
    public String ooorder_in_pkts = null;
    @JsonProperty("ooorder_out_pkts")
    public String ooorder_out_pkts = null;
    @JsonProperty("l2_proto")
    public String l2_proto = null;
    @JsonProperty("untunneled_src")
    public String untunneled_src = null;
    @JsonProperty("untunneled_srcport")
    public String untunneled_srcport = null;
    @JsonProperty("untunneled_dst_str")
    public String untunneled_dst_str = null;
    @JsonProperty("untunneled_dstport")
    public String untunneled_dstport = null;
    @JsonProperty("l7_proto")
    public String l7_proto = null;
    @JsonProperty("l7_proto_name")
    public String l7_proto_name = null;
    @JsonProperty("downstream_tunnel_id")
    public String downstream_tunnel_id = null;
    @JsonProperty("flow_user_name")
    public String flow_user_name = null;
    @JsonProperty("flow_server_name")
    public String flow_server_name = null;
    @JsonProperty("plugin_name")
    public String plugin_name = null;
    @JsonProperty("num_pkts_ttl_eq_1")
    public String num_pkts_ttl_eq_1 = null;
    @JsonProperty("num_pkts_ttl_2_5")
    public String num_pkts_ttl_2_5 = null;
    @JsonProperty("num_pkts_ttl_5_32")
    public String num_pkts_ttl_5_32 = null;
    @JsonProperty("num_pkts_ttl_32_64")
    public String num_pkts_ttl_32_64 = null;
    @JsonProperty("num_pkts_ttl_64_96")
    public String num_pkts_ttl_64_96 = null;
    @JsonProperty("num_pkts_ttl_96_128")
    public String num_pkts_ttl_96_128 = null;
    @JsonProperty("num_pkts_ttl_128_160")
    public String num_pkts_ttl_128_160 = null;
    @JsonProperty("num_pkts_ttl_160_192")
    public String num_pkts_ttl_160_192 = null;
    @JsonProperty("num_pkts_ttl_192_224")
    public String num_pkts_ttl_192_224 = null;
    @JsonProperty("num_pkts_ttl_224_255")
    public String num_pkts_ttl_224_255 = null;
    @JsonProperty("http_url")
    public String http_url = null;
    @JsonProperty("http_host")
    public String http_host = null;
    @JsonProperty("http_host_l1")
    public String http_host_l1 = null;
    @JsonProperty("http_host_l2")
    public String http_host_l2 = null;
    @JsonProperty("http_user_agent")
    public String http_user_agent = null;
    @JsonProperty("http_user_agent_os")
    public String http_user_agent_os = null;
    @JsonProperty("http_referer")
    public String http_referer = null;
    @JsonProperty("http_referer_l1")
    public String http_referer_l1 = null;
    @JsonProperty("http_referer_l2")
    public String http_referer_l2 = null;
    @JsonProperty("http_social_media")
    public String http_social_media = null;
    @JsonProperty("http_social_user")
    public String http_social_user = null;
    @JsonProperty("wlan_ssid")
    public String wlan_ssid = null;
    @JsonProperty("sta_mac_address_floor")
    public String sta_mac_address_floor = null;
    @JsonProperty("sta_mac_address_building")
    public String sta_mac_address_building = null;
    @JsonProperty("sta_mac_address_campus")
    public String sta_mac_address_campus = null;
    @JsonProperty("ap_mac")
    public String ap_mac = null;
    @JsonProperty("src")
    public String src = null;

}
