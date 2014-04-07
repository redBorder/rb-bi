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
public class SchemeEvent_rb_event {
    @JsonProperty("timestamp")
    public String timestamp = null;
    @JsonProperty("sensor_id_snort")
    public Integer sensor_id_snort = null;
    @JsonProperty("sensor_id")
    public Integer sensor_id = null;
    @JsonProperty("sensor_ip")
    public String sensor_ip = null;
    @JsonProperty("sensor_name")
    public String sensor_name = null;
    @JsonProperty("domain_name")
    public String domain_name = null;
    @JsonProperty("group_name")
    public String group_name = null;
    @JsonProperty("group_id")
    public Integer group_id = null;
    @JsonProperty("type")
    public String type = null;
    @JsonProperty("action")
    public String action = null;
    @JsonProperty("sig_generator")
    public Integer sig_generator = null;
    @JsonProperty("sig_id")
    public Integer sig_id = null;
    @JsonProperty("sig_rev")
    public String sig_rev = null;
    @JsonProperty("priority")
    public Integer priority = null;
    @JsonProperty("priority_name")
    public String priority_name = null;
    @JsonProperty("classification")
    public String classification = null;
    @JsonProperty("msg")
    public String msg = null;
    @JsonProperty("payload")
    public String payload = null;
    @JsonProperty("l4_proto_name")
    public String l4_proto_name = null;
    @JsonProperty("l4_proto")
    public String l4_proto = null;
    @JsonProperty("ethsrc")
    public String ethsrc = null;
    @JsonProperty("ethdst")
    public String ethdst = null;
    @JsonProperty("ethtype")
    public String ethtype = null;
    @JsonProperty("arp_hw_saddr")
    public String arp_hw_saddr = null;
    @JsonProperty("arp_hw_sprot")
    public String arp_hw_sprot = null;
    @JsonProperty("arp_hw_taddr")
    public String arp_hw_taddr = null;
    @JsonProperty("arp_hw_tprot")
    public String arp_hw_tprot = null;
    @JsonProperty("vlan")
    public String vlan = null;
    @JsonProperty("vlan_name")
    public String vlan_name = null;
    @JsonProperty("vlan_priority")
    public Integer vlan_priority = null;
    @JsonProperty("vlan_drop")
    public Integer vlan_drop = null;
    @JsonProperty("udplength")
    public Integer udplength = null;
    @JsonProperty("ethlen")
    public Integer ethlen = null;
    @JsonProperty("ethlength_range")
    public String ethlength_range = null;
    @JsonProperty("trheader")
    public String trheader = null;
    @JsonProperty("l4_srcport")
    public String l4_srcport = null;
    @JsonProperty("l4_srcport_name")
    public String l4_srcport_name = null;
    @JsonProperty("l4_dstport")
    public String l4_dstport = null;
    @JsonProperty("l4_dstport_name")
    public String l4_dstport_name = null;
    @JsonProperty("src_asnum")
    public String src_asnum = null;
    @JsonProperty("src")
    public String src = null;
    @JsonProperty("src_name")
    public String src_name = null;
    @JsonProperty("src_net")
    public String src_net = null;
    @JsonProperty("src_net_name")
    public String src_net_name = null;
    @JsonProperty("dst_asnum")
    public String dst_asnum = null;
    @JsonProperty("dst_name")
    public String dst_name = null;
    @JsonProperty("dst")
    public String dst = null;
    @JsonProperty("dst_net")
    public String dst_net = null;
    @JsonProperty("dst_net_name")
    public String dst_net_name = null;
    @JsonProperty("icmptype")
    public Integer icmptype = null;
    @JsonProperty("icmpcode")
    public Integer icmpcode = null;
    @JsonProperty("icmpid")
    public Integer icmpid = null;
    @JsonProperty("icmpseq")
    public Integer icmpseq = null;
    @JsonProperty("ttl")
    public Integer ttl = null;
    @JsonProperty("tos")
    public Integer tos = null;
    @JsonProperty("id")
    public Integer id = null;
    @JsonProperty("iplen")
    public Integer iplen = null;
    @JsonProperty("iplen_range")
    public String iplen_range = null;
    @JsonProperty("dgmlen")
    public Integer dgmlen = null;
    @JsonProperty("tcpseq")
    public String tcpseq = null;
    @JsonProperty("tcpack")
    public String tcpack = null;
    @JsonProperty("tcplen")
    public Integer tcplen = null;
    @JsonProperty("tcpwindow")
    public Integer tcpwindow = null;
    @JsonProperty("tcpflags")
    public String tcpflags = null;
    @JsonProperty("src_country")
    public String src_country = null;
    @JsonProperty("dst_country")
    public String dst_country = null;
    @JsonProperty("src_country_code")
    public String src_country_code = null;
    @JsonProperty("dst_country_code")
    public String dst_country_code = null;
    @JsonProperty("src_as")
    public String src_as = null;
    @JsonProperty("dst_as")
    public String dst_as = null;
    @JsonProperty("src_as_name")
    public String src_as_name = null;
    @JsonProperty("dst_as_name")
    public String dst_as_name = null;


}
