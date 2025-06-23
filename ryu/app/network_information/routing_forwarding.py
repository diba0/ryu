import networkx as nx

from ryu.base.app_manager import lookup_service_brick
from ryu.lib.packet import packet, arp, ipv4

from ryu.controller import ofp_event

from ryu.controller.handler import set_ev_cls, MAIN_DISPATCHER
from ryu.ofproto import ofproto_v1_3

from ryu.base import app_manager


class RoutingForwarding(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(RoutingForwarding, self).__init__(*args, **kwargs)
        self.name = 'routing_forwarding'
        self.shortest_hop_paths = {}
        self.shortest_delay_paths = {}
        self.max_available_bandwidth_paths = {}
        self.min_packet_loss_paths = {}
        self.topology_discover_service = lookup_service_brick('topology_discover')
        self.information_collector_service = lookup_service_brick('information_collector')

    @staticmethod
    def add_flow(datapath, priority, match, actions, idle_timeout=None, hard_timeout=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # construct flow_mod message and send it.
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                idle_timeout=idle_timeout, hard_timeout=hard_timeout,
                                match=match, instructions=inst)
        datapath.send_msg(mod)

    def send_flow_mod(self, datapath, forwarding_info, src_ip, dst_ip):
        print(f'config flow table in {datapath.id}, match: {src_ip} -> {dst_ip}, out_port: {forwarding_info[1]}')
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch(in_port=forwarding_info[0], eth_type=0x0800, ipv4_src=src_ip, ipv4_dst=dst_ip)
        actions = [parser.OFPActionOutput(forwarding_info[1])]
        priority = 1
        # delete previous flow
        delete_flow_mod = parser.OFPFlowMod(
            datapath=datapath,
            command=ofproto.OFPFC_DELETE,  # 删除命令
            out_port=ofproto.OFPP_ANY,  # 删除所有端口相关规则
            out_group=ofproto.OFPG_ANY,  # 删除所有组相关规则
            priority=priority,  # 删除规则的优先级
            match=match  # 删除规则的匹配条件
        )
        datapath.send_msg(delete_flow_mod)
        # add new flow
        self.add_flow(datapath, priority, match, actions, idle_timeout=0, hard_timeout=0)

    @staticmethod
    def send_packet_out(datapath, buffer_id, in_port, out_port=None, data=None):
        ofp = datapath.ofproto
        ofp_parser = datapath.ofproto_parser
        if out_port:
            actions = [ofp_parser.OFPActionOutput(out_port)]
        else:
            actions = [ofp_parser.OFPActionOutput(ofp.OFPP_FLOOD, 0)]
        if buffer_id == ofp.OFP_NO_BUFFER:
            req = ofp_parser.OFPPacketOut(datapath=datapath, buffer_id=ofp.OFP_NO_BUFFER,
                                          in_port=in_port, actions=actions, data=data)
        else:
            req = ofp_parser.OFPPacketOut(datapath=datapath, buffer_id=buffer_id,
                                          in_port=in_port, actions=actions, data=None)
        datapath.send_msg(req)

    def arp_forwarding(self, msg, dst_ip):
        # if dst is known, forwarding to dst's datapath
        # else flooding in src's datapath
        if dst_ip in self.topology_discover_service.host_location:
            dp, port = self.topology_discover_service.host_location[dst_ip]
            self.send_packet_out(datapath=dp, buffer_id=msg.buffer_id, in_port=dp.ofproto.OFPP_CONTROLLER,
                                 out_port=port, data=msg.data)

    @staticmethod
    def get_max_available_bandwidth_path(graph, paths):
        max_available_bandwidth = 0
        max_available_bandwidth_path = []
        for path in paths:
            # print(f'path: {path}')
            # 如果路径上只有一个交换机，直接返回该路径
            if len(path) == 1:
                max_available_bandwidth_path = path
                break
            # for i in range(len(path) - 2):
            #     print(path[i], path[i+1])
            #     print('abd:', {graph[path[i]][path[i+1]]['available_bandwidth']})
            available_bandwidth = min(
                [graph[path[i]][path[i + 1]]['available_bandwidth'] for i in range(len(path) - 2)])
            if available_bandwidth > max_available_bandwidth:
                max_available_bandwidth = available_bandwidth
                max_available_bandwidth_path = path
        return max_available_bandwidth_path

    def install_path(self, path, msg, src_ip, dst_ip):

        if len(path) == 0:
            return
        elif len(path) == 1:
            dp, in_port = self.topology_discover_service.host_location[src_ip]
            dp, out_port = self.topology_discover_service.host_location[dst_ip]
            self.send_flow_mod(dp, (in_port, out_port), src_ip, dst_ip)
        elif len(path) == 2:
            first_dp, in_port = self.topology_discover_service.host_location[src_ip]
            out_port, _ = self.topology_discover_service.links[(path[0], path[1])]
            self.send_flow_mod(first_dp, (in_port, out_port), src_ip, dst_ip)
            last_dp, last_port = self.topology_discover_service.host_location[dst_ip]
            _, in_port = self.topology_discover_service.links[(path[0], path[1])]
            self.send_flow_mod(last_dp, (in_port, last_port), src_ip, dst_ip)
        else:
            switches = self.topology_discover_service.switches
            links = self.topology_discover_service.links
            first_dp, in_port = self.topology_discover_service.host_location[src_ip]
            out_port, _ = links[(path[0], path[1])]
            self.send_flow_mod(first_dp, (in_port, out_port), src_ip, dst_ip)
            for i in range(1, len(path) - 1):
                _, in_port = links[path[i - 1], path[i]]
                out_port, _ = links[path[i], path[i + 1]]
                dp = switches[path[i]][0]
                self.send_flow_mod(dp, (in_port, out_port), src_ip, dst_ip)
            last_dp, last_port = self.topology_discover_service.host_location[dst_ip]
            _, in_port = links[(path[-2], path[-1])]
            self.send_flow_mod(last_dp, (in_port, last_port), src_ip, dst_ip)

    def ipv4_forwarding(self, msg, src_ip, dst_ip):
        graph = self.topology_discover_service.graph.copy()

        # add delay, available_bandwidth, packet_loss to graph
        for (src, dst), delay in self.information_collector_service.delay_info.items():
            graph[src][dst]['delay'] = delay
        for (src, dst), available_bandwidth in self.information_collector_service.available_bandwidth_info.items():
            graph[src][dst]['available_bandwidth'] = available_bandwidth
        for (src, dst), packet_loss in self.information_collector_service.packet_loss_info.items():
            graph[src][dst]['packet_loss'] = packet_loss

        src_dp, _ = self.topology_discover_service.host_location[src_ip]
        dst_dp, _ = self.topology_discover_service.host_location[dst_ip]

        shortest_hop_path = nx.dijkstra_path(graph, src_dp.id, dst_dp.id)
        shortest_delay_path = nx.dijkstra_path(graph, src_dp.id, dst_dp.id, weight='delay')
        shortest_simple_paths = nx.shortest_simple_paths(graph, src_dp.id, dst_dp.id)
        max_available_bandwidth_path = self.get_max_available_bandwidth_path(graph,
                                                                             shortest_simple_paths)
        min_packet_loss_path = nx.dijkstra_path(graph, src_dp.id, dst_dp.id,
                                                weight='packet_loss')

        self.install_path(shortest_hop_path, msg, src_ip, dst_ip)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        pkt = packet.Packet(msg.data)
        arp_packet = pkt.get_protocol(arp.arp)
        ipv4_packet = pkt.get_protocol(ipv4.ipv4)

        # if self.topology_discover_service.graph_stable:

        if arp_packet:
            self.arp_forwarding(msg, arp_packet.dst_ip)
        if ipv4_packet:
            self.ipv4_forwarding(msg, ipv4_packet.src, ipv4_packet.dst)
