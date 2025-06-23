import time

import networkx as nx

from ryu.lib.packet import packet, arp

from ryu.controller import ofp_event

from ryu.controller.handler import set_ev_cls, MAIN_DISPATCHER, CONFIG_DISPATCHER
from ryu.ofproto import ofproto_v1_3

from ryu.base import app_manager
from ryu.topology import event
from ryu.topology.api import get_switch, get_link

from config import TOPO_SHOW, TOPO_GRAPH_STABLE_TIME

class TopologyDiscover(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    events = [event.EventSwitchEnter,
              event.EventSwitchLeave, event.EventPortAdd,
              event.EventPortDelete, event.EventPortModify,
              event.EventLinkAdd, event.EventLinkDelete]

    def __init__(self, *args, **kwargs):
        super(TopologyDiscover, self).__init__(*args, **kwargs)
        self.name = 'topology_discover'
        self.topology_api_app = self
        self.switches = {}          # switch_dp_id --> (switch_dp, switch_ports)
        self.links = {}             # (src_dpid, dst_dpid) --> (src_port_no, dst_port_no)
        self.ports_curr_speed = {}   # (dpid, port_no) -> curr_speed
        self.graph = nx.DiGraph()   # Directed graphs
        self.host_location = {}     # host_ip --> (switch_dp, switch_port_no)
        self.graph_not_changed = False
        self.graph_not_changed_start_time = 0
        self.graph_stable = False

    @set_ev_cls(events)
    def topology_discover(self, ev):
        # get_switch function returns a list of ryu.topology.Switch class
        # this class has two properties: dp and ports
        # dp is ryu.controller.controller.Datapath class
        # ports is a list of ryu.topology.switches.Port class
        switch_list = get_switch(self.topology_api_app, None)
        # get_link function returns a list a ryu.topology.Link class
        # this class has two properties: src and dst
        # src and dst are both ryu.topology.Port class which has properties
        # dpid and port_no
        link_list = get_link(self.topology_api_app, None)
        # print(f'switch_list: {switch_list}')
        # for switch in switch_list:
        #     print(f'switch_dp: {switch.dp}, switch_dpid: {switch.dp.id}')
        # for switch in switch_list:
        #     print(f'switch.ports: {switch.ports}, switch.port.dpid: {switch.ports[0].dpid}, switch.port.port_no: {switch.ports[0].port_no}')
        for switch in switch_list:
            self.switches.setdefault(switch.dp.id, (switch.dp, switch.ports))
        # print(f'self.switches = {self.switches}')
        # print(f'link_list: {link_list}')
        # for link in link_list:
        #     print(f'link_src_dpid: {link.src.dpid}, link_dst_dpid: {link.dst.dpid}')
        #     print(f'link_src_port: {link.src.port_no}, link_dst_port: {link.dst.port_no}')
        for link in link_list:
            self.links[(link.src.dpid, link.dst.dpid)] = (link.src.port_no, link.dst.port_no)

        # save previous graph
        pre_graph = self.graph.copy()
        # reset self.graph to null graph
        # it's not ture, other apps need to modify the graph, and we need to keep the graph id
        # self.graph = nx.DiGraph()
        # so, we need to clear the graph
        self.graph.clear()
        for link, port in self.links.items():
            self.graph.add_edge(link[0], link[1], port_pair=(port[0], port[1]))
            self.graph.add_edge(link[1], link[0], port_pair=(port[1], port[0]))

        # print(f'id of td_graph: {id(self.graph)}')

        if not self.are_graphs_equal(pre_graph, self.graph):
            self.graph_not_changed = False
            self.graph_stable = False
            print(f'upgrade topology graph')
            # for u,v,data in self.graph.edges(data=True):
            #     print(f"edge: {u}, {v}, {data}")
            print(f'pre_graph_nodes: {pre_graph.nodes}')
            print(f'graph_nodes: {self.graph.nodes}')
            print(f'pre_graph_edges: {pre_graph.edges}')
            print(f'graph_edges: {self.graph.edges}')
        else:
            if not self.graph_not_changed:
                self.graph_not_changed_start_time = time.time()
            else:
                if time.time() - self.graph_not_changed_start_time > TOPO_GRAPH_STABLE_TIME:
                    self.graph_stable = True
                    print('topology graph is stable')
            self.graph_not_changed = True
            print(f'topology graph is not changed')
            return

        for _, (datapath, _) in self.switches.items():
            self.send_port_desc_stats_request(datapath)
        if TOPO_SHOW:
            self.show_topology()

        # import random
        # src = random.choice(list(self.graph.nodes))
        # dst = random.choice(list(self.graph.nodes))
        # print(f'shortest path from {src} to {dst}: {nx.shortest_path(self.graph,src,dst)}')

    @staticmethod
    def send_port_desc_stats_request(datapath):
        ofp_parser = datapath.ofproto_parser

        req = ofp_parser.OFPPortDescStatsRequest(datapath, 0)
        datapath.send_msg(req)

    @staticmethod
    def are_graphs_equal(G1, G2):
        # check nodes are same or not
        if G1.nodes != G2.nodes:
            return False

        # check edges are same or not
        if G1.edges != G2.edges:
            return False

        # check graph attributes are same or not
        if G1.graph != G2.graph:
            return False

        return True

    @set_ev_cls(ofp_event.EventOFPPortDescStatsReply, MAIN_DISPATCHER)
    def port_desc_stats_reply_handler(self, ev):
        msg = ev.msg
        dpid = msg.datapath.id
        # record port current bandwidth
        for p in msg.body:
            if p.port_no == ofproto_v1_3.OFPP_LOCAL:
                continue
            # print(f'port_stats: {p}')
            self.ports_curr_speed[(dpid, p.port_no)] = p.curr_speed
        # print(f'self.ports_curr_speed: {self.ports_curr_speed}')

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def port_status_handler(self, ev):
        msg = ev.msg
        dp = msg.datapath
        ofp = dp.ofproto

        # when link is down or blocked, set port_curr_speed = 0
        if msg.desc.state == ofp.OFPPS_LINK_DOWN or msg.desc.state == ofp.OFPPS_BLOCKED:
            self.ports_curr_speed[(dp.id, msg.desc.port_no)] = 0
            print(f'dpid {dp.id} port {msg.desc.port_no}: down')
        # when link is down, set port_curr_speed = 0
        elif msg.desc.state == ofp.OFPPS_LIVE:
            self.ports_curr_speed[(dp.id, msg.desc.port_no)] = msg.desc.curr_speed
            print(f'dpid {dp.id} port {msg.desc.port_no}: up')


    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # install the table-miss flow entry.
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_flow(self, datapath, priority, match, actions):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # construct flow_mod message and send it.
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath

        in_port = msg.match['in_port']
        pkt = packet.Packet(msg.data)
        arp_pkt = pkt.get_protocol(arp.arp)

        if arp_pkt:
            arp_src_ip = arp_pkt.src_ip

            self.host_location[arp_src_ip] = (datapath, in_port)

            # for ip, datapath in self.host_location.items():
            #     print(f'host {ip} is linked to {datapath}')

    def show_topology(self):
        import matplotlib.pyplot as plt
        nx.draw(self.graph, with_labels=True)
        plt.show()
