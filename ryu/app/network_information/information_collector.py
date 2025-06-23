import time
from collections import defaultdict

import config
from ryu.lib import hub

from ryu.controller import ofp_event

from ryu.base.app_manager import lookup_service_brick
from ryu.controller.handler import set_ev_cls, MAIN_DISPATCHER
from ryu.ofproto import ofproto_v1_3

from ryu.base import app_manager
from ryu.topology.switches import LLDPPacket


class InformationCollector(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(InformationCollector, self).__init__(*args, **kwargs)
        self.name = 'information_collector'
        self.lldp_delay = {}                    # (src, dst) --> lldp_delay
        self.echo_delay = {}                    # dpid --> echo_delay
        self.port_stats = defaultdict(list)     # (dpid, port_no) --> [port_stats..]
        self.delay_info = {}                    # (src, dst) --> delay_time
        self.available_bandwidth_info = {}      # (src, dst) --> available_bandwidth
        self.packet_loss_info = {}              # (src, dst) --> packet_loss
        self.delay_info_all_get = False
        self.available_bandwidth_info_all_get = False
        self.packet_loss_info_all_get = False
        self.topology_discover_service = lookup_service_brick('topology_discover')
        self.graph_stability_monitor_thread = hub.spawn(self.monitor_graph_stability)
        self.delay_collector_thread = hub.spawn(self.delay_collector)
        self.bandwidth_collector_thread = hub.spawn(self.bandwidth_and_loss_collector)

    def monitor_graph_stability(self):
        while True:
            if self.topology_discover_service.graph_stable:
                if not hasattr(self, 'delay_collector_thread') or self.delay_collector_thread.dead:
                    print('Graph stable, starting delay collector...')
                    self.delay_collector_thread = hub.spawn(self.delay_collector)
                if not hasattr(self, 'bandwidth_collector_thread') or self.bandwidth_collector_thread.dead:
                    print('Graph stable, starting bandwidth and loss collector...')
                    self.bandwidth_collector_thread = hub.spawn(self.bandwidth_and_loss_collector)
            hub.sleep(1)  # check every 1 second

    def delay_collector(self):
        while True:
            if not self.topology_discover_service.graph_stable:
                print('topology graph became unstable, delay collector stop')
                break
            self.send_echo_request()
            self.calculate_all_delay()
            if config.DELAY_SHOW:
                self.display_delay()
            hub.sleep(config.DELAY_COLLECTOR_PERIOD)

    def bandwidth_and_loss_collector(self):
        while True:
            if not self.topology_discover_service.graph_stable:
                print('topology graph became unstable, bandwidth and loss collector stop')
                break
            self.request_port_stats()
            self.calculate_available_bandwidth()
            self.calculate_packet_loss()
            if config.BANDWIDTH_SHOW:
                self.display_bandwidth()
            if config.PACKET_LOSS_SHOW:
                self.display_packet_loss()
            hub.sleep(config.BANDWIDTH_AND_LOSS_COLLECTOR_PERIOD)

    def calculate_all_delay(self):
        for src, dst in self.topology_discover_service.graph.edges():
            delay = self.calculate_delay(src, dst)
            self.delay_info[(src, dst)] = delay

    def calculate_delay(self, src, dst):
        if (src, dst) not in self.lldp_delay or (dst, src) not in self.lldp_delay:
            return 0
        src_lldp_delay = self.lldp_delay[(src, dst)]
        dst_lldp_delay = self.lldp_delay[(dst, src)]
        src_echo_delay = self.echo_delay[src]
        dst_echo_delay = self.echo_delay[dst]
        delay = (src_lldp_delay + dst_lldp_delay - src_echo_delay - dst_echo_delay) / 2
        if delay <= 0:
            print(f'src: {src}--> dst: {dst}')
            print(f'src_lldp_delay: {src_lldp_delay}, dst_lldp_delay: {dst_lldp_delay}')
            print(f'src_echo_delay: {src_echo_delay}, dst_echo_delay: {dst_echo_delay}')
            delay = 0
        return delay

    @set_ev_cls(ofp_event.EventOFPEchoReply, MAIN_DISPATCHER)
    def echo_reply_handler(self, ev):
        recv_timestamp = time.time()
        latency = recv_timestamp - eval(ev.msg.data)
        self.echo_delay[ev.msg.datapath.id] = latency

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def port_stats_reply_handler(self, ev):
        dpid = ev.msg.datapath.id
        for stat in ev.msg.body:
            if stat.port_no == ofproto_v1_3.OFPP_LOCAL:
                continue
            key = (dpid, stat.port_no)
            value = [stat.rx_packets, stat.tx_packets, stat.rx_bytes, stat.tx_bytes, stat.rx_dropped, stat.tx_dropped,
                     stat.rx_errors, stat.tx_errors, stat.duration_sec, stat.duration_nsec]
            # print(f'value: {value}')
            if len(self.port_stats[key]) < config.PORT_STAT_LIST_LEN:
                self.port_stats[key].append(value)

    def calculate_available_bandwidth(self):
        for src, dst, data in self.topology_discover_service.graph.edges(data=True):
            key = (src, data['port_pair'][0])
            curr_speed = self.topology_discover_service.ports_curr_speed[key]
            port_load = self.calculate_port_load(key)
            available_bandwidth = curr_speed - port_load
            self.available_bandwidth_info[(src, dst)] = available_bandwidth

    def calculate_port_load(self, key):
        port_info = self.port_stats[key]
        if len(port_info) == 0:
            # this port not hava port_info, try to request port stats again.
            # return 0 temporarily
            datapath = lookup_service_brick('topology_discover').switches[key[0]][0]
            self.send_port_stats_request(datapath, key[1])
            return 0
        elif len(port_info) == 1:
            # kbit/s
            return (port_info[-1][2] + port_info[-1][3]) * 8 / (port_info[-1][8] + port_info[-1][9] / (10 ** 9)) / 1000
        else:
            pre_bytes = port_info[-2][2] + port_info[-2][3]
            now_bytes = port_info[-1][2] + port_info[-1][3]
            pre2now_time = ((port_info[-1][8] + port_info[-1][9] / (10 ** 9)) -
                            (port_info[-2][8] + port_info[-2][9] / (10 ** 9)))
            if pre2now_time == 0:
                # time calculate error, return 0
                # print('[calculate_port_load]: time calculate same')
                if pre_bytes == now_bytes:
                    # last two port info same, do as one port info
                    # print('[calculate_port_load]: bytes calculate same')
                    return now_bytes * 8 / (port_info[-1][8] + port_info[-1][9] / (10 ** 9)) / 1000
                # else it's error, return 0
                return 0
            # kbit/s
            return (now_bytes - pre_bytes) * 8 / pre2now_time / 1000

    def calculate_packet_loss(self):
        for src, dst, data in self.topology_discover_service.graph.edges(data=True):
            key = (src, data['port_pair'][0])
            if self.port_stats[key]:
                port_info = self.port_stats[key][-1]
                rx_total_packets = port_info[0] + port_info[4] + port_info[6]
                tx_total_packets = port_info[1] + port_info[5] + port_info[7]
                rx_dropped_packets = port_info[4]
                tx_dropped_packets = port_info[5]
                rx_errors_packets = port_info[6]
                tx_errors_packets = port_info[7]
                if rx_total_packets == 0 or tx_total_packets == 0:
                    # this port not have packet info, return 0
                    self.packet_loss_info[(src, dst)] = 0
                    continue
                packet_loss = (rx_dropped_packets + tx_dropped_packets) / (rx_total_packets + tx_total_packets)
                rx_packet_loss = rx_dropped_packets / rx_total_packets
                tx_packet_loss = tx_dropped_packets / tx_total_packets
                rx_packet_errors = rx_errors_packets / rx_total_packets
                tx_packet_errors = tx_errors_packets / tx_total_packets
                # print(f'src{src}, dst{dst}: rx_loss:{rx_packet_loss:.2f}, tx_loss:{tx_packet_loss:.2f},'
                #      f' rx_packet_errors:{rx_packet_errors}, tx_packet_errors:{tx_packet_errors}, ')
                self.packet_loss_info[(src, dst)] = packet_loss
            else:
                # this port not have packet info, return 0
                self.packet_loss_info[(src, dst)] = 0

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        try:
            src_dpid, src_port_no = LLDPPacket.lldp_parse(msg.data)
        except LLDPPacket.LLDPUnknownFormat:
            # This handler can receive all the packets which can be
            # not-LLDP packet. Ignore it silently
            return

        recv_timestamp = time.time()
        dst_pid = msg.datapath.id

        # print(f'get in function: packet_in_handler')

        # check_topology_service()

        # switches_service is ryu.topology.switches.Switches class
        switches_service = lookup_service_brick('switches')

        for port in switches_service.ports.keys():
            if src_dpid == port.dpid and src_port_no == port.port_no:
                send_timestamp = switches_service.ports[port].timestamp
                if send_timestamp is None:
                    print(f'dpid {src_dpid} port_no {src_port_no} has no timestamp')
                    continue
                lldp_delay = recv_timestamp - send_timestamp
                # print(f'switch{src_dpid}--> switch{dst_pid}: lldp_delay {lldp_delay}')
                self.lldp_delay[(src_dpid, dst_pid)] = lldp_delay

    def check_topology_service(self):
        switches_service = lookup_service_brick('switches')
        print(f'topology_discover_service: {self.topology_discover_service}')
        print(f'switches_service: {switches_service}')
        if self.topology_discover_service:
            print(f'topology_graph: {self.topology_discover_service.graph}')
            print(f'topology_switches: {self.topology_discover_service.switches}')
            print(f'topology_links: {self.topology_discover_service.links}')
            print(f'topology_host_location: {self.topology_discover_service.host_location}')

    def send_echo_request(self):
        for _, (datapath, _) in self.topology_discover_service.switches.items():
            parser = datapath.ofproto_parser
            echo_request = parser.OFPEchoRequest(datapath, data=f'{time.time():.12f}'.encode())
            datapath.send_msg(echo_request)
            hub.sleep(config.ECHO_REQUEST_PERIOD)

    def request_port_stats(self):
        for _, (datapath, _) in self.topology_discover_service.switches.items():
            self.send_port_stats_request(datapath)

    @staticmethod
    def send_port_stats_request(datapath, port_no=ofproto_v1_3.OFPP_ANY):
        ofp = datapath.ofproto
        ofp_parser = datapath.ofproto_parser

        req = ofp_parser.OFPPortStatsRequest(datapath, 0, port_no)
        datapath.send_msg(req)

    def display_delay(self):
        for src, dst in self.topology_discover_service.graph.edges():
            if (src, dst) in self.delay_info:
                print(f'src: {src}, dst: {dst}, delay: {self.delay_info[(src,dst)]} seconds')
            else:
                print(f'src: {src}, dst: {dst} not hava delay info')

    def display_bandwidth(self):
        for src, dst in self.topology_discover_service.graph.edges():
            if (src, dst) in self.available_bandwidth_info:
                print(f'src: {src}, dst: {dst}, available_bandwidth: {self.available_bandwidth_info[(src, dst)]} kbit/s')
            else:
                print(f'src: {src}, dst: {dst}, not have available_bandwidth info')

    def display_packet_loss(self):
        for src, dst in self.topology_discover_service.graph.edges():
            if (src, dst) in self.packet_loss_info:
                print(f'src: {src}, dst: {dst}, packet_loss: {self.packet_loss_info[(src,dst)]} %')
            else:
                print(f'src: {src}, dst: {dst}, not have packet_loss')