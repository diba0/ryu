# import requests
import ast
import csv
import json
import time
from operator import attrgetter

import setting
import simple_awareness
import simple_delay
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.lib import hub
from ryu.lib.packet import arp, tcp
from ryu.lib.packet import packet
from ryu.lib.packet import ipv4
from ryu.ofproto import ofproto_v1_3
from ryu.ofproto.ether import ETH_TYPE_IP


class simple_Monitor(app_manager.RyuApp):
    """
        A Ryu app for netowrk monitoring. It retreieves statistics information through openflow
        of datapaths at the Data Plane.
        This class contains functions belonging to the Statistics module and Flow Installation module
        of the Control Plane. 
        I also contains the functions corresponding to the Process Statistics module of the 
        Management Plane in order to adventage the monitorin threading for statistics processing.
    """

    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {"simple_awareness": simple_awareness.simple_Awareness,
                 "simple_delay": simple_delay.simple_Delay}

    def __init__(self, *args, **kwargs):
        super(simple_Monitor, self).__init__(*args, **kwargs)
        self.name = "monitor"
        self.count_monitor = 0
        self.topology_api_app = self
        self.datapaths = {}
        self.port_stats = {}
        self.port_speed = {}
        self.flow_stats = {}
        self.flow_speed = {}
        self.flow_loss = {}
        self.port_loss = {}
        self.link_loss = {}
        self.net_info = {}
        self.net_metrics = {}
        self.link_free_bw = {}
        self.link_used_bw = {}
        self.stats = {}
        self.port_features = {}
        self.free_bandwidth = {}
        self.awareness = kwargs["simple_awareness"]
        self.delay = kwargs["simple_delay"]
        self.paths = {}
        self.installed_paths = {}

        self.monitor_thread = hub.spawn(self.monitor)

    @set_ev_cls(ofp_event.EventOFPStateChange,
                [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def state_change_handler(self, ev):
        """
            Record datapath information.
        """
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.logger.debug('Datapath registered: %016x', datapath.id)
                print('Datapath registered:', datapath.id)  ##
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                self.logger.debug('Datapath unregistered: %016x', datapath.id)
                print('Datapath unregistered:', datapath.id)
                del self.datapaths[datapath.id]

    def monitor(self):
        """
            Main entry method of monitoring traffic.
        """
        while True:
            self.count_monitor += 1
            self.stats['flow'] = {}
            self.stats['port'] = {}
            print("[Statistics Module Ok]")
            print("[{0}]".format(self.count_monitor))
            for dp in self.datapaths.values():
                self.port_features.setdefault(dp.id, {})
                self.paths = None
                self.request_stats(dp)

            if self.awareness.link_to_port:
                self.flow_install_monitor()
            if self.stats['port']:
                self.get_port_loss()
                self.get_link_free_bw()
                self.get_link_used_bw()
                self.write_values()

            hub.sleep(setting.MONITOR_PERIOD)
            if self.stats['port']:
                self.show_stat('link')
                hub.sleep(1)

    # ---------------------CONTROL PLANE FUNCTIONS----------------------------------------
    # ---------------------FLOW INSTALLATION MODULE FUNCTIONS ----------------------------

    def flow_install_monitor(self):
        print("[Flow Installation Ok]")
        out_time = time.time()
        for dp in self.datapaths.values():
            for dp2 in self.datapaths.values():
                if dp.id != dp2.id:
                    ip_src = '10.0.0.' + str(dp.id)
                    ip_dst = '10.0.0.' + str(dp2.id)
                    self.forwarding(dp.id, ip_src, ip_dst, dp.id, dp2.id)
                    time.sleep(0.0005)
        end_out_time = time.time()
        out_total_ = end_out_time - out_time
        print("FLow installation ends in: {0}s".format(out_total_))
        return

    def forwarding(self, dpid, ip_src, ip_dst, src_sw, dst_sw):
        """
            Get paths and install them into datapaths.
        """

        self.installed_paths.setdefault(dpid, {})
        path = self.get_path(str(src_sw), str(dst_sw))
        self.installed_paths[src_sw][dst_sw] = path
        flow_info = (ip_src, ip_dst)
        self.install_flow(self.datapaths, self.awareness.link_to_port, path, flow_info)

    def request_stats(self, datapath):  # OK
        self.logger.debug('send stats request: %016x', datapath.id)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        req = parser.OFPPortDescStatsRequest(datapath, 0)  # for port description
        datapath.send_msg(req)

        req = parser.OFPFlowStatsRequest(datapath)  # individual flow statistics
        datapath.send_msg(req)

        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
        datapath.send_msg(req)

    def install_flow(self, datapaths, link_to_port, path,
                     flow_info, data=None):
        init_time_install = time.time()
        ''' 
            Install flow entires.
            path=[dpid1, dpid2...]
            flow_info=(src_ip, dst_ip)
        '''
        if path is None or len(path) == 0:
            self.logger.info("Path error!")
            return

        in_port = 1
        first_dp = datapaths[path[0]]

        out_port = first_dp.ofproto.OFPP_LOCAL
        back_info = (flow_info[1], flow_info[0])

        # Flow installing por middle datapaths in path
        if len(path) > 2:
            for i in range(1, len(path) - 1):
                port = self.get_port_pair_from_link(link_to_port,
                                                    path[i - 1], path[i])
                port_next = self.get_port_pair_from_link(link_to_port,
                                                         path[i], path[i + 1])
                if port and port_next:
                    src_port, dst_port = port[1], port_next[0]
                    datapath = datapaths[path[i]]
                    self.send_flow_mod(datapath, flow_info, src_port, dst_port)
                    self.send_flow_mod(datapath, back_info, dst_port, src_port)
        if len(path) > 1:
            # The last flow entry
            port_pair = self.get_port_pair_from_link(link_to_port,
                                                     path[-2], path[-1])
            if port_pair is None:
                self.logger.info("Port is not found")
                return
            src_port = port_pair[1]
            dst_port = 1  # I know that is the host port
            last_dp = datapaths[path[-1]]
            self.send_flow_mod(last_dp, flow_info, src_port, dst_port)
            self.send_flow_mod(last_dp, back_info, dst_port, src_port)

            # The first flow entry
            port_pair = self.get_port_pair_from_link(link_to_port, path[0], path[1])
            if port_pair is None:
                self.logger.info("Port not found in first hop.")
                return
            out_port = port_pair[0]
            self.send_flow_mod(first_dp, flow_info, in_port, out_port)
            self.send_flow_mod(first_dp, back_info, out_port, in_port)

        # src and dst on the same datapath
        else:
            out_port = 1
            self.send_flow_mod(first_dp, flow_info, in_port, out_port)
            self.send_flow_mod(first_dp, back_info, out_port, in_port)

        end_time_install = time.time()
        total_install = end_time_install - init_time_install

    def send_flow_mod(self, datapath, flow_info, src_port, dst_port):
        """
            Build flow entry, and send it to datapath.
        """
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        actions = []
        actions.append(parser.OFPActionOutput(dst_port))

        match = parser.OFPMatch(
            eth_type=ETH_TYPE_IP, ipv4_src=flow_info[0],
            ipv4_dst=flow_info[1])

        self.add_flow(datapath, 1, match, actions,
                      idle_timeout=250, hard_timeout=0)

    def add_flow(self, dp, priority, match, actions, idle_timeout=0, hard_timeout=0):
        """
            Send a flow entry to datapath.
        """
        ofproto = dp.ofproto
        parser = dp.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=dp, command=dp.ofproto.OFPFC_ADD, priority=priority,
                                idle_timeout=idle_timeout,
                                hard_timeout=hard_timeout,
                                match=match, instructions=inst)
        dp.send_msg(mod)

    def del_flow(self, datapath, dst):
        """
            Deletes a flow entry of the datapath.
        """
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        match = parser.OFPMatch(eth_type=ETH_TYPE_IP, ipv4_src=flow_info[0], ipv4_dst=flow_info[1])
        mod = parser.OFPFlowMod(datapath=datapath, match=match, cookie=0, command=ofproto.OFPFC_DELETE)
        datapath.send_msg(mod)

    def build_packet_out(self, datapath, buffer_id, src_port, dst_port, data):
        """
            Build packet out object.
        """
        actions = []
        if dst_port:
            actions.append(datapath.ofproto_parser.OFPActionOutput(dst_port))

        msg_data = None
        if buffer_id == datapath.ofproto.OFP_NO_BUFFER:
            if data is None:
                return None
            msg_data = data

        out = datapath.ofproto_parser.OFPPacketOut(
            datapath=datapath, buffer_id=buffer_id,
            data=msg_data, in_port=src_port, actions=actions)
        return out

    def arp_forwarding(self, msg, src_ip, dst_ip):
        """
            Send ARP packet to the destination host if the dst host record
            is existed.
            result = (datapath, port) of host
        """
        datapath = msg.datapath
        ofproto = datapath.ofproto

        result = self.awareness.get_host_location(dst_ip)
        if result:
            # Host has been recorded in access table.
            datapath_dst, out_port = result[0], result[1]
            datapath = self.datapaths[datapath_dst]
            out = self.build_packet_out(datapath, ofproto.OFP_NO_BUFFER,
                                        ofproto.OFPP_CONTROLLER,
                                        out_port, msg.data)
            datapath.send_msg(out)
            self.logger.debug("Deliver ARP packet to knew host")
        else:
            # self.flood(msg)
            pass

    def get_port_pair_from_link(self, link_to_port, src_dpid, dst_dpid):
        """
            Get port pair of link, so that controller can install flow entry.
            link_to_port = {(src_dpid,dst_dpid):(src_port,dst_port),}
        """
        if (src_dpid, dst_dpid) in link_to_port:
            return link_to_port[(src_dpid, dst_dpid)]
        else:
            self.logger.info("Link from dpid:%s to dpid:%s is not in links" %
                             (src_dpid, dst_dpid))
            return None

    def get_path(self, src, dst):

        if self.paths != None:
            # print ('PATHS: OK')
            path = self.paths.get(src).get(dst)[0]
            return path
        else:
            # print('Getting paths: OK')
            paths = self.get_RL_paths()
            path = paths.get(src).get(dst)[0]
            return path

    def get_RL_paths(self):

        file = '/home/ubuntu/ryu/ryu/app/RSIR-Reinforcement-Learning-and-SDN-Intelligent-Routing/SDNapps_proac/paths.json'
        try:
            with open(file, 'r') as json_file:
                paths_dict = json.load(json_file)
                paths_dict = ast.literal_eval(json.dumps(paths_dict))
                self.paths = paths_dict
                return self.paths
        except ValueError as e:  # error excpetion when trying to read the json and is still been updated
            return
        else:
            with open(file, 'r') as json_file:  # try again
                paths_dict = json.load(json_file)
                paths_dict = ast.literal_eval(json.dumps(paths_dict))
                self.paths = paths_dict
                return self.paths

    # ---------------------CONTROL PLANE -----------------------------------------
    # -----------------------STATISTICS MODULE FUNCTIONS -------------------------

    def save_stats(self, _dict, key, value, length=5):  # Save values in dics (max len 5)
        if key not in _dict:
            _dict[key] = []
        _dict[key].append(value)
        if len(_dict[key]) > length:
            _dict[key].pop(0)

    def get_speed(self, now, pre, period):  # bits/s
        if period:
            return ((now - pre) * 8) / period
        else:
            return 0

    def get_time(self, sec, nsec):  # Total time that the flow was alive in seconds
        return sec + nsec / 1000000000.0

    def get_period(self, n_sec, n_nsec, p_sec, p_nsec):  # (time las flow, time)
        # calculates period of time between flows
        return self.get_time(n_sec, n_nsec) - self.get_time(p_sec, p_nsec)

    def get_sw_dst(self, dpid, out_port):
        for key in self.awareness.link_to_port:
            src_port = self.awareness.link_to_port[key][0]
            if key[0] == dpid and src_port == out_port:
                dst_sw = key[1]
                dst_port = self.awareness.link_to_port[key][1]
                return (dst_sw, dst_port)

    def get_link_bw(self, file, src_dpid, dst_dpid):
        fin = open(file, "r")
        bw_capacity_dict = {}
        for line in fin:
            a = line.split(',')
            if a:
                s1 = a[0]
                s2 = a[1]
                # bwd = a[2] #random capacities
                bwd = a[3]  # original capacities
                bw_capacity_dict.setdefault(s1, {})
                bw_capacity_dict[str(a[0])][str(a[1])] = bwd
        fin.close()
        bw_link = bw_capacity_dict[str(src_dpid)][str(dst_dpid)]
        return bw_link

    def get_free_bw(self, port_capacity, speed):
        # freebw: Kbit/s
        return max(port_capacity - (speed / 1000.0), 0)

    # ------------------MANAGEMENT PLANE MODULE ---------------------------
    # ------------------PROCESS STATISTICS MODULE FUNCTIONS----------------

    def get_flow_loss(self):
        # Get per flow loss
        bodies = self.stats['flow']
        for dp in bodies.keys():
            list_flows = sorted([flow for flow in bodies[dp] if flow.priority == 1],
                                key=lambda flow: (flow.match.get('ipv4_src'), flow.match.get('ipv4_dst')))
            for stat in list_flows:
                out_port = stat.instructions[0].actions[0].port
                if self.awareness.link_to_port and out_port != 1:  # get loss from ports of network
                    key = (stat.match.get('ipv4_src'), stat.match.get('ipv4_dst'))
                    tmp1 = self.flow_stats[dp][key]
                    byte_count_src = tmp1[-1][1]

                    result = self.get_sw_dst(dp, out_port)
                    dst_sw = result[0]
                    tmp2 = self.flow_stats[dst_sw][key]
                    byte_count_dst = tmp2[-1][1]
                    flow_loss = byte_count_src - byte_count_dst
                    self.save_stats(self.flow_loss[dp], key, flow_loss, 5)

    def get_port_loss(self):
        # Get loss_port
        bodies = self.stats['port']
        for dp in sorted(bodies.keys()):
            for stat in sorted(bodies[dp], key=attrgetter('port_no')):
                if self.awareness.link_to_port and stat.port_no != 1 and stat.port_no != ofproto_v1_3.OFPP_LOCAL:  # get loss form ports of network
                    key1 = (dp, stat.port_no)
                    tmp1 = self.port_stats[key1]
                    tx_bytes_src = tmp1[-1][0]
                    tx_pkts_src = tmp1[-1][8]

                    key2 = self.get_sw_dst(dp, stat.port_no)
                    tmp2 = self.port_stats[key2]
                    rx_bytes_dst = tmp2[-1][1]
                    rx_pkts_dst = tmp2[-1][9]
                    loss_port = float(tx_pkts_src - rx_pkts_dst) / tx_pkts_src  # loss rate
                    values = (loss_port, key2)
                    self.save_stats(self.port_loss[dp], key1, values, 5)

        # Calculates the total link loss and save it in self.link_loss[(node1,node2)]:loss
        for dp in self.port_loss.keys():
            for port in self.port_loss[dp]:
                key2 = self.port_loss[dp][port][-1][1]
                loss_src = self.port_loss[dp][port][-1][0]
                # tx_src = self.port_loss[dp][port][-1][1]
                loss_dst = self.port_loss[key2[0]][key2][-1][0]
                # tx_dst = self.port_loss[key2[0]][key2][-1][1]
                loss_l = (abs(loss_src) + abs(loss_dst)) / 2
                link = (dp, key2[0])
                self.link_loss[link] = loss_l * 100.0

    def get_link_free_bw(self):
        # Calculates the total free bw of link and save it in self.link_free_bw[(node1,node2)]:link_free_bw
        for dp in self.free_bandwidth.keys():
            for port in self.free_bandwidth[dp]:
                free_bw1 = self.free_bandwidth[dp][port]
                key2 = self.get_sw_dst(dp, port)  # key2 = (dp,port)
                free_bw2 = self.free_bandwidth[key2[0]][key2[1]]
                link_free_bw = (free_bw1 + free_bw2) / 2
                link = (dp, key2[0])
                self.link_free_bw[link] = link_free_bw

    def get_link_used_bw(self):
        # Calculates the total free bw of link and save it in self.link_free_bw[(node1,node2)]:link_free_bw
        for key in self.port_speed.keys():
            used_bw1 = self.port_speed[key][-1]
            key2 = self.get_sw_dst(key[0], key[1])  # key2 = (dp,port)
            used_bw2 = self.port_speed[key2][-1]
            link_used_bw = (used_bw1 + used_bw2) / 2
            link = (key[0], key2[0])
            self.link_used_bw[link] = link_used_bw

    def write_values(self):
        a = time.time()
        if self.delay.link_delay:
            for link in self.link_free_bw:
                self.net_info[link] = [round(self.link_free_bw[link], 6), round(self.delay.link_delay[link], 6),
                                       round(self.link_loss[link], 6)]
                self.net_metrics[link] = [round(self.link_free_bw[link], 6), round(self.link_used_bw[link], 6),
                                          round(self.delay.link_delay[link], 6), round(self.link_loss[link], 6)]

            with open(
                    '/home/ubuntu/ryu/ryu/app/RSIR-Reinforcement-Learning-and-SDN-Intelligent-Routing/SDNapps_proac/net_info.csv',
                    'w') as csvfile:
                header_names = ['node1', 'node2', 'bwd', 'delay', 'pkloss']
                file = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
                links_in = []
                file.writerow(header_names)
                for link, values in sorted(self.net_info.items()):
                    links_in.append(link)
                    tup = (link[1], link[0])
                    if tup not in links_in:
                        file.writerow([link[0], link[1], values[0], values[1], values[2]])

            file_metrics = '/home/ubuntu/ryu/ryu/app/RSIR-Reinforcement-Learning-and-SDN-Intelligent-Routing/SDNapps_proac/Metrics/' + str(
                self.count_monitor) + '_net_metrics.csv'
            with open(file_metrics, 'w') as csvfile:
                header_ = ['node1', 'node2', 'free_bw', 'used_bw', 'delay', 'pkloss']
                file = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
                links_in = []
                file.writerow(header_)
                for link, values in sorted(self.net_metrics.items()):
                    links_in.append(link)
                    tup = (link[1], link[0])
                    if tup not in links_in:
                        file.writerow([link[0], link[1], values[0], values[1], values[2], values[3]])
            b = time.time()
            return

    # ---------------------CONTROL PLANE FUNCTIONS---------------------------------
    # ---------------------STATISTICS MODULE FUNCTIONS ----------------------------

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def flow_stats_reply_handler(self, ev):
        """
            Save flow stats reply information into self.flow_stats.
            Calculate flow speed and Save it.
            self.flow_stats = {dpid:{(ipv4_src, ipv4_dst):[(packet_count, byte_count, duration_sec,  duration_nsec),],},}
            self.flow_speed = {dpid:{(ipv4_src, ipv4_dst):[speed,],},}
            self.flow_loss = {dpid:{(ipv4_src, ipv4_dst, dst_sw):[loss,],},}
        """

        body = ev.msg.body
        dpid = ev.msg.datapath.id
        self.stats['flow'][dpid] = body
        self.flow_stats.setdefault(dpid, {})
        self.flow_speed.setdefault(dpid, {})
        self.flow_loss.setdefault(dpid, {})

        # flows.append('table_id=%s '
        # 'duration_sec=%d duration_nsec=%d '
        # 'priority=%d '
        # 'idle_timeout=%d hard_timeout=%d flags=0x%04x '
        # 'cookie=%d packet_count=%d byte_count=%d '
        # 'match=%s instructions=%s' %
        # (stat.table_id,
        #  stat.duration_sec, stat.duration_nsec,
        #  stat.priority,
        #  stat.idle_timeout, stat.hard_timeout, stat.flags,
        #  stat.cookie, stat.packet_count, stat.byte_count,
        #  stat.match, stat.instructions)

        for stat in sorted([flow for flow in body if flow.priority == 1],
                           key=lambda flow: (flow.match.get('ipv4_src'),
                                             flow.match.get('ipv4_dst'))):
            key = (stat.match.get('ipv4_src'), stat.match.get('ipv4_dst'))

            value = (stat.packet_count, stat.byte_count,
                     stat.duration_sec, stat.duration_nsec)  # duration_sec: Time flow was alive in seconds
            # duration_nsec: Time flow was alive in nanoseconds beyond duration_sec
            self.save_stats(self.flow_stats[dpid], key, value, 5)

            # CALCULATE FLOW BYTE RATE 
            pre = 0
            period = setting.MONITOR_PERIOD
            tmp = self.flow_stats[dpid][key]
            if len(tmp) > 1:
                pre = tmp[-2][1]  # penultimo flow byte_count
                period = self.get_period(tmp[-1][2], tmp[-1][3],  # valores  (sec,nsec) ultimo flow, penultimo flow)
                                         tmp[-2][2], tmp[-2][3])
            speed = self.get_speed(self.flow_stats[dpid][key][-1][1],
                                   # ultimo flow byte_count, penultimo byte_count, periodo
                                   pre, period)
            self.save_stats(self.flow_speed[dpid], key, speed, 5)  # bits/s

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def port_stats_reply_handler(self, ev):
        a = time.time()
        body = ev.msg.body
        dpid = ev.msg.datapath.id

        self.stats['port'][dpid] = body
        self.free_bandwidth.setdefault(dpid, {})
        self.port_loss.setdefault(dpid, {})
        """
            Save port's stats information into self.port_stats.
            Calculate port speed and Save it.
            self.port_stats = {(dpid, port_no):[(tx_bytes, rx_bytes, rx_errors, duration_sec,  duration_nsec),],}
            self.port_speed = {(dpid, port_no):[speed,],}
                    
        Replay message content:
            (stat.port_no,
             stat.rx_packets, stat.tx_packets,
             stat.rx_bytes, stat.tx_bytes,
             stat.rx_dropped, stat.tx_dropped,
             stat.rx_errors, stat.tx_errors,
             stat.rx_frame_err, stat.rx_over_err,
             stat.rx_crc_err, stat.collisions,
             stat.duration_sec, stat.duration_nsec))
        """

        for stat in sorted(body, key=attrgetter('port_no')):
            port_no = stat.port_no
            key = (dpid, port_no)
            value = (stat.tx_bytes, stat.rx_bytes, stat.rx_errors,
                     stat.duration_sec, stat.duration_nsec, stat.tx_errors, stat.tx_dropped, stat.rx_dropped,
                     stat.tx_packets, stat.rx_packets)
            self.save_stats(self.port_stats, key, value, 5)

            if port_no != ofproto_v1_3.OFPP_LOCAL:
                if port_no != 1 and self.awareness.link_to_port:
                    # Get port speed and Save it.
                    pre = 0
                    period = setting.MONITOR_PERIOD
                    tmp = self.port_stats[key]
                    if len(tmp) > 1:
                        # Calculate with the tx_bytes and rx_bytes
                        pre = tmp[-2][0] + tmp[-2][1]  # penultimo port tx_bytes
                        period = self.get_period(tmp[-1][3], tmp[-1][4], tmp[-2][3], tmp[-2][
                            4])  # periodo entre el ultimo y penultimo total bytes en el puerto
                    speed = self.get_speed(self.port_stats[key][-1][0] + self.port_stats[key][-1][1], pre,
                                           period)  # speed in bits/s
                    self.save_stats(self.port_speed, key, speed, 5)

                    # Get links capacities

                    file = '/home/ubuntu/ryu/ryu/app/RSIR-Reinforcement-Learning-and-SDN-Intelligent-Routing/SDNapps_proac/bw_r.txt'  # link capacities
                    link_to_port = self.awareness.link_to_port

                    for k in list(link_to_port.keys()):
                        if k[0] == dpid:
                            if link_to_port[k][0] == port_no:
                                dst_dpid = k[1]

                                # FUNCIONA CON LISTA-----------------------------
                                # list_dst_dpid = [k for k in list(link_to_port.keys()) if k[0] == dpid and link_to_port[k][0] == port_no]
                                # if len(list_dst_dpid) > 0:
                                #     dst_dpid = list_dst_dpid[0][1]
                                # ----------------------------------------- 
                                bw_link = float(self.get_link_bw(file, dpid, dst_dpid))
                                port_state = self.port_features.get(dpid).get(port_no)

                                if port_state:
                                    bw_link_kbps = bw_link * 1000.0
                                    self.port_features[dpid][port_no].append(bw_link_kbps)
                                    free_bw = self.get_free_bw(bw_link_kbps, speed)
                                    self.free_bandwidth[dpid][port_no] = free_bw

    @set_ev_cls(ofp_event.EventOFPPortDescStatsReply, MAIN_DISPATCHER)
    def port_desc_stats_reply_handler(self, ev):
        """
            Save port description info.
        """
        msg = ev.msg
        dpid = msg.datapath.id
        ofproto = msg.datapath.ofproto

        config_dict = {ofproto.OFPPC_PORT_DOWN: "Down",
                       ofproto.OFPPC_NO_RECV: "No Recv",
                       ofproto.OFPPC_NO_FWD: "No Farward",
                       ofproto.OFPPC_NO_PACKET_IN: "No Packet-in"}

        state_dict = {ofproto.OFPPS_LINK_DOWN: "Down",
                      ofproto.OFPPS_BLOCKED: "Blocked",
                      ofproto.OFPPS_LIVE: "Live"}

        ports = []
        for p in ev.msg.body:
            if p.port_no != 1:

                ports.append('port_no=%d hw_addr=%s name=%s config=0x%08x '
                             'state=0x%08x curr=0x%08x advertised=0x%08x '
                             'supported=0x%08x peer=0x%08x curr_speed=%d '
                             'max_speed=%d' %
                             (p.port_no, p.hw_addr,
                              p.name, p.config,
                              p.state, p.curr, p.advertised,
                              p.supported, p.peer, p.curr_speed,
                              p.max_speed))
                if p.config in config_dict:
                    config = config_dict[p.config]
                else:
                    config = "up"

                if p.state in state_dict:
                    state = state_dict[p.state]
                else:
                    state = "up"

                # Recording data.
                port_feature = [config, state]
                self.port_features[dpid][p.port_no] = port_feature

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def port_status_handler(self, ev):
        """
            Handle the port status changed event.
        """
        msg = ev.msg
        ofproto = msg.datapath.ofproto
        reason = msg.reason
        dpid = msg.datapath.id
        port_no = msg.desc.port_no

        reason_dict = {ofproto.OFPPR_ADD: "added",
                       ofproto.OFPPR_DELETE: "deleted",
                       ofproto.OFPPR_MODIFY: "modified", }

        if reason in reason_dict:
            print("switch%d: port %s %s" % (dpid, reason_dict[reason], port_no))
        else:
            print("switch%d: Illegal port state %s %s" % (dpid, port_no, reason))

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        '''
            In packet_in handler, we need to learn access_table by ARP and IP packets.
            Therefore, the first packet from UNKOWN host MUST be ARP
        '''
        msg = ev.msg
        datapath = msg.datapath
        dpid = datapath.id
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        pkt = packet.Packet(msg.data)
        arp_pkt = pkt.get_protocol(arp.arp)
        ipv4_pkt = pkt.get_protocol(ipv4.ipv4)
        if isinstance(arp_pkt, arp.arp):
            self.arp_forwarding(msg, arp_pkt.src_ip, arp_pkt.dst_ip)
        # if isinstance(ipv4_pkt, ipv4.ipv4):
        #     ip_proto = ipv4_pkt.proto
        #     if ip_proto == 6:
        #         tcp_pkt = pkt.get_protocol(tcp.tcp)
        #         src_ip = ipv4_pkt.src
        #         dst_ip = ipv4_pkt.dst
        #         match = parser.OFPMatch(
        #             eth_type=ETH_TYPE_IP, ipv4_src=src_ip, ipv4_dst=dst_ip,
        #             ip_proto=6, tcp_src=tcp_pkt.src_port, tcp_dst=tcp_pkt.dst_port
        #         )
        #         path = self.get_path(src_ip.split('.')[-1], dst_ip.split('.')[-1])
        #
        #         first_dp = self.datapaths[path[0]]
        #         back_match = parser.OFPMatch(
        #             eth_type=ETH_TYPE_IP, ipv4_src=dst_ip, ipv4_dst=src_ip,
        #             ip_proto=6, tcp_src=tcp_pkt.dst_port, tcp_dst=tcp_pkt.src_port
        #         )
        #
        #         if len(path) > 2:
        #             for i in range(1, len(path) - 1):
        #                 port = self.get_port_pair_from_link(self.awareness.link_to_port,
        #                                                     path[i - 1], path[i])
        #                 port_next = self.get_port_pair_from_link(self.awareness.link_to_port,
        #                                                          path[i], path[i + 1])
        #                 if port and port_next:
        #                     src_port, dst_port = port[1], port_next[0]
        #                     datapath = self.datapaths[path[i]]
        #                     actions = [parser.OFPActionOutput(dst_port)]
        #                     actions_back = [parser.OFPActionOutput(src_port)]
        #                     self.add_flow(datapath, 3, match, actions,
        #                                   idle_timeout=250, hard_timeout=0)
        #                     self.add_flow(datapath, 3, back_match, actions_back,
        #                                   idle_timeout=250, hard_timeout=0)
        #         if len(path) > 1:
        #             # The last flow entry
        #             port_pair = self.get_port_pair_from_link(self.awareness.link_to_port,
        #                                                      path[-2], path[-1])
        #             if port_pair is None:
        #                 self.logger.info("Port is not found")
        #                 return
        #             src_port = port_pair[1]
        #             dst_port = 1
        #             last_dp = self.datapaths[path[-1]]
        #             actions = [parser.OFPActionOutput(dst_port)]
        #             actions_back = [parser.OFPActionOutput(src_port)]
        #             self.add_flow(last_dp, 3, match, actions,
        #                           idle_timeout=250, hard_timeout=0)
        #             self.add_flow(last_dp, 3, back_match, actions_back,
        #                             idle_timeout=250, hard_timeout=0)
        #
        #             # The first flow entry
        #             port_pair = self.get_port_pair_from_link(self.awareness.link_to_port, path[0], path[1])
        #             if port_pair is None:
        #                 self.logger.info("Port not found in first hop.")
        #                 return
        #             out_port = port_pair[0]
        #             actions = [parser.OFPActionOutput(out_port)]
        #             actions_back = [parser.OFPActionOutput(1)]  # host port
        #             self.add_flow(first_dp, 3, match, actions,
        #                           idle_timeout=250, hard_timeout=0)
        #             self.add_flow(first_dp, 3, back_match, actions_back,
        #                           idle_timeout=250, hard_timeout=0)
        #         else:
        #             out_port = 1
        #             actions = [parser.OFPActionOutput(out_port)]
        #             actions_back = [parser.OFPActionOutput(1)]
        #             self.add_flow(first_dp, 3, match, actions,
        #                           idle_timeout=250, hard_timeout=0)
        #             self.add_flow(first_dp, 3, back_match, actions_back,
        #                             idle_timeout=250, hard_timeout=0)

    def show_stat(self, _type):
        '''
            Show statistics information according to data type.
            _type: 'port' / 'flow'
        '''
        if setting.TOSHOW is False:
            return

        if _type == 'flow' and self.awareness.link_to_port:
            bodies = self.stats['flow']
            print('datapath         ''   ip_src        ip-dst      '
                  'out-port packets  bytes  flow-speed(b/s)')
            print('---------------- ''  -------- ----------------- '
                  '-------- -------- -------- -----------')
            for dpid in bodies.keys():
                for stat in sorted(
                        [flow for flow in bodies[dpid] if flow.priority == 1],
                        key=lambda flow: (flow.match.get('ipv4_src'),
                                          flow.match.get('ipv4_dst'))):
                    key = (stat.match.get('ipv4_src'), stat.match.get('ipv4_dst'))
                    print('{:>016} {:>9} {:>17} {:>8} {:>8} {:>8} {:>8.1f}'.format(
                        dpid,
                        stat.match['ipv4_src'], stat.match['ipv4_dst'],  # flow match
                        stat.instructions[0].actions[0].port,  # port
                        stat.packet_count, stat.byte_count,
                        abs(self.flow_speed[dpid][key][-1])))
            print()

        if _type == 'port':  # and self.awareness.link_to_port:
            bodies = self.stats['port']
            print('\ndatapath  port '
                  '   rx-pkts     rx-bytes ''   tx-pkts     tx-bytes '
                  ' port-bw(Kb/s)  port-speed(Kb/s)  port-freebw(Kb/s) '
                  ' port-state  link-state')
            print('--------  ----  '
                  '---------  -----------  ''---------  -----------  '
                  '-------------  ---------------  -----------------  '
                  '----------  ----------')
            format_ = '{:>8}  {:>4}  {:>9}  {:>11}  {:>9}  {:>11}  {:>13.3f}  {:>15.5f}  {:>17.5f}  {:>10}  {:>10}  {:>10}  {:>10}'

            for dpid in sorted(bodies.keys()):
                for stat in sorted(bodies[dpid], key=attrgetter('port_no')):
                    if stat.port_no != 1:
                        if stat.port_no != ofproto_v1_3.OFPP_LOCAL:  # port 1 is the host output
                            if self.free_bandwidth[dpid]:
                                self.logger.info(format_.format(
                                    dpid, stat.port_no,  # datapath , num_port
                                    stat.rx_packets, stat.rx_bytes,
                                    stat.tx_packets, stat.tx_bytes,
                                    self.port_features[dpid][stat.port_no][2],  # port_bw (kb/s) MAX
                                    abs(self.port_speed[(dpid, stat.port_no)][-1] / 1000.0),  # port_speed Kbits/s
                                    self.free_bandwidth[dpid][stat.port_no],  # port_free bw kb/s
                                    self.port_features[dpid][stat.port_no][0],  # port state
                                    self.port_features[dpid][stat.port_no][1],  # link state
                                    stat.rx_dropped, stat.tx_dropped))
            print()

        if _type == 'link':
            print('\nnode1  node2  used-bw(Kb/s)   free-bw(Kb/s)    latency(ms)     loss')
            print('-----  -----  --------------   --------------   -----------    ---- ')

            format_ = '{:>5}  {:>5} {:>14.5f}  {:>14.5f}  {:>12}  {:>12}'

            links_in = []
            for link, values in sorted(self.net_info.items()):
                links_in.append(link)
                tup = (link[1], link[0])
                if tup not in links_in:
                    print(format_.format(link[0], link[1],
                                         self.link_used_bw[link] / 1000.0,
                                         values[0], values[1], values[2]))
