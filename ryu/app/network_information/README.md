编译
```bash
sudo python3 setup.py install 
```
运行
```bash
# 拓扑
sudo mn --custom geant2004.py --topo geant2004topo --mac --link tc --switch ovsk,protocol=OpenFLow13 --controller remote
pingall
py [net.get('h%d' % i).cmd('bash /home/ubuntu/Documents/GEANT2004_Traffic/TM_00/Servers/iperf_server_%02d.sh &' % i) for i in range(1,24)]
py [net.get('h%d' % i).cmd('bash /home/ubuntu/Documents/GEANT2004_Traffic/TM_00/Clients/iperf_client_%02d.sh &' % i) for i in range(1,24)]
# 控制器
ryu-manager topology_discover.py --observe-links
```