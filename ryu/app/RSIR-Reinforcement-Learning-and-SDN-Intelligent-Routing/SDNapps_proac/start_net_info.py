import pandas as pd

file = '/home/ubuntu/ryu/ryu/app/RSIR-Reinforcement-Learning-and-SDN-Intelligent-Routing/SDNapps_proac/net_info.csv'
df = pd.read_csv(file)
df.delay = 0
df.pkloss = 0

print
df
