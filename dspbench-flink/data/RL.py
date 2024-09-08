import uuid
from random import randrange
import random

event=open('/home/gmap/DSPBench/dspbench-flink/data/reinforcement-events.csv', 'w')
reward=open('/home/gmap/DSPBench/dspbench-flink/data/reinforcement-rewards.csv', 'w')

actions=["page1", "page2", "page3"]
actionCtrDistr=[[30, 12], [60, 30],[80, 10]]

for i in range(800000): 
    event.write(str(uuid.uuid4())+","+str(i)+"\n")

    rand=randrange(3)
    distr=actionCtrDistr[rand]
    sum = 0

    for j in range(4): #12
        sum = sum + random.randint(1, 101)

        r = (sum - 100) / 100.0
        r2 = r * distr[1] + distr[0]

        if (r2 < 0):
            r2 = 0

        reward.write(actions[rand]+","+str(int(r2))+"\n")
