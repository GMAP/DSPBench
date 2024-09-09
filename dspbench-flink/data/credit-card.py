import random   
import string  
import secrets 

fraud=open('/home/gmap/DSPBench/dspbench-flink/data/credit-card.dat', 'w')

idk=["HHL","HHN","HHS","HNL","HNN","HNS","LHL","LHN","LHS","LNL","LNN","LNS","MHL","MHN","MHS","MNL","MNN","MNS"]

for i in range(800000): 
    entityID = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for x in range(3)) 
    record = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for x in range(12)) 

    fraud.write(entityID+","+record+","+idk[random.randint(0,17)]+"\n")