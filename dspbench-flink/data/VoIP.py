import random
import datetime

error = ['true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'false', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true',
        'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true', 'true']

def gen_numbers():
    allowed_numbers = '0123456789'
    indx = '1'
    for i in range(10):
        indx += allowed_numbers[random.randint(0, 9)]
    return indx

file=open('/home/gmap/DSPBench/dspbench-flink/data/VoIP.txt', 'w')

for i in range(800000): 
    CallingNumber = gen_numbers()
    CalledNumber = gen_numbers()

    while(CallingNumber == CalledNumber):
        CalledNumber = gen_numbers()
    
    AnswerTime = datetime.datetime.now() + datetime.timedelta(seconds=random.randint(0, 60))
    CallDuration = random.randint(0, 60*5)
    CallEstablished = error[random.randint(0,199)]  

    #calling,called,answerTime,calling;called;answerTime;duration;estabilished
    file.write(CallingNumber+','+CalledNumber+','+AnswerTime.strftime('%Y-%m-%dT%H:%M:%S')+','+CallingNumber+';'+CalledNumber+';'+AnswerTime.strftime('%Y-%m-%dT%H:%M:%S')+';'+str(CallDuration)+';'+CallEstablished+"\n")

