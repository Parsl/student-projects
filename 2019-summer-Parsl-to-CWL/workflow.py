import time
import os
from scipy.integrate import odeint
from operator import itemgetter

import parsl
from parsl.app.app import python_app, bash_app
#from parsl.configs.local_threads import config

import logging
from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor
from parsl.monitoring import MonitoringHub
from parsl.addresses import address_by_hostname

# Define a configuration for using local threads and pilot jobs
#parsl.set_stream_logger()
FILENAME = 'log_monitor.txt'
parsl.set_file_logger(FILENAME, level=logging.DEBUG)
config = Config(
    executors=[
        ThreadPoolExecutor(
            max_threads=8, 
            label='local_threads'
        )
    ],
    monitoring =MonitoringHub(
        hub_address=address_by_hostname(),
        hub_port=55055,
        logging_level=logging.INFO,
        resource_monitoring_interval=10,
    ),
    strategy=None
)

parsl.clear()
parsl.load(config)
print( 'Modules Imported!')


# App that generates 
@bash_app
def echo1(inputs=[], outputs=[], stdout=parsl.AUTO_LOGNAME, stderr=parsl.AUTO_LOGNAME):
    command = '{exe} {output}'.format(
        exe    = inputs[0],
        output = outputs[0]
    )
    #print( template.format(executable=executable, output = output))
    return command

@bash_app
def echo2(inputs = [], outputs=[], stdout=parsl.AUTO_LOGNAME, stderr=parsl.AUTO_LOGNAME):
    command =  '{exe} {input_1} {input_2} {input_3} {input_4} {output}'.format(
        exe     = inputs[0],
        input_1 = inputs[1],
        input_2 = inputs[2],
        input_3 = inputs[3],
        input_4 = inputs[4],
        output  = outputs[0]
    )
    #print(executable + ' {0} {1} {2} {3} {4}'.format(inputs[0],inputs[1],inputs[2],inputs[3], outputs[0]))
    return command

@bash_app
def concat(inputs=[], outputs=[], stdout=parsl.AUTO_LOGNAME, stderr=parsl.AUTO_LOGNAME):
    command = '{exe} {file_0} {file_1} > {output}'.format(
        exe    = 'cat',
        file_0 = inputs[0],
        file_1 = inputs[1],
        output = outputs[0]
    )
    return command

@bash_app
def untar(inputs=[], outputs=[], stdout=parsl.AUTO_LOGNAME, stderr=parsl.AUTO_LOGNAME):
    #outputs = ['1.txt', '2.txt']
    command = '{exe} {arg} {tarfile} {outputfile_0} {outputfile_1}'.format(
        exe          = 'tar',
        arg          = 'xvf',
        tarfile      = inputs[0],
        outputfile_0 = outputs[0],
        outputfile_1 = outputs[1]
    )
    
    return command

future_0 = echo1(inputs =['./echo1.sh'],outputs =['echo1.txt'])
echo2_inputs = ['./echo2.sh', 'Next','College', 'Student', 'Athelete' ]
future_1 = echo2(inputs=echo2_inputs,outputs = ['echo2.txt'],)
future_2 = concat(inputs=[future_0.outputs[0],future_1.outputs[0]], outputs=['NCSA.txt'])

future_3 = untar(inputs=['texts.tar'], outputs = ['1.txt', '2.txt'], )
future_4 = concat(inputs = future_3.outputs, outputs = ['SPIN.txt'])
future_5 = concat(inputs = future_2.outputs + future_4.outputs, outputs = ['Result.txt'])
futures = [future_0, future_1,future_2,future_3,future_4,future_5 ]
for i in range(len(futures)):
    try:
        futures[i].result()
    except:
        print("Future {0} failed".format(i))        

print("Execution successful")