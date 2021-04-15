
import json
import time
from datetime import datetime
import pandas as pd
import sys
import AWSIoTPythonSDK
import os
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
sys.path.insert(0, os.path.dirname(AWSIoTPythonSDK.__file__))

myMQTTClient = AWSIoTMQTTClient('pomelo')
myMQTTClient.configureEndpoint(
    'a1nqb22lhmgglf-ats.iot.us-west-2.amazonaws.com', 8883)
myMQTTClient.configureCredentials(
    'AmazonRootCA1.pem', 'b7c56fe9c2-private.pem.key', 'b7c56fe9c2-certificate.pem.crt')

try:
    myMQTTClient.connect()
    df1 = pd.read_csv("drumheller_replay_1_out.csv")
    items1 = json.loads(df1.to_json(orient="records"))
    df2 = pd.read_csv("drumheller_replay_1_out_with_station.csv")
    items2 = json.loads(df2.to_json(orient="records"))
    df3 = pd.read_csv("drumheller_replay_1_out_reverse.csv")
    items3 = json.loads(df3.to_json(orient="records"))
    for item in range(90, len(items1)):

        f = items1[item-9:item+1] + items3[item-9:item+1]
        f.append(items2[item])
        for x in range(0, len(f)):
            f[x]["computer_time"] = datetime.now().strftime(
                "%Y-%m-%dT%H:%M:%S.000Z")
            if x < 10:
                f[x]["system_id"] = "pomelo_1_" + str(len(f)-11-x)
            elif 9 < x < 20:
                f[x]["system_id"] = "pomelo_3_" + str(len(f)-1-x)

        myMQTTClient.publish(
            "gswlab-prd/ingress/uploaded/vehicle", json.dumps(f), 0)
        print(json.dumps(f))
        time.sleep(3)
    myMQTTClient.disconnect()
except OSError as error:
    print("................................."+error)
