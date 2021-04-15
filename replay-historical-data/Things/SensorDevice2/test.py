from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import json
import time

def my_callback(client, userdata, message):
    print("hello you successfully subscribed")
    print(message.payload)

myMQTTClient = AWSIoTMQTTClient('pomelo')
myMQTTClient.configureEndpoint('a1nqb22lhmgglf-ats.iot.us-west-2.amazonaws.com',8883)
myMQTTClient.configureCredentials('AmazonRootCA1.pem', 'a00ed214c5-private.pem.key','a00ed214c5-certificate.pem.crt')


js={"mess":"hi"}
try:
    myMQTTClient.connect()
    myMQTTClient.publish("gswlab-prd/ingress/uploaded/vehicle",json.dumps(js), 0)
    time.sleep(0.5)
    myMQTTClient.disconnect()
except OSError as error:
    print("................................."+error)

