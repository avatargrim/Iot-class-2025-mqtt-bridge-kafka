from quixstreams import Application
import paho.mqtt.client as paho
from paho import mqtt
from datetime import datetime
import signal
import json
import time
import sys
import os
import logging

# Load environment variables (useful when working locally)
from dotenv import load_dotenv
# load_dotenv(os.path.dirname(os.path.abspath(__file__))+"/.env")
load_dotenv(".env")

# Logggin env
log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

logging.basicConfig(
    level=log_level,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

MQTT_TOPIC = os.getenv("MQTT_TOPIC", "iot-frames-model")
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = os.getenv("MQTT_PORT", "1883")
MQTT_QOS = os.getenv("MQTT_QOS", "1")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "6510301032_AQ")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "192.168.1.104:9092")
UID = os.getenv("UID", "123456789")

def mqtt_protocol_version():
    if os.environ["MQTT_VERSION"] == "3.1":
        logging.info("Using MQTT version 3.1")
        return paho.MQTTv31
    if os.environ["MQTT_VERSION"] == "3.1.1":
        logging.info("Using MQTT version 3.1.1")
        return paho.MQTTv311
    if os.environ["MQTT_VERSION"] == "5":
        logging.info("Using MQTT version 5")
        return paho.MQTTv5
    logging.info("Defaulting to MQTT version 3.1.1")
    return paho.MQTTv311

def configure_authentication(mqtt_client):
    MQTT_USERNAME = os.getenv("MQTT_USERNAME", "") 
    if MQTT_USERNAME != "":
        MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
        if MQTT_PASSWORD == "":
           raise ValueError('mqtt_password must set when mqtt_username is set')
        logging.info("Using username & password authentication")
        mqtt_client.username_pw_set(os.environ["MQTT_USERNAME"], os.environ["MQTT_PASSWORD"])
        return
    logging.info("Using anonymous authentication")

# Validate the config
if KAFKA_OUTPUT_TOPIC == "":
    raise ValueError("output (topic) environment variable is required")
if MQTT_TOPIC == "":
    raise ValueError('mqtt_topic must be supplied')
if not MQTT_PORT.isnumeric():
    raise ValueError('mqtt_port must be a numeric value')

client_id = os.getenv("QUIX__DEPLOYMENT__MODEL_NAME", "bridge"+ UID)
mqtt_client = paho.Client(callback_api_version=paho.CallbackAPIVersion.VERSION2,
                          client_id = client_id, userdata = None, protocol = mqtt_protocol_version())
# mqtt_client.tls_set(tls_version = mqtt.client.ssl.PROTOCOL_TLS)  # we'll be using tls
mqtt_client.reconnect_delay_set(5, 60)
configure_authentication(mqtt_client)

# Create a Quix Application, this manages the connection to the Quix platform
app = Application(broker_address=KAFKA_BROKER,
                loglevel="INFO",
                auto_offset_reset="earliest",
                state_dir=os.path.dirname(os.path.abspath(__file__))+"/state/",
                consumer_group="iot-mqtt-bridge-kafka",
      )
# Create the producer, this is used to write data to the output topic
producer = app.get_producer()
# create a topic object for use later on
output_topic = app.topic(KAFKA_OUTPUT_TOPIC, value_serializer="json")

logging.info(f"Connected: KAFKA={KAFKA_BROKER}")
# setting callbacks for different events to see if it works, print the message etc.
def on_connect_cb(client: paho.Client, userdata: any, connect_flags: paho.ConnectFlags,
                  reason_code: paho.ReasonCode, properties: paho.Properties):
    if reason_code == 0:
        mqtt_client.subscribe(MQTT_TOPIC, qos = 1)
        logging.info(f"CONNECTED! MQTT={client.host}:{client.port}") # required for Quix to know this has connected
    else:
        logging.error(f"ERROR! - ({reason_code.value}). {reason_code.getName()}")

# print message, useful for checking if it was successful
def on_message_cb(client: paho.Client, userdata: any, msg: paho.MQTTMessage):
    # message_key = str(msg.topic).replace("/", "-")

    # logging.info(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
    logging.info(f"MQTT_BROKER={MQTT_BROKER} PORT={MQTT_PORT} TOPIC={MQTT_TOPIC} QOS={str(msg.qos)}")
    logging.info(f"\tPAYLOAD={str(msg.payload)}")

    try:
        # Decode and parse the JSON payload into a dictionary
        payload = json.loads(msg.payload.decode('utf-8'))
        message_key = payload["name"]
        # # Now it's a dict, and we can add timestamps
        payload["payload"]["timestamp"] = int(time.time() * 1000)  # Current time in milliseconds
        payload["payload"]["date"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())  # UTC date-time เวลาปัจจุบันในรูปแบบ UTC (ไม่ใช่เวลาท้องถิ่น)

        # logging.info(f"[MQTT] Received: {payload}")

        # Re-encode the modified payload to JSON string before sending
        new_payload = json.dumps(payload).encode('utf-8')

    except json.JSONDecodeError:
        logging.error("Failed to decode JSON from MQTT message.")
        message_key = str(msg.topic).replace("/", "-")
        new_payload = msg.payload  # fallback to original payload if decoding failed

    # publish to the putput topic
    producer.produce(topic=output_topic.name,
                    key=message_key,
                    value=new_payload,
                    timestamp=int(time.time() * 1000))
    logging.info(f"KAFKA_BROKER={KAFKA_BROKER} TOPIC={KAFKA_OUTPUT_TOPIC}")
    logging.info(f"\tKey={message_key} Payload={new_payload}\n")

# print which topic was subscribed to
def on_subscribe_cb(client: paho.Client, userdata: any, mid: int,
                    reason_code_list: list[paho.ReasonCode], properties: paho.Properties):
    logging.info("Subscribed: " + str(mid))
    for reason_code in reason_code_list:
        logging.info(f"\tReason code ({reason_code.value}): {reason_code.getName()}")
    
def on_disconnect_cb(client: paho.Client, userdata: any, disconnect_flags: paho.DisconnectFlags,
                     reason_code: paho.ReasonCode, properties: paho.Properties):
    logging.info(f"DISCONNECTED! Reason code ({reason_code.value}) {reason_code.getName()}!")
    
mqtt_client.on_connect = on_connect_cb
mqtt_client.on_message = on_message_cb
mqtt_client.on_subscribe = on_subscribe_cb
mqtt_client.on_disconnect = on_disconnect_cb

# connect to MQTT Cloud on port 8883 (default for MQTT)
mqtt_client.connect(MQTT_BROKER, int(MQTT_PORT))

# start the background process to handle MQTT messages
mqtt_client.loop_start()

# Define a handler function that will be called when SIGTERM is received
def handle_sigterm(signum, frame):
    logging.info("SIGTERM received, terminating connection")
    mqtt_client.loop_stop()
    logging.info("Exiting")
    sys.exit(0)

# Register the handler for the SIGTERM signal
signal.signal(signal.SIGTERM, handle_sigterm)

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    logging.error("Interrupted by the use, terminating connection")
    mqtt_client.loop_stop() # clean up
    logging.error("Exiting")