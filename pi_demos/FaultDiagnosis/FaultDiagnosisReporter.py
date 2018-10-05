#!/usr/bin/python3
from PiConfigReader import Reader
import MqttBrokerConnection
import logging
import datetime
import time
from threading import Lock
import json
from FaultDetector import FaultDetector

class Subscription:
   def __init__(self, topic, qos):
      self.topic = topic
      self.qos = qos

   def get_topic(self):
      return self.topic

   def get_qos(self):
      return self.qos

class ConnectionHandler:
   def __init__(self, pi_config, publish_topics_config):
      self.pi_config = pi_config
      self.publish_topics_config = publish_topics_config
      self.broker_connection = None

   def set_mqtt_broker_connection(self, broker_connection):
      self.broker_connection = broker_connection

   def on_connect(self):
      pass

class PublishCallbackHandler:
   def __init__(self, pi_config, subscribe_topics_config, publish_topics_config):
      self.pi_config = pi_config
      self.subscribe_topics_config = subscribe_topics_config
      self.publish_topics_config = publish_topics_config
      self.broker_connection = None
      self.knockout_tank_status = True
      self.status_lock = Lock()

   def set_broker_connection(self, broker_connection):
      self.broker_connection = broker_connection

   def handle(self, msg):
      if msg.topic == self.subscribe_topics_config.get_ui():
         # UI publishes in SparkPlug format, so parse it differently
         inboundPayload = sparkplug_b_pb2.Payload()
         inboundPayload.ParseFromString(msg.payload)
         for metric in inboundPayload.metrics:
            logging.debug('Tag Name: {}'.format(metric.name))
            if metric.name == "knockout_tank_status":
               self.set_knockout_tank_status(metric.boolean_value)
               msg = "{\"gateway-name\": \"" + self.pi_config.get_name() + "\", "
               msg += "\"system-time-ms-local\": " + str(time.time()*1000) + ", "
               if metric.boolean_value:
                  msg +='"status": "1" '
               else:
                  msg +='"status": "0" '
               msg += "}"
               self.broker_connection.publish(msg, self.publish_topics_config.get_knockout_tank_status(), qos=0, retain=False, check_for_completion=False)
               if self.pi_config.get_print_debug_messages():
                  logging.debug('Knockout Tank Status Msg: {}'.format(msg))

def main():
   # Initialize log file
   log_filename = '/tmp/FaultDiagnosisReporter_{date:%Y_%m_%d_%H:%M:%S}.log'.format( date=datetime.datetime.now() )
   logging.basicConfig(filename=log_filename,level=logging.DEBUG)
   print('Logging to: {}'.format(log_filename))

   # Read Pi Config
   config_reader = Reader('fault-diagnosis-reporter.yaml')

   # Add subscriptions to list 
   subscriptions = []
   subscriptions.append(Subscription(config_reader.get_subscribe_topics_config().get_ui(), 0))

   # Create a connection handler
   connection_handler = ConnectionHandler(config_reader.get_pi_config(), config_reader.get_publish_topics_config())

   # Create a publish callback handler
   publish_callback_handler = PublishCallbackHandler(config_reader.get_pi_config(), config_reader.get_subscribe_topics_config(),
                                                     config_reader.get_publish_topics_config())

   # Create lock
   publish_lock = Lock()

   # Create broker connection
   mqtt_broker_connection = MqttBrokerConnection.Manager(config_reader.get_pi_config(), config_reader.get_mqtt_broker_config(),
                                                         config_reader.get_mqtt_client_config(), connection_handler,
                                                         subscriptions, publish_callback_handler, publish_lock)

   # Initialize broker connection of connection handler
   connection_handler.set_mqtt_broker_connection(mqtt_broker_connection)
   publish_callback_handler.set_broker_connection(mqtt_broker_connection)

   # Start the broker connection thread
   mqtt_broker_connection.start_broker_connection_thread()

   # Wait forever
   while True:
      time.sleep(config_reader.get_pi_config().get_main_loop_delay_timer())

if __name__ == "__main__":
   main()
