#!/usr/bin/python3
from PiConfigReader import Reader
import MqttBrokerConnection
import logging
import datetime
import time
from threading import Lock
from enum import Enum
import json
from random import randint

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
      self.periodic_publish_handler = None

   def set_mqtt_broker_connection(self, broker_connection):
      self.broker_connection = broker_connection

   def set_periodic_publish_handler(self, periodic_publish_handler):
      self.periodic_publish_handler = periodic_publish_handler

   def on_connect(self):
      if self.periodic_publish_handler:
         self.periodic_publish_handler.start()

class PublishCallbackHandler:
   def __init__(self, pi_config, subscribe_topics_config, publish_topics_config):
      self.pi_config = pi_config
      self.subscribe_topics_config = subscribe_topics_config
      self.publish_topics_config = publish_topics_config
      self.baseline_pressure = 50
      self.pressure_lock = Lock()
      self.speed = 50
      self.speed_lock = Lock()
      self.broker_connection = None

   def set_broker_connection(self, broker_connection):
      self.broker_connection = broker_connection
     
   def get_baseline_pressure(self):
      with self.pressure_lock:
         baseline_pressure = self.baseline_pressure
      return baseline_pressure

   def get_speed(self):
      with self.speed_lock:
         speed = self.speed
      return speed

   def set_speed(self, speed):
      with self.speed_lock:
         self.speed = speed

   def handle(self, msg):
      if msg.topic == self.subscribe_topics_config.get_ui():
         # UI publishes in SparkPlug format, so parse it differently
         inboundPayload = sparkplug_b_pb2.Payload()
         inboundPayload.ParseFromString(msg.payload)
         for metric in inboundPayload.metrics:
            logging.debug('Tag Name: {}'.format(metric.name))
            if metric.name == "oil_pressure":
               with self.pressure_lock:
                  self.baseline_pressure = metric.int_value
                  logging.debug('Baseline pressure changed to: {}'.format(self.baseline_pressure))
            elif metric.name == "pressure_threshold":
               pressure_threshold = metric.int_value
               # Send the threshold message in streams format
               msg = "{\"gateway-name\": \"" + self.pi_config.get_name() + "\", "
               msg += "\"version\": 1, "
               msg += "\"system-time-ms-local\": " + str(time.time() * 1000) + ", "
               msg += "\"threshold\": \"" + str(pressure_threshold) + "\" "
               msg += "}"
               self.broker_connection.publish(msg, self.publish_topics_config.get_pressure_threshold(),
                                              qos=0, retain=True, check_for_completion=False)
               if self.pi_config.get_print_debug_messages():
                  logging.debug('Pressure Threshold Msg: {}'.format(msg))
      else:
         parsed_json = json.loads(str(msg.payload.decode('UTF-8')))
         # Backend and other entities publish in JSON format
         # Check if this is a valid command JSON message with the key value in it
         if 'command' in parsed_json and parsed_json['command'] == 'Set Speed' and 'value' in parsed_json:
            # One last check: Make sure that value is an integer
            if not isinstance(parsed_json['value'], int):
               logging.error('Got Set Speed command: {} on topic: {} with a non-numeric value: {}'.format(str(msg.payload.decode('UTF-8')), msg.topic, parsed_json['value']))
            else:
               with self.speed_lock:
                  speed = int(parsed_json['value'])
                  # Save the new speed sent by backend in the global
                  if self.pi_config.get_print_debug_messages():
                     logging.debug('Set speed command received with new speed of: {}'.format(speed))
               # Send the speed message
               msg = "{\"gateway-name\": \"" + self.pi_config.get_name() + "\", "
               msg += "\"version\": 1, "
               msg += "\"system-time-ms-local\": " + str(time.time() * 1000) + ", "
               msg += "\"speed\": \"" + str(parsed_json['value']) + "\" "
               msg += "}"
               self.broker_connection.publish(msg, self.publish_topics_config.get_well_pump_speed(), qos=0, retain=False, check_for_completion=False)
               if self.pi_config.get_print_debug_messages():
                  logging.debug('MQTT Well Pump Speed MQTT Msg: {}'.format(msg))

class WellPumpPeriodicPublishHandler:
   def __init__(self, pi_config, publish_topics_config, publish_callback_handler, broker_connection):
      self.baseline_temperature = 70
      self.pi_config = pi_config
      self.publish_topics_config = publish_topics_config
      self.publish_callback_handler = publish_callback_handler
      self.broker_connection = broker_connection
      self.periodic_publish = MqttBrokerConnection.PeriodicPublish(pi_config, self)

   def start(self):
      self.periodic_publish.start()

   # Temperature and pressure are to be randomly generated in the range of
   # +/- 10% of their current baseline values
   def generate_random_value_within_ten_percent_of(self, num):
      return randint(int(0.9 * num), int(1.1 * num))

   def calculate_production_volume(self, pressure, speed):
      if speed == 0:
         barrelsPerDay = 0
      else:
         barrelsPerDay = (pressure + speed)/10.0
      gallonsPerDay = 42.0 * barrelsPerDay
      # Every 2 seconds, we act as if it is 1 hour since last interval
      volumePer3Hours = gallonsPerDay/24.0
      logging.debug("At pressure: {}, speed: {} -> barrelsPerDay = {}, gallonsPerDay = {} and volumeFor2Secs = {}".format(pressure, speed, barrelsPerDay, gallonsPerDay, volumePer3Hours))
      return volumePer3Hours

   def handle(self):
      baseline_pressure = self.publish_callback_handler.get_baseline_pressure()
      instantaneous_pressure = self.generate_random_value_within_ten_percent_of(baseline_pressure)
      speed = self.publish_callback_handler.get_speed()
      volume = self.calculate_production_volume(instantaneous_pressure, speed)

      # Send the production volume message
      msg = "{\"gateway-name\": \"" + self.pi_config.get_name() + "\", "
      msg += "\"version\": 1, "
      msg += "\"system-time-ms-local\": " + str(time.time() * 1000) + ", "
      msg += "\"volume\": \"" + str(volume) + "\""
      msg += "}"
      self.broker_connection.publish(msg, self.publish_topics_config.get_well_pump_production_volume(), qos=0, retain=False, check_for_completion=False)
      if self.pi_config.get_print_debug_messages():
         logging.debug('Well Pump Production Volume Msg: {}'.format(msg))

      # Send the pressure message
      msg = "{\"gateway-name\": \"" + self.pi_config.get_name() + "\", "
      msg += "\"version\": 1, "
      msg += "\"system-time-ms-local\": " + str(time.time() * 1000) + ", "
      msg += "\"pressure\": \"" + str(instantaneous_pressure) + "\" "
      msg += "}"
      self.broker_connection.publish(msg, self.publish_topics_config.get_well_pump_pressure(), qos=0, retain=False, check_for_completion=False)
      if self.pi_config.get_print_debug_messages():
         logging.debug("Well Pump Pressure Msg: " + msg)

      # Send the temperature message
      msg = "{\"gateway-name\": \"" + self.pi_config.get_name() + "\", "
      msg += "\"version\": 1, "
      msg += "\"system-time-ms-local\": " + str(time.time() * 1000) + ", "
      msg += "\"temperature\": \"" + str(self.generate_random_value_within_ten_percent_of(self.baseline_temperature)) + "\" "
      msg += "}"
      self.broker_connection.publish(msg, self.publish_topics_config.get_well_pump_temperature(), qos=0, retain=False, check_for_completion=False)
      if self.pi_config.get_print_debug_messages():
         logging.debug("MQTT Well Pump Temperature MQTT Msg: " + msg)

def main():
   # Initialize log file
   log_filename = '/tmp/WellPump_{date:%Y_%m_%d_%H:%M:%S}.log'.format( date=datetime.datetime.now() )
   logging.basicConfig(filename=log_filename,level=logging.DEBUG)
   print('Logging to: {}'.format(log_filename))

   # Read Pi Config
   config_reader = Reader('well-pump.yaml')

   # Add subscriptions to list 
   subscriptions = []
   subscriptions.append(Subscription(config_reader.get_subscribe_topics_config().get_well_pump_command(), 1))
   subscriptions.append(Subscription(config_reader.get_subscribe_topics_config().get_ui(), 1))

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

   # Initiate the periodic publish mechanism
   periodic_publish_handler = WellPumpPeriodicPublishHandler(config_reader.get_pi_config(), config_reader.get_publish_topics_config(),
                                                             publish_callback_handler, mqtt_broker_connection)

   connection_handler.set_periodic_publish_handler(periodic_publish_handler)

   # Start the broker connection thread
   mqtt_broker_connection.start_broker_connection_thread()

   # Wait forever
   while True:
      time.sleep(config_reader.get_pi_config().get_main_loop_delay_timer())

if __name__ == "__main__":
   main()
