#!/usr/bin/python3
import sys
sys.path.insert(0, "/home/pi/Sparkplug/client_libraries/python/")
import sparkplug_b as sparkplug
from sparkplug_b import *
from PiConfigReader import Reader
import MqttBrokerConnection
import logging
import datetime
import time
from threading import Thread, Lock
import json
from gpiozero import LED, Button
from time import sleep

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

   def get_knockout_tank_status(self):
      with self.status_lock:
         status = self.knockout_tank_status
      return self.knockout_tank_status

   def set_knockout_tank_status(self, status):
      with self.status_lock:
         self.knockout_tank_status = status

   def calculate_water_cut(self, production_volume):
    return (0.3 * production_volume, 0.7 * production_volume)

   def send_volume_messages(self, volume):
      # Compute water cut and publish oil and water level MQTT messages
      (water_volume, oil_volume) = self.calculate_water_cut(volume)

      msg = "{\"gateway-name\": \"" + self.pi_config.get_name() + "\", "
      msg += "\"system-time-ms-local\": " + str(time.time()*1000) + ", "
      msg +='"volume": "' + str(oil_volume) + '" '
      msg += "}"
      self.broker_connection.publish(msg, self.publish_topics_config.get_knockout_tank_oil_volume(), qos=0, retain=False, check_for_completion=False)
      if self.pi_config.get_print_debug_messages():
         logging.debug('MQTT Knockout Tank Oil Volume Msg: {}'.format(msg))

      msg = "{\"gateway-name\": \"" + self.pi_config.get_name() + "\", "
      msg += "\"system-time-ms-local\": " + str(time.time()*1000) + ", "
      msg +='"volume": "' + str(water_volume) + '" '
      msg += "}"
      self.broker_connection.publish(msg, self.publish_topics_config.get_knockout_tank_water_volume(), qos=0, retain=False, check_for_completion=False)
      if self.pi_config.get_print_debug_messages():
         logging.debug('Knockout Tank Water Volume Msg: {}'.format(msg))
     
   def handle(self, msg):
      if msg.topic == self.subscribe_topics_config.get_well_pump_production_volume():
         parsed_json = json.loads(str(msg.payload.decode('UTF-8')))
         if 'volume' in parsed_json:
            volume = float(parsed_json['volume'])
            logging.debug("Set Volume to: {}".format(volume))
            send_volume_messages(volume)

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

class KnockoutTankHWManager:
   def __init__(self, pi_config, publish_topics_config, publish_callback_handler, mqtt_broker_connection):
      # Initialize LED and button states and handlers
      self.stop_btn = Button(27)
      self.stop_led = LED(23, active_high=False)
      self.active_led = LED(24, active_high=False)
      self.publish_callback_handler = publish_callback_handler
      self.broker_connection = mqtt_broker_connection
      self.led_thread = Thread(target=self.manage_led, args=())
      self.led_thread.start()
      self.stop_btn.when_pressed = self.btn_press_event
      self.pi_config = pi_config
      self.publish_topics_config = publish_topics_config
      
   def manage_led(self):
      while True:
         if self.publish_callback_handler.get_knockout_tank_status():
            self.active_led.on()
            self.stop_led.off()
         else:
            self.stop_led.on()
            self.active_led.off()
         sleep(0.02)

   def btn_press_event(self):
      knockout_tank_status = self.publish_callback_handler.get_knockout_tank_status()
      knockout_tank_status = not knockout_tank_status
      self.publish_callback_handler.set_knockout_tank_status(knockout_tank_status)

      msg = "{\"gateway-name\": \"" + self.pi_config.get_name() + "\", "
      msg += "\"system-time-ms-local\": " + str(time.time()*1000) + ", "
      if knockout_tank_status:
         msg +='"status": "1"'
      else:
         msg +='"status": "0"'
      msg += "}"
      self.broker_connection.publish(msg, self.publish_topics_config.get_knockout_tank_status(), qos=0, retain=False, check_for_completion=False)
      if self.pi_config.get_print_debug_messages():
         logging.debug("MQTT Knockout Tank Status Msg: " + msg)

def main():
   # Initialize log file
   log_filename = '/tmp/KnockoutTank_{date:%Y_%m_%d_%H:%M:%S}.log'.format( date=datetime.datetime.now() )
   logging.basicConfig(filename=log_filename,level=logging.DEBUG)
   print('Logging to: {}'.format(log_filename))

   # Read Pi Config
   config_reader = Reader('knockout-tank.yaml')

   # Add subscriptions to list 
   subscriptions = []
   subscriptions.append(Subscription(config_reader.get_subscribe_topics_config().get_well_pump_production_volume(), 1))
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

   hw_manager = KnockoutTankHWManager(config_reader.get_pi_config(), config_reader.get_publish_topics_config(), publish_callback_handler,
                                      mqtt_broker_connection)

   # Wait forever
   while True:
      time.sleep(config_reader.get_pi_config().get_main_loop_delay_timer())

if __name__ == "__main__":
   main()
