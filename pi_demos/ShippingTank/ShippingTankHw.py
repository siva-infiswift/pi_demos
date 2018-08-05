#!/usr/bin/python3
from PiConfigReader import Reader
import MqttBrokerConnection
import logging
import datetime
import time
from threading import Thread, Lock
from enum import Enum
from sense_hat import SenseHat
import json

class TankActivity(Enum):
    Quiescent=1
    Fill=2
    Drain=3

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

   def set_mqtt_broker_connection(self, broker_connection):
      self.broker_connection = broker_connection

   def on_connect(self):
        # Send Tank restart status message to indicate successful restart and connectivity to broker
        msg = "{\"gateway-name\": \"" + self.pi_config.get_name() + "\", "
        msg += "\"version\": 1, "
        msg += "\"system-time-ms-local\": " + str(time.time() * 1000)
        msg += "}"
        response = self.broker_connection.publish(msg, self.publish_topics_config.get_tank_restart_status(), 
                                                  qos=0, retain=True, check_for_completion=False)

class PublishCallbackHandler:
   def __init__(self, pi_config, subscribe_topics_config):
      self.pi_config = pi_config
      self.subscribe_topics_config = subscribe_topics_config
      self.oil_volume = 0
      self.fill_rate = 100
      self.tank_activity_lock = Lock()
      self.volume_lock = Lock()
      self.volume = 0
      self.current_tank_activity = TankActivity.Quiescent
     
   def get_volume(self):
      with self.volume_lock:
         volume = self.volume
      return volume

   def set_volume(self, volume):
      with self.volume_lock:
         self.volume = volume

   def get_current_tank_activity(self):
      with self.tank_activity_lock:
         current_tank_activity = self.current_tank_activity
      return current_tank_activity

   def handle(self, msg):
      if msg.topic == self.subscribe_topics_config.get_shipping_tank_command():
         if '"command":"fill"' in str(msg.payload):
            with self.tank_acitivity_lock:
               self.current_tank_activity = TankActivity.Fill
               if PRINT_DEBUG_MSGS:
                  logging.debug("Start of tank fill activity")
         elif '"command":"drain"' in str(msg.payload):
            with self.tank_acitivity_lock:
               self.current_tank_activity = TankActivity.Drain
               if PRINT_DEBUG_MSGS:
                  logging.debug("Start of tank drain activity")
         return

      parsed_json = json.loads(str(msg.payload.decode('UTF-8')))

      if msg.topic == self.subscribe_topics_config.get_analytics_engine_fill_rate():
         if 'fill-rate' in parsed_json:
            self.fill_rate = float(parsed_json['fill-rate'])
            logging.debug("Fill rate is: {}".format(self.fill_rate))
      if msg.topic == self.subscribe_topics_config.get_knockout_tank_oil_volume():
         if 'volume' in parsed_json:
            with self.oil_volume_lock:
               self.oil_volume = float(parsed_json['volume'])
               logging.debug("Oil Volume added is: {}".format(self.oil_volume))
            # Update the tank volume depending on type of activity
            current_tank_activity = get_current_tank_activity()
            if current_tank_activity == TankActivity.Fill:
               with self.volume_lock:
                  new_volume = (self.volume + self.oil_volume) * 100.0/self.fill_rate
                  logging.debug("New volume is: {}".format(new_volume))
                  if new_volume <= 100.0:
                     self.volume = new_volume
                     new_volume = (self.volume + self.oil_volume) * 100.0/self.fill_rate
                     logging.debug("Tank Volume after filling is: {}".format(self.volume))
                  if self.volume == 100.0:
                     if self.pi_config.get_print_debug_messages():
                        logging.debug("Tank is now full")
                  elif self.volume < 100.0 and new_volume > 100.0:
                     self.volume = 100.0
                     if self.pi_config.get_print_debug_messages():
                        logging.debug("Tank is now full")

class ShippingTankPeriodicPublishHandler:
   def __init__(self, pi_config, publish_callback_handler, broker_connection):
      self.pi_config = pi_config
      self.publish_callback_handler = publish_callback_handler
      self.broker_connection = broker_connection
      self.periodic_publish = MqttBrokerConnection.PeriodicPublish(pi_config, self)
      self.periodic_publish.start()

   def handle(self):
      volume = self.publish_callback_handler.get_volume()
      current_tank_activity = self.publish_callback_handler.get_current_tank_activity()

      # Update the tank volume as part of drain activity
      if current_tank_activity == TankActivity.Drain:
         if volume >= 100.0/8:
            volume -= 100.0/8
            self.publish_callback_handler.set_volume(volume)
            if volume == 0:
               if self.pi_config.get_print_debug_messages():
                  logging.debug("Tank has been fully drained")

            msg = "{\"gateway-name\": \"" + self.pi_config.get_name() + "\", "
            msg += "\"version\": 1, "
            msg += "\"system-time-ms-local\": " + str(time.time() * 1000) + ", "
            msg += "\"production-volume-percentage\": \"" + str(volume) + "\" "
            msg += "}"
            self.broker_connection.publish(msg, self.publish_topics_config.get_tank_volume_percent(),
                                           qos=0, retain=True, check_for_completion=True)

class LEDColor:
   Green = (0, 255, 0)
   Off = (0, 0, 0)

class LEDManager(LEDColor):
   def __init__(self, publish_callback_handler):
      self.LEDThread = None
      self.publish_callback_handler = publish_callback_handler
      try:
         self.senseHat = SenseHat()
         # Clear all LEDs
         senseHat.clear()
      except Exception as error:
         logging.error('Could not connect to sense-hat. '.format(error))

   def start(self):
      self.LEDThread = Thread(target=self.manage, args=())
      self.LEDThread.start()

   def setLED(self, LED_position, LED_color):
      self.senseHat.set_pixel(LED_position[0], LED_position[1], LED_color)

   def manage(self):
      while True:
         volume = self.publish_callback_handler.get_volume()
         num_rows_to_glow = int(round(volume * 8 / 100))
         logging.debug('Volume: {} num_rows_to_glow: {}'.format(volume, num_rows_to_glow))
         # Color the number of rows to glow from bottom green
         for row in range(0, num_rows_to_glow):
            for column in range(8):
               #logging.debug('Setting ({}, {}) to green color'.format(row, column))
               self.setLED((row, column), self.Green)
               if num_rows_to_glow - prev_num_rows_to_glow > 1:
                  time.sleep(0.01)

         prev_num_rows_to_glow = num_rows_to_glow

         # Turn off the top rows of LEDs that are above num_rows_to_glow
         for row in range(num_rows_to_glow, 8):
            for column in range(8):
               #logging.debug('Clearing ({}, {})'.format(row, column))
               self.setLED((row, column), self.Off)

         time.sleep(0.5)
      

def main():
   # Initialize log file
   log_filename = '/tmp/ShippingTank_{date:%Y_%m_%d_%H:%M:%S}.log'.format( date=datetime.datetime.now() )
   logging.basicConfig(filename=log_filename,level=logging.DEBUG)
   print('Logging to: {}'.format(log_filename))

   # Read Pi Config
   config_reader = Reader('shipping-tank.yaml')

   # Add subscriptions to list 
   subscriptions = []
   subscriptions.append(Subscription(config_reader.get_subscribe_topics_config().get_shipping_tank_command(), 1))
   subscriptions.append(Subscription(config_reader.get_subscribe_topics_config().get_analytics_engine_fill_rate(), 1))
   subscriptions.append(Subscription(config_reader.get_subscribe_topics_config().get_knockout_tank_oil_volume(), 1))

   # Create a connection handler
   connection_handler = ConnectionHandler(config_reader.get_pi_config(), config_reader.get_publish_topics_config())

   # Create a publish callback handler
   publish_callback_handler = PublishCallbackHandler(config_reader.get_pi_config(), config_reader.get_subscribe_topics_config())

   # Create lock
   publish_lock = Lock()

   # Create broker connection
   mqtt_broker_connection = MqttBrokerConnection.Manager(config_reader.get_pi_config(), config_reader.get_mqtt_broker_config(),
                                                         config_reader.get_mqtt_client_config(), connection_handler,
                                                         subscriptions, publish_callback_handler, publish_lock)

   # Initialize broker connection of connection handler
   connection_handler.set_mqtt_broker_connection(mqtt_broker_connection)

   # Start the broker connection thread
   mqtt_broker_connection.start_broker_connection_thread()

   # Initiate the periodic publish mechanism
   periodic_publish_handler = ShippingTankPeriodicPublishHandler(config_reader.get_pi_config(), publish_callback_handler, 
                                                                 mqtt_broker_connection)

   # Start LED manager
   led_manager = LEDManager(publish_callback_handler)
   led_manager.start()
   

   # Wait forever
   while True:
      time.sleep(config_reader.get_pi_config().get_main_loop_delay_timer())

if __name__ == "__main__":
   main()
