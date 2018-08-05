import paho.mqtt.client as mqtt
import time
from threading import Thread, Lock
import PiConfigReader
import logging

class Manager:
   def __init__(self, pi_config, mqtt_broker_config, mqtt_client_config, connection_handler, 
                subscriptions, publish_callback_handler, publish_lock):
      self.pi_config = pi_config
      self.mqtt_broker_config = mqtt_broker_config
      self.mqtt_client_config = mqtt_client_config
      self.keepalive_interval = 60
      self.connected_to_broker = False
      self.broker_connection_thread = None
      self.mqtt_client = None
      self.connection_handler = connection_handler
      self.subscriptions = subscriptions
      self.publish_callback_handler = publish_callback_handler
      self.publish_lock = publish_lock
      self.current_mqtt_host = self.mqtt_broker_config.get_host()

   def start_broker_connection_thread(self):
      self.broker_connection_thread = Thread(target=self.manage_broker_connection, args=())
      self.broker_connection_thread.start()

   def _connect_to_broker(self):
      try:
         # Setup MQTT Client object
         self.mqtt_client = mqtt.Client(client_id=self.mqtt_client_config.get_client_id(), clean_session=False,  protocol=mqtt.MQTTv311, transport="tcp")
         self.mqtt_client.username_pw_set(self.mqtt_client_config.get_username(), self.mqtt_client_config.get_password())
         
         # Setup asynchronous client callback functions
         self.mqtt_client.on_connect = self.on_connect
         self.mqtt_client.on_disconnect = self.on_disconnect
         self.mqtt_client.on_message = self.on_message
   
         # Connect to the broker now
         self.mqtt_client.connect(self.current_mqtt_host, self.mqtt_broker_config.get_port(), self.keepalive_interval)
         self.mqtt_client.loop_start()
      except Exception as error:
         logging.debug('Failed to connect to {}'.format(self.current_mqtt_host))

   def manage_broker_connection(self):
      while True:
         if self.connected_to_broker:
            logging.debug('MQTT client is connected to {}, not doing anything'.format(self.current_mqtt_host))
            time.sleep(5)
         else:
            self._connect_to_broker()
            timeout_secs = 0
            max_timeout_secs = 7;
            while (not self.connected_to_broker) and timeout_secs < max_timeout_secs:
                logging.debug('current_mqtt_host = {}. Waiting {} of {} seconds ...'.format(self.current_mqtt_host, timeout_secs, max_timeout_secs))
                time.sleep(1)
                timeout_secs += 1
            if not self.connected_to_broker:
                # Try the other host.
                if (self.current_mqtt_host == self.mqtt_broker_config.get_host()):
                    self.current_mqtt_host = self.mqtt_broker_config.get_fallback()
                else:
                    self.current_mqtt_host = self.mqtt_broker_config.get_host()

                logging.debug('Timed out. Changing MQTT host to {}'.format(self.current_mqtt_host))


   def _update_connection_status(self, status):
      if status:
         self.connected_to_broker = True
         if self.pi_config.get_print_debug_messages():
            logging.debug('Connected to broker')
      else:
         self.connected_to_broker = False
         if self.pi_config.get_print_debug_messages():
            logging.debug('Broker connection disconnected')

   def on_connect(self, client, userdata, flags, rc):
      if rc == 0:
         logging.debug('Successfully connected to {}'.format(self.current_mqtt_host))
         self._update_connection_status(True)

         self.connection_handler.on_connect()

         for subscription in self.subscriptions:
            self.mqtt_client.subscribe(topic=subscription.get_topic(), qos=subscription.get_qos())
      else:
         self._update_connection_status(False)
         logging.debug('Broker connection refused. Code = {}'.format(rc))
         self.mqtt_client.loop_stop()

   def on_disconnect(self, client, userdata, rc):
      self._update_connection_status(False)
      logging.debug('Broker disconnected. Code = {}'.format(rc))
      self.mqtt_client.loop_stop()

   def on_message(self, client, userdata, msg):
      logging.debug('{} payload = {}'.format(msg.topic, str(msg.payload)))
      self.publish_callback_handler.handle(msg)

   def publish(self, msg, topic, qos, retain, check_for_completion=True):
      logging.debug('Publishing msg {} on topic {} with qos {} and retain {}'.format(msg, topic, qos, retain))
      with self.publish_lock:
         response = self.mqtt_client.publish(topic, payload=msg, qos=qos, retain=retain)
         if not response.is_published() and check_for_completion:
            logging.debug('Waiting for publish to complete for topic: {}'.format(topic))
            response.wait_for_publish()
         logging.debug('Publish msg: {} to topic: {} complete'.format(str(msg), topic))

class PeriodicPublish:
   def __init__(self, pi_config, periodic_publish_handler):
      self.pi_config = pi_config
      self.periodic_publish_handler = periodic_publish_handler
      self.periodic_publish_thread = None

   def start(self):
      self.periodic_publish_thread = Thread(target=self.run, args=())
      self.periodic_publish_thread.start()

   def run(self):
      while True:
         self.periodic_publish_handler.handle()
         time.sleep(self.pi_config.get_mqtt_publish_rate_timer()) 




