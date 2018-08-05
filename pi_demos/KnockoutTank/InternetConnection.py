import time
from threading import Thread
from urllib.request import urlopen
import PiConfigReader
import logging

class Manager:
   def __init__(self, internet_connection_check_config):
      self.internet_connection_check_config = internet_connection_check_config
      self.internet_connection_threshold = 1

   def start_internet_connection_thread(self):
      self.internet_connection_thread = Thread(target=self.manage_internet_connection, args=())
      self.internet_connection_thread.start()

   def _connect_to_internet(self):
      try:
         url = self.internet_connection_check_config.get_url()
         urlopen(url, timeout=5.0)
         self.internet_connected= True
      except Exception as error:
         self.internet_connected= False

   def manage_internet_connection(self):
      # No debouncing. Alert users for momentary internet drops.
      internet_connection_threadold = 1
      internet_connected = False
      internet_connected_stable = False
      internet_connected_count = 0
      internet_disconnected_count = 0
      while True:
         self._connect_to_internet()
         if self.internet_connected:
            if not internet_connected_stable:
               # We just connected the wifi
               internet_connected_count += 1
               if (internet_connected_count >= internet_connection_threadold):
                  # We've consistently had wifi
                  logging.info('Internet connection is stable')
                  internet_connected_stable = True
                  internet_disconnected_count = 0
         else:
            logging.info('Not connected to internet')
            if internet_connected_stable:
               # We've just lost the wifi connection
               # Debounce this change in state
               internet_disconnected_count += 1
               if (internet_disconnected_count >= self.internet_connection_threshold):
                  # We've consistently lost wifi
                  logging.info('Internet connection is no longer stable')
                  internet_connected_stable = False
                  internet_connected_count = 0
         time.sleep(1.0)

