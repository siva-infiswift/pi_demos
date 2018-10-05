import yaml

class PiConfig:
   def __init__(self, pi_config_stanza):
      self.config = pi_config_stanza
   def get_name(self):
      return self.config['name']
   def get_print_debug_messages(self):
      return self.config['print_debug_messages']
   def get_main_loop_delay_timer(self):
      return self.config['timers']['main_loop_delay']
   def get_mqtt_publish_rate_timer(self):
      return self.config['timers']['mqtt_publish_rate']

class InternetConnectionCheckConfig:
   def __init__(self, internet_connection_check_stanza):
      self.config = internet_connection_check_stanza
   def get_url(self):
      return self.config['url']

class MqttBrokerConfig:
   def __init__(self, mqtt_broker_stanza):
      self.config = mqtt_broker_stanza
   def get_host(self):
      return self.config['host']
   def get_fallback(self):
      return self.config['fallback']
   def get_port(self):
      return self.config['port']

class MqttClientConfig:
   def __init__(self, mqtt_client_stanza):
      self.config = mqtt_client_stanza
   def get_client_id(self):
      return self.config['client_id']
   def get_username(self):
      return self.config['username']
   def get_password(self):
      return self.config['password']

class PublishTopicsConfig:
   def __init__(self, publish_topics_stanza):
      self.config = publish_topics_stanza
   def get_well_pump_speed(self):
      return self.config['well_pump_speed']
   def get_pressure_threshold(self):
      return self.config['pressure_threshold']
   def get_well_pump_production_volume(self):
      return self.config['well_pump_production_volume']
   def get_well_pump_pressure(self):
      return self.config['well_pump_pressure']
   def get_well_pump_temperature(self):
      return self.config['well_pump_temperature']
   def get_tank_restart_status(self):
      return self.config['tank_restart_status']
   def get_tank_volume_percent(self):
      return self.config['tank_volume_percent']
   def get_knockout_tank_status(self):
      return self.config['knockout_tank_status']
   def get_knockout_tank_oil_volume(self):
      return self.config['knockout_tank_oil_volume']
   def get_knockout_tank_water_volume(self):
      return self.config['knockout_tank_water_volume']

class SubscribeTopicsConfig:
   def __init__(self, subscribe_topics_stanza):
      self.config = subscribe_topics_stanza
   def get_well_pump_command(self):
      return self.config['well_pump_command']
   def get_ui(self):
      return self.config['ui']
   def get_analytics_engine_fill_rate(self):
      return self.config['analytics_engine_fill_rate']
   def get_knockout_tank_oil_volume(self):
      return self.config['knockout_tank_oil_volume']
   def get_shipping_tank_command(self):
      return self.config['shipping_tank_command']
   def get_analytics_engine_fill_rate(self):
      return self.config['analytics_engine_fill_rate']
   def get_well_pump_production_volume(self):
      return self.config['well_pump_production_volume']

class Reader:
   def __init__(self, config_file_name):
      self.config_file_name = config_file_name
      with open(config_file_name, 'r') as file:
          self.config = yaml.load(file)
      self.pi_config = PiConfig(self.config['pi'])
      self.internet_connection_check_config = InternetConnectionCheckConfig(self.config['internet_connection_check'])
      self.mqtt_broker_config = MqttBrokerConfig(self.config['mqtt_broker'])
      self.mqtt_client_config = MqttClientConfig(self.config['mqtt_client'])
      self.publish_topics_config = PublishTopicsConfig(self.config['topics']['publish'])
      self.subscribe_topics_config = SubscribeTopicsConfig(self.config['topics']['subscribe'])
   def get_pi_config(self):
      return self.pi_config
   def get_internet_connection_check_config(self):
      return self.internet_connection_check_config
   def get_mqtt_broker_config(self):
      return self.mqtt_broker_config
   def get_mqtt_client_config(self):
      return self.mqtt_client_config
   def get_publish_topics_config(self):
      return self.publish_topics_config
   def get_subscribe_topics_config(self):
      return self.subscribe_topics_config
