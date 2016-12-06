"""class EmonHubSMXGenInterfacer

   Based on EmonHubMqttGenInterfacer.

   Connects to MQTT brokers on SMX, maps OBIS codes to emoncms format, then pushes to dispatcher sub channels for further processing.
   Will only push mapped OBIS codes.
"""
import time
import paho.mqtt.client as mqtt
from pydispatch import dispatcher
from emonhub_interfacer import EmonHubInterfacer
import Cargo

obis_code_map={
    '':'',
}

class EmonHubSMXInterfacer(EmonHubInterfacer):

    def __init__(self, name, mqtt_user=" ", mqtt_passwd=" ", mqtt_host="127.0.0.1", mqtt_port=1883):
        # Initialization
        super(EmonHubSMXInterfacer, self).__init__(name)

        self._log.info(str(name)+" Init mqtt_host="+str(mqtt_host)+" mqtt_port="+str(mqtt_port)+ " mqtt_user="+str(mqtt_user))
        self._name = name
        self._host = mqtt_host
        self._port = mqtt_port
        self._user = mqtt_user
        self._passwd = mqtt_passwd
        self._connected = False

        self._settings = {
            'subchannels':['ch1'],
            'pubchannels':['ch2'],

            'smx_basetopic': 'smx/',

            'obis_codes_map':'',

        };

        self._mqttc = mqtt.Client()
        self._mqttc.on_connect = self.on_connect
        self._mqttc.on_disconnect = self.on_disconnect
        self._mqttc.on_message = self.on_message
        self._mqttc.on_subscribe = self.on_subscribe


    def action(self):
        if not self._connected:
            self._log.info("Connecting to MQTT Server")
            try:
                self._mqttc.username_pw_set(self._user, self._passwd)
                self._mqttc.connect(self._host, self._port, 60)
            except:
                self._log.info("Could not connect...")
                time.sleep(1.0)
        self._mqttc.loop(0)

    def on_connect(self, client, userdata, flags, rc):

        connack_string = {0:'Connection successful',
                          1:'Connection refused - incorrect protocol version',
                          2:'Connection refused - invalid client identifier',
                          3:'Connection refused - server unavailable',
                          4:'Connection refused - bad username or password',
                          5:'Connection refused - not authorised'}

        if rc:
            self._log.warning(connack_string[rc])
        else:
            self._log.info("connection status: "+connack_string[rc])
            self._connected = True
            # Subscribe to SMX MQTT broker - filter relevant OBIS codes
            self._mqttc.subscribe(str(self._settings["smx_basetopic"])+"/#")

        self._log.debug("CONACK => Return code: "+str(rc))

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            self._log.info("Unexpected disconnection")
            self._connected = False

    def on_subscribe(self, mqttc, obj, mid, granted_qos):
        self._log.info("on_subscribe")

    def on_message(self, client, userdata, msg):
        topic_parts = msg.topic.split("/")

        if topic_parts[0] == self._settings["smx_basetopic"][:-1]:
            if topic_parts[1] == "tx":
                if topic_parts[3] == "values":
                    nodeid = int(topic_parts[2])

                    payload = msg.payload
                    realdata = payload.split(",")
                    self._log.debug("Nodeid: "+str(nodeid)+" values: "+msg.payload)

                    rxc = Cargo.new_cargo(realdata=realdata)
                    rxc.nodeid = nodeid

                    if rxc:
                        for channel in self._settings["pubchannels"]:
                            dispatcher.send(channel, cargo=rxc)
                            self._log.debug(str(rxc.uri) + " Sent to channel' : " + str(channel))

    def receiver(self, cargo):
        if self._connected:


            # Node id or nodename if given
            nodestr = str(cargo.nodeid)
            if cargo.nodename!=False: nodestr = str(cargo.nodename)

            varid = 1
            for value in cargo.realdata:
                # Variable id or variable name if given
                varstr = str(varid)
                if (varid-1)<len(cargo.names):
                    varstr = str(cargo.names[varid-1])
                # Construct topic
                topic = self._settings["nodevar_format_basetopic"]+nodestr+"/"+varstr
                payload = str(value)

                self._log.info("Publishing: "+topic+" "+payload)
                result =self._mqttc.publish(topic, payload=payload, qos=2, retain=False)

                if result[0]==4:
                    self._log.info("Publishing error? returned 4")

                varid += 1

    def set(self, **kwargs):
        for key,setting in self._settings.iteritems():
            if key in kwargs.keys():
                # replace default
                self._settings[key] = kwargs[key]

        # Subscribe to internal channels
        for channel in self._settings["subchannels"]:
            dispatcher.connect(self.receiver, channel)
            self._log.debug(self._name+" Subscribed to channel' : " + str(channel))
