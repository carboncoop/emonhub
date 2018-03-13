#!/usr/local/bin/python
#
# ----------------------------------------------------------------------------
# "THE BEER-WARE LICENSE" (Revision 42):
# <phk@FreeBSD.ORG> wrote this file.  As long as you retain this notice you
# can do whatever you want with this stuff. If we meet some day, and you think
# this stuff is worth it, you can buy me a beer in return.   Poul-Henning Kamp
# ----------------------------------------------------------------------------
#

import time
import math
import serial
import Cargo
from emonhub_interfacer import EmonHubInterfacer

"""class EmonhubKMPInterfacer

Polls a Kamstrup meter for data using the KMP protocol.

"""

kamstrup_382_var = {

    0x0001: "Energy-in-low-res",
    0x0002: "Energy-out-low-res",

    0x000d: "Ap",
    0x000e: "Am",

    0x041e: "U1",
    0x041f: "U2",
    0x0420: "U3",

    0x0434: "I1",
    0x0435: "I2",
    0x0436: "I3",

    0x0438: "P1",
    0x0439: "P2",
    0x043a: "P3",
    0x03e9: 'Meter-serialnumber',  # not user configurable
}

kamstrup_162J_var = {

    0x0001: "Energy-in-low-res",
    0x0002: "Energy-out-low-res",

    0x000d: "Ap",
    0x000e: "Am",

    0x041e: "U1",

    0x0434: "I1",

    0x0438: "P1",
    0x03e9: 'Meter-serialnumber',
}

kamstrup_362J_var = {

    0x0001: "Energy-in-low-res",
    0x0002: "Energy-out-low-res",

    0x000d: "Ap",
    0x000e: "Am",

    0x041e: "U1",
    0x041f: "U2",
    0x0420: "U3",

    0x0434: "I1",
    0x0435: "I2",
    0x0436: "I3",

    0x0438: "P1",
    0x0439: "P2",
    0x043a: "P3",
    0x03e9: 'Meter-serialnumber',
}

#######################################################################
# Units, provided by Erik Jensen

units = {
    0: '', 1: 'Wh', 2: 'kWh', 3: 'MWh', 4: 'GWh', 5: 'j', 6: 'kj', 7: 'Mj',
    8: 'Gj', 9: 'Cal', 10: 'kCal', 11: 'Mcal', 12: 'Gcal', 13: 'varh',
    14: 'kvarh', 15: 'Mvarh', 16: 'Gvarh', 17: 'VAh', 18: 'kVAh',
    19: 'MVAh', 20: 'GVAh', 21: 'kW', 22: 'kW', 23: 'MW', 24: 'GW',
    25: 'kvar', 26: 'kvar', 27: 'Mvar', 28: 'Gvar', 29: 'VA', 30: 'kVA',
    31: 'MVA', 32: 'GVA', 33: 'V', 34: 'A', 35: 'kV',36: 'kA', 37: 'C',
    38: 'K', 39: 'l', 40: 'm3', 41: 'l/h', 42: 'm3/h', 43: 'm3xC',
    44: 'ton', 45: 'ton/h', 46: 'h', 47: 'hh:mm:ss', 48: 'yy:mm:dd',
    49: 'yyyy:mm:dd', 50: 'mm:dd', 51: '', 52: 'bar', 53: 'RTC',
    54: 'ASCII', 55: 'm3 x 10', 56: 'ton x 10', 57: 'GJ x 10',
    58: 'minutes', 59: 'Bitfield', 60: 's', 61: 'ms', 62: 'days',
    63: 'RTC-Q', 64: 'Datetime'
}

#######################################################################
# Kamstrup uses the "true" CCITT CRC-16
#

def crc_1021(message):
        poly = 0x1021
        reg = 0x0000
        for byte in message:
                mask = 0x80
                while(mask > 0):
                        reg<<=1
                        if byte & mask:
                                reg |= 1
                        mask>>=1
                        if reg & 0x10000:
                                reg &= 0xffff
                                reg ^= poly
        return reg

#######################################################################
# Byte values which must be escaped before transmission
#

escapes = {
    0x06: True,
    0x0d: True,
    0x1b: True,
    0x40: True,
    0x80: True,
}

#######################################################################
# And here we go....
#
class kamstrup(object):

    def __init__(self, logger,serial_port = "/dev/ttyUSB0"):
        self.logger=logger
        self.logger.debug("\n\nStart\n")
        self.debug_id = None

        self.ser = serial.Serial(
            port = serial_port,
            baudrate = 9600,
            timeout = 1.0)

    def debug(self, dir, b):
        for i in b:
            if dir != self.debug_id:
                if self.debug_id != None:
                    self.logger.debug("\n")
                self.logger.debug(dir + "\t")
                self.debug_id = dir
            self.logger.debug(" %02x " % i)

    def debug_msg(self, msg):
        if self.debug_id != None:
            self.logger.debug("\n")
        self.debug_id = "Msg"
        self.logger.debug("Msg\t" + msg)

    def wr(self, b):
        b = bytearray(b)
        self.debug("Wr", b);
        self.ser.write(b)

    def rd(self):
        a = self.ser.read(1)
        if len(a) == 0:
            self.debug_msg("Rx Timeout")
            return None
        b = bytearray(a)[0]
        self.debug("Rd", bytearray((b,)));
        return b

    def send(self, pfx, msg):
        b = bytearray(msg)

        b.append(0)
        b.append(0)
        c = crc_1021(b)
        b[-2] = c >> 8
        b[-1] = c & 0xff

        c = bytearray()
        c.append(pfx)
        for i in b:
            if i in escapes:
                c.append(0x1b)
                c.append(i ^ 0xff)
            else:
                c.append(i)
        c.append(0x0d)
        self.wr(c)

    def recv(self):
        b = bytearray()
        while True:
            d = self.rd()
            if d == None:
                return None
            if d == 0x40:
                b = bytearray()
            b.append(d)
            if d == 0x0d:
                break
        c = bytearray()
        i = 1;
        while i < len(b) - 1:
            if b[i] == 0x1b:
                v = b[i + 1] ^ 0xff
                if v not in escapes:
                    self.debug_msg(
                        "Missing Escape %02x" % v)
                c.append(v)
                i += 2
            else:
                c.append(b[i])
                i += 1
        if crc_1021(c):
            self.debug_msg("CRC error")
        return c[:-2]

    def readvar(self, nbr):
        # I wouldn't be surprised if you can ask for more than
        # one variable at the time, given that the length is
        # encoded in the response.  Havn't tried.

        self.send(0x80, (0x3f, 0x10, 0x01, nbr >> 8, nbr & 0xff))

        b = self.recv()
        if b == None:
            return (None, None)

        if b[0] != 0x3f or b[1] != 0x10:
            return (None, None)

        if b[2] != nbr >> 8 or b[3] != nbr & 0xff:
            return (None, None)

        if b[4] in units:
            u = units[b[4]]
        else:
            u = None

        # Decode the mantissa
        x = 0
        for i in range(0,b[5]):
            x <<= 8
            x |= b[i + 7]

        # Decode the exponent
        i = b[6] & 0x3f
        if b[6] & 0x40:
            i = -i
        i = math.pow(10,i)
        if b[6] & 0x80:
            i = -i
        x *= i

        if False:
            # Debug print
            s = ""
            for i in b[:4]:
                s += " %02x" % i
            s += " |"
            for i in b[4:7]:
                s += " %02x" % i
            s += " |"
            for i in b[7:]:
                s += " %02x" % i

            print(s, "=", x, units[b[4]])

        return (x, u)

class EmonHubKMPInterfacer(EmonHubInterfacer):

    (WAIT_HEADER, IN_KEY, IN_VALUE, IN_CHECKSUM) = range(4)

    def __init__(self, name, com_port='', toextract='' , poll_interval=5, meter_type=''):
        """Initialize interfacer

        com_port (string): path to COM port
        poll_interval (int): time interval in seconds between meter reads
        meter_type (string): a string
        """

        # Initialization
        super(EmonHubKMPInterfacer, self).__init__(name)

        # Create kamstrup object
        self._kamstrup = kamstrup(self._log,serial_port=com_port)

        # Initialize RX buffer
        self._rx_buf = ''

        #KMP requirements
        meter_type_str=str(meter_type)
        if meter_type_str in "162J":
            self.meter_type_var=kamstrup_162J_var
        elif meter_type_str in "362J":
            self.meter_type_var=kamstrup_362J_var
        elif meter_type_str in "382":
            self.meter_type_var=kamstrup_382_var
        else:
            raise ValueError("ERROR: Meter type not defined!")

        # Minimum poll interval
        self.poll_interval = int(poll_interval)
        self.last_read = time.time()

        #Parser requirments
        self._extract = toextract
        #print "init system with to extract %s"%self._extract

    def close(self):
        """Close serial port"""
        pass

    def parse_package(self,data):
        """
        Convert package from kamstrup dictionary format to emonhub expected format

        """
        clean_data = "%s"%self._settings['nodeoffset']
        self._log.info("Extracting "+str(self._extract)+ " from "+str(data))
        for key in self._extract:
            if key in data:
                    #Emonhub doesn't like strings so we convert them to ints
                tempval = 0
                try:
                    tempval = float(data[key])
                except Exception,e:
                    tempval = data[key]
                if not isinstance(tempval,float):
                    if data[key] == "OFF":
                        data[key] = 0
                    else:
                        data[key] = 1

                clean_data = clean_data + " " + str(data[key])
        return clean_data


    def _read_serial(self):
        self._log.debug(" Starting KMP read")
        try:
            self._rx_buf={}
            for i in self.meter_type_var:
                x,u = self._kamstrup.readvar(i)
                mtv=self.meter_type_var[i]
                if x is None:
                    raise RuntimeError("Meter returning None for %s"%mtv)

                self._rx_buf[mtv]=x
                self._log.debug("Read %-25s %i %s" %(mtv,x,u))

        except Exception,e:
            self._log.error(e)
            self._rx_buf = ""


    def read(self):
        """Read data from serial port and process if complete line received.

        Return data as a list: [NodeID, val1, val2]

        """

        # Read serial RX
        now = time.time()
        if not (now - self.last_read) > self.poll_interval:
            self._log.debug(" Waiting for %s seconds "%str((now - self.last_read)))
            time.sleep(self.poll_interval - (now - self.last_read))
            return

        # Read from serial
        self._read_serial()
        # Update last read time
        self.last_read = now
        # If line incomplete, exit
        if self._rx_buf == None:
            return

        #Sample data looks like {'FW': '0307', 'SOC': '1000', 'Relay': 'OFF', 'PID': '0x203', 'H10': '6', 'BMV': '700', 'TTG': '-1', 'H12': '0', 'H18': '0', 'I': '0', 'H11': '0', 'Alarm': 'OFF', 'CE': '0', 'H17': '9', 'P': '0', 'AR': '0', 'V': '26719', 'H8': '29011', 'H9': '0', 'H2': '0', 'H3': '0', 'H1': '-1633', 'H6': '-5775', 'H7': '17453', 'H4': '0', 'H5': '0'}

        # Create a Payload object
        c = Cargo.new_cargo(rawdata = self._rx_buf)
        f = self.parse_package(self._rx_buf)
        f = f.split()
        self._log.info("Parsed data"+str(f))
        # Reset buffer
        self._rx_buf = ''

        if f:
            if int(self._settings['nodeoffset']):
                c.nodeid = int(self._settings['nodeoffset'])
                c.realdata = f[1:]
            else:
                self._log.error("nodeoffset needed in emonhub configuratio, make sure it exits ans is integer ")
                pass

        return c
