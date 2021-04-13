"""
[Name] ALBAImageTracking

[Description] Hardware object used to control image tracking
By default ADXV is used

Copy from EMBLImageTracking
"""
import os
import time
import logging
import socket
from HardwareRepository.BaseHardwareObjects import Device


class ALBAImageTracking(Device):

    def __init__(self, *args):
        Device.__init__(self, *args)
        self.logger = logging.getLogger("HWR.ALBAImageTracking")
        self.binary = None
        self.host = None
        self.port = None
        self.autofront = None
        self.start_adxv_cmd = None

    def init(self):
        self.binary = self.getProperty('executable')
        self.host = self.getProperty('host')
        self.port = self.getProperty('port', '8100')
        self.autofront = self.getProperty('autofront', True)

        if self.binary:
            _cmd = '{} -socket {}'.format(self.binary, self.port)
            if self.host:
                self.start_adxv_cmd = 'ssh {} "{}"'.format(self.host, _cmd)
            else:
                self.host = socket.gethostname()
                self.start_adxv_cmd = _cmd

    def load_image(self, image_name):
        self._load_image(str(image_name))

    def _load_image(self, image_file_name):
        """
        Send the image path associated to this spot to the adxv via socket.

        :param image_file_name: image file associated to the spot.
        :return: None
        """
        def send():
            self.logger.debug(
                "Opening socket connection for image: %s" % image_file_name)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.host, self.port))
            self.logger.debug("Sending image {}".format(image_file_name))
            if self.autofront:
                s.send("raise_window Image\n")
            s.send("load_image %s\n" % image_file_name)
        try:
            send()
        except Exception as e:
            os.system(self.start_adxv_cmd)
            time.sleep(2)
            send()

