from Resolution import Resolution
import logging
import math

class ALBAResolution(Resolution):
    def __init__(self, *args, **kwargs):
        Resolution.__init__(self, name="ALBAResolution")
        self.logger = logging.getLogger("HWR.ALBAResolution")

        self._chnBeamX = None
        self._chnBeamY = None

    def init(self):
        self.logger.debug("Initializing {0}".format(self.__class__.__name__))
        Resolution.init(self)
        self._chnBeamX = self.detector.getChannelObject('beamx')
        self._chnBeamY = self.detector.getChannelObject('beamy')
    
    def get_beam_centre(self, dtox=None):
        return self._chnBeamX.getValue(), self._chnBeamY.getValue()

    def getLimits(self):
        return Resolution.getLimits()
