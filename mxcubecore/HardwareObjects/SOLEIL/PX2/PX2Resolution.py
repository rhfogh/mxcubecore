import logging
from mxcubecore.HardwareObjects.abstract.AbstractResolution import AbstractResolution
from mxcubecore import HardwareRepository as HWR

try:
    from resolution import resolution, resolution_mockup
    from speaking_beam_center import speaking_beam_center as beam_center
except ModuleNotFoundError:
    from experimental_methods import (
        resolution,
        resolution_mockup,
        speaking_beam_center as beam_center,
    )

class PX2Resolution(AbstractResolution):
    def __init__(self, name):
        super(PX2Resolution, self).__init__(name)
        self.resolution_motor = resolution()
        #self.resolution_motor = resolution_mockup()
        self.beam_center = beam_center()

    def connect_notify(self, signal):
        if signal == "stateChanged":
            self.update_state(self.get_state())

    def get_value(self):
        self._nominal_value = self.resolution_motor.get_resolution()
        #logging.info(f"resolution returning {self._nominal_value:.4f}")
        return self._nominal_value

    def _set_value(self, value):
        self.resolution_motor.set_resolution(value)

    def get_beam_centre(self, dtox=None):
        return self.beam_center.get_beam_center()

    def get_limits(self):
        return self.resolution_motor.get_resolution_limits()

    def stop(self):
        self.resolution_motor.stop()

    def is_ready(self):
        return True

    def update_distance(self, value=None):
        """Update the resolution when distance changed.
        Args:
            value (float): Detector distance [mm].
        """
        self._nominal_value = self.resolution_motor.get_resolution()
        self.emit("valueChanged", (self._nominal_value,))

    def resolution_to_distance(self, resolution=None, wavelength=None):
        if resolution is None:
            resolution = self.get_value()
        #logging.info(f"self._hwr_detector.get_radius() {self._hwr_detector.get_radius()}")
        distance = self.resolution_motor.get_distance_from_resolution(resolution, wavelength=wavelength)
        return distance