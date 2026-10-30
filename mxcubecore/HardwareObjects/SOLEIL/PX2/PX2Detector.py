#
#  Project: MXCuBE
#  https://github.com/mxcube
#
#  This file is part of MXCuBE software.
#
#  MXCuBE is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  MXCuBE is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with MXCuBE. If not, see <http://www.gnu.org/licenses/>.

import logging
from mxcubecore import HardwareRepository as HWR
from mxcubecore.HardwareObjects.mockup.DetectorMockup import DetectorMockup

from resolution import resolution, resolution_mockup
from speaking_beam_center import speaking_beam_center as beam_center

class PX2Detector(DetectorMockup):

    def __init__(self, name):
        super().__init__(self, name)

    def init(self):
        super().init()

        self.bc = beam_center()
        self._beam_centre = self.bc.get_beam_center() #(xval, yval)

    def get_beam_position(self, distance=None, wavelength=None):
        self._beam_centre = self.bc.get_beam_center(wavelength=wavelength, ts=distance)
        return self._beam_centre

