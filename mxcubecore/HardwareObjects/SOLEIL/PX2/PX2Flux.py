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
from mxcubecore.HardwareObjects.mockup.FluxMockup import FluxMockup

try:
    from flux import flux, flux_mockup
except ModuleNotFoundError:
    from experimental_methods import flux, flux_mockup


class PX2Flux(FluxMockup):
    def __init__(self, name):
        FluxMockup.__init__(self, name)
        self.flux = flux()
        self.flux_mockup = flux_mockup()
        # logging.info('flux %.2e'% self.flux.get_flux())

    def get_value(self):
        try:
            f = self.flux.get_flux()
        except:
            f = self.flux_mockup.get_flux()
        return f
