#  Project: MXCuBE
#  https://github.com/mxcube.
#
#  This file is part of MXCuBE software.
#
#  MXCuBE is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  MXCuBE is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with MXCuBE.  If not, see <http://www.gnu.org/licenses/>.

"""
[Name] ALBAFlux

[Description]
HwObj used to get the flux

[Signals]
- None
"""

from __future__ import print_function

import logging

from HardwareRepository.BaseHardwareObjects import Device

__credits__ = ["ALBA Synchrotron"]
__version__ = "2.3"
__category__ = "General"


class ALBAFlux(Device):

    def __init__(self, *args):
        self.logger = logging.getLogger("HWR.ALBAFlux")
        Device.__init__(self, *args)
        self.current_chn = None
        self.transmission_chn = None
        self.last_flux_chn = None
        self.last_flux_norm_chn = None

    def init(self):
        self.logger.debug("Initializing {0}".format(self.__class__.__name__))
        self.current_chn = self.getChannelObject("current")
        self.transmission_chn = self.getChannelObject("transmission")
        self.last_flux_chn = self.getChannelObject("last_flux")
        self.last_flux_norm_chn = self.getChannelObject("last_flux_norm")

    def get_flux(self):
        last_flux = self.last_flux_chn.getValue()
        try:
            if last_flux > 1e7:
                return self.get_last_current() * self.get_transmission()
        except Exception as e:
            self.logger.debug("Cannot read flux\n%s" % str(e))

        default_flux = 6e11 * self.get_transmission()
        self.logger.debug("Flux value abnormally low, returning default value (%s)" %
                          default_flux)
        return default_flux

    def get_transmission(self):
        """ returns transmission between 0 and 1"""
        return self.transmission_chn.getValue() / 100.

    def get_last_current(self):
        last_flux_norm = self.last_flux_norm_chn.getValue()
        current = self.current_chn.getValue()
        last_current = (last_flux_norm / 250.) * current
        return last_current


def test_hwo(hwo):
    print("Flux = %.4e" % hwo.get_flux())
    print("Last current = %.4e" % hwo.get_last_current())
    print("Transmission = %.2f" % hwo.get_transmission())
