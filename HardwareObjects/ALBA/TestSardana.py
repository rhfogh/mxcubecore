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
[Name] ALBATestSardana

[Description]
HwObj used to test the Sardana implementation

[Signals]
- valuesChanged
"""
from __future__ import print_function

import logging

from HardwareRepository.BaseHardwareObjects import Equipment

__credits__ = ["ALBA Synchrotron"]
__version__ = "2.3"
__category__ = "Test"


class TestSardana(Equipment):

    def __init__(self, *args):
        self.logger = logging.getLogger("HWR.TestSardana")
        Equipment.__init__(self, *args)
        self.channel = None
        self.command = None
        self.macro = None

    def init(self):
        self.logger.debug("Initializing {0}".format(self.__class__.__name__))
        self.channel = self.getChannelObject("position")
        if self.channel is not None:
            self.channel.connectSignal(
                'update', self.update_values)
        self.command = self.getCommandObject("st")
        self.macro = self.getCommandObject("wm")

    def update_values(self, value):
        # We re-emit the channel value received in the event to any Qt client
        self.emit('valueChanged', value)
        self.logger.debug("valueChanged emitted ({})".format(value))
