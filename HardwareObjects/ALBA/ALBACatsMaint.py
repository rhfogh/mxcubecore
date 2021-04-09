#
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
[Name] ALBACatsMaint

[Description]
HwObj used to operate the CATS sample changer via Tango in maintenance mode

[Signals]
- None
"""

from __future__ import print_function
import logging
from sample_changer.CatsMaint import CatsMaint

__credits__ = ["ALBA Synchrotron"]
__version__ = "2.3"
__category__ = "General"


class ALBACatsMaint(CatsMaint):

    def __init__(self, *args):
        CatsMaint.__init__(self, *args)
        self.logger = logging.getLogger("HWR.ALBACatsMaint")
        self.chan_shifts = None
        self.chan_at_home = None
        self.cmd_super_abort = None

    def init(self):
        self.logger.debug("Initializing {0}".format(self.__class__.__name__))
        CatsMaint.init(self)

        # channel to ask diffractometer for mounting position
        self.chan_shifts = self.getChannelObject("shifts")
        self.chan_at_home = self.getChannelObject("_chnAtHome")
        self.cmd_super_abort = self.getCommandObject("super_abort")

        # To get acces to recovery functions
        self.Cats90 = self.getObjectByRole("cats90")

    def _doAbort(self):
        if self.cmd_super_abort is not None:
            self.cmd_super_abort()
        self._cmdAbort()

    def _doResetMemory(self):
        """
        Reset CATS memory.
        """
        # Check do_PRO6_RAH first
        if self.chan_at_home.getValue() is True:
            CatsMaint._doResetMemory(self)

    def _check_unknown_sample_presence(self):
        self.Cats90._check_unknown_sample_presence()

    def _check_incoherent_sample_info(self):
        """
          Check for sample info in CATS but no physically mounted sample
           (Fix failed PUT)
          Returns False in case of incoherence, True if all is ok
        """
        self.Cats90._check_incoherent_sample_info()

    def _doRecoverFailure(self):
        """
          Failed get
        """
        self.Cats90._doRecoverFailure()

    def _doReset(self):
        """
           Reset CATS system after failed put
           Deletes sample info on diff, but should retain info of samples on tools, eg when doing picks
           TODO: tool2 commands are not working, eg SampleNumberInTool2
        """
        self.Cats90._doReset()

    def _get_shifts(self):
        """
        Get the mounting position from the Diffractometer DS.

        @return: 3-tuple
        """
        if self.chan_shifts is not None:
            shifts = self.chan_shifts.getValue()
        else:
            shifts = None
        return shifts


def test_hwo(hwo):
    print(hwo._get_shifts())
