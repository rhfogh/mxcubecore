# encoding: utf-8
#
#  Project name: MXCuBE
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
#  You should have received a copy of the GNU General Lesser Public License
#  along with MXCuBE. If not, see <http://www.gnu.org/licenses/>.

from mxcubecore.HardwareObjects.ExporterNState import ExporterNState

class PX2Phase(ExporterNState):

    def _update_value(self, value):
        value = self.value_to_enum(value)
        super().update_value(value)
        self.emit("diffractometerPhaseChanged", value)

    def _update_state(self, state=None):
        if not state:
            state = self.get_state()
        else:
            state = self._str2state(state)

        super().update_state(self.STATES.READY)
        self.emit("diffractometerPhaseStateChanged", self.STATES.READY)