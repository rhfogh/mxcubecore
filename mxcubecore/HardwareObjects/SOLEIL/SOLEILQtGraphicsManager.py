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
#   You should have received a copy of the GNU Lesser General Public License
#  along with MXCuBE. If not, see <http://www.gnu.org/licenses/>.

from mxcubecore.HardwareObjects.QtGraphicsManager import QtGraphicsManager
from mxcubecore.utils import qt_import

__credits__ = ["MXCuBE collaboration"]
__category__ = "Graphics"

class SOLEILQtGraphicsManager(QtGraphicsManager):

    def realign_beam(self):
        self.diffractometer_hwobj.beam_position_check()

    def anneal(self, time=1.):
        self.diffractometer_hwobj.anneal(time=time)

    def excenter(
        self,
        scan_length=0.40,
        step=45.,
        dozor=True,
        colspot=False,
        along=False,
        parent=None,
    ):
        self.diffractometer_hwobj.excenter(
            scan_length=scan_length,
            step=step,
            dozor=dozor,
            colspot=colspot,
            along=along,
            parent=parent,
        )

    def show_excenter_finished_dialog(self, scan_length=0.40, step=90.0, parent=None):
        if parent is not None:
            qt_import.QMessageBox.warning(
                            parent,
                            "Success!",
                            "Sample moved to optimal position\nplease, proceed with the data collection.",
                            qt_import.QMessageBox.Ok,
                        )
