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

import logging
import copy

from mxcubecore.HardwareObjects.QtGraphicsManager import QtGraphicsManager
from mxcubecore.HardwareObjects import QtGraphicsLib as GraphicsLib
from mxcubecore.utils import qt_import

from goniometer import goniometer
__credits__ = ["MXCuBE collaboration"]
__category__ = "Graphics"

class SOLEILQtGraphicsManager(QtGraphicsManager):

    def __init__(self, name):
        super().__init__(name)

        self.log = logging.getLogger("HWR")
        self.goniometer = goniometer()
        self.omega_reference_par = {"motor_name": "phiz", "position": 0.0, "camera_axis": "x", "direction": 1}
        self.beam_position = [680., 512.]

    def accept_centring(self):
        self.centring_done()
        super().accept_centring()

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

    def save_click(self, x, y):
        return self.diffractometer_hwobj.save_click(x, y)

    def translate_from_md_to_mxcube(self, position):
        return self.diffractometer_hwobj.translate_from_md_to_mxcube(position)

    def diffractometer_pixels_per_mm_changed(self, pixels_per_mm, optional=None):
        """Updates graphics scale when zoom changed

        :param pixels_per_mm: two floats for scaling
        :type pixels_per_mm: list with two floats
        """
        self.log.info(f"diffractometer_pixels_per_mm_changed {pixels_per_mm} {optional}")

        if type(pixels_per_mm) in (list, tuple):
            if pixels_per_mm != self.pixels_per_mm:
                self.pixels_per_mm = pixels_per_mm
                for item in self.graphics_view.graphics_scene.items():
                    if isinstance(item, GraphicsLib.GraphicsItem):
                        item.set_pixels_per_mm(self.pixels_per_mm)
                self.graphics_view.graphics_scene.update()

        self.pixels_per_mm_x, self.pixels_per_mm_y = self.pixels_per_mm

        self.diffractometer_state_changed()

    def get_centred_point_from_coord(self, x, y, return_by_names=None, save=True):
        """
        Descript. :
        """

        self.log.info("get_centered_point_from_coord: x, y: %s %s" % (x, y))
        self.log.info(
            "get_centered_point_from_coord: self.pixels_per_mm_x, self.pixels_per_mm_ y: %s %s"
            % (self.pixels_per_mm_x, self.pixels_per_mm_y)
        )
        self.log.info(
            "get_centered_point_from_coord: self.beam_position: %s"
            % str(self.beam_position)
        )

        if save:
            self.save_click(x, y)

        current_position = self.goniometer.get_aligned_position()

        omega_reference = self.omega_reference_par["position"]

        alignmentz_shift = current_position["AlignmentZ"] - omega_reference

        vertical_shift = y - self.beam_position[1]

        horizontal_shift = x - self.beam_position[0]

        #calibration = self.camera.get_calibration()
        #self.pixels_per_mm_y, self.pixels_per_mm_x = 1.0 / calibration

        vertical_shift /= self.pixels_per_mm_y
        horizontal_shift /= self.pixels_per_mm_x

        self.log.info(
            "get_centered_point_from_coord: original_vertical_shift: %s "
            % vertical_shift
        )

        # vertical_shift += alignmentz_shift
        horizontal_shift -= alignmentz_shift

        self.log.info(f"p: {current_position}")
        self.log.info(f"h: {horizontal_shift}")
        self.log.info(f"v: {vertical_shift}")
        self.log.info(f"omega: {current_position['Omega']}")

        self.log.info(
            f"executing centringx_shift, centringy_shift = self.goniometer.get_x_and_y(0, {horizontal_shift}, {current_position['Omega']})"
        )

        centringx_shift, centringy_shift = self.goniometer.get_x_and_y(
            0, horizontal_shift, current_position["Omega"]
        )

        centered_point = copy.deepcopy(current_position)

        self.log.info(f"cx_shift: {centringx_shift}")
        self.log.info(f"cy_shift: {centringy_shift}")

        centered_point["AlignmentZ"] -= alignmentz_shift
        centered_point["AlignmentY"] += vertical_shift

        centered_point["CentringX"] -= centringx_shift
        centered_point["CentringY"] += centringy_shift

        self.log.info("get_centered_point_from_coord, centered_point: %s " % str(centered_point))
        # centered_point['Omega'] += 90.
        pos = self.translate_from_md_to_mxcube(centered_point)

        self.log.info("get_centered_point_from_coord, translated to pos: %s " % str(pos))

        return pos
