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

"""Abstract class for a Sample View.
Defines methods to handle snapshots, animation and shapes.
"""

__copyright__ = """2019 by the MXCuBE collaboration """
__license__ = "LGPLv3+"

import abc
from ast import literal_eval
import logging
import math
from mxcubecore import HardwareRepository as HWR
import numpy as np
from typing import (
    Literal,
    Union,
)

from mxcubecore.BaseHardwareObjects import HardwareObject
from mxcubecore.HardwareObjects import sample_centring
from mxcubecore.model import queue_model_objects as qmo

ShapeState = Literal["HIDDEN", "SAVED", "TMP"]


class AbstractSampleView(HardwareObject):
    """AbstractSampleView Class"""

    __metaclass__ = abc.ABCMeta

    def __init__(self, name):
        super().__init__(name)
        self._focus = None
        self._zoom = None
        self._frontlight = None
        self._backlight = None
        self._shapes = {}
        self.centring_motors = {}
        self.current_centring_procedure = None

    def init(self):
        super().init()

        centring_motor_roles = self.get_property("centring_motors", [])
        if isinstance(centring_motor_roles, str):
            centring_motor_roles = literal_eval(centring_motor_roles)
        # need to set the motor names for the centring points
        qmo.CentredPosition.DIFFRACTOMETER_MOTOR_NAMES = centring_motor_roles

        centring_ref_position = self.get_property("centring_reference_position", {})
        if isinstance(centring_ref_position, str):
            centring_ref_position = literal_eval(centring_ref_position)

        motor_directions = self.get_property("motor_directions", {})
        if isinstance(motor_directions, str):
            motor_directions = literal_eval(motor_directions)
        diffr = HWR.beamline.diffractometer

        for role in centring_motor_roles:
            if role in diffr.motors_hwobj_dict:
                motor_obj = diffr.motors_hwobj_dict[role]
                ref_position = None
                if role in centring_ref_position:
                    ref_position = centring_ref_position[role]
                direction = motor_directions.get(role, 1)
                self.centring_motors[role] = sample_centring.CentringMotor(
                    motor_obj, reference_position=ref_position, direction=direction
                )

    @property
    def camera(self):
        return self.get_object_by_role("camera")

    @abc.abstractmethod
    def get_snapshot(
        self,
        overlay: bool | str = True,
        bw: bool = False,
        return_as_array: bool = False,
    ):
        """Get snappshot(s)
        Args:
            overlay(bool | str): Display shapes and other items on the snapshot
            bw(bool): return grayscale image
            return_as_array(bool): return as np array
        """

    @abc.abstractmethod
    def save_snapshot(
        self, filename, overlay: Union[bool, str] = True, bw: bool = False
    ):
        """Save a snapshot to file.
        Args:
            filename (str): The filename.
            overlay(bool | str): Display shapes and other items on the snapshot.
            bw(bool): Return grayscale image.
        """

    def save_scene_animation(self, filename, duration=1):
        """Take snapshots and create an animation.
        Args:
            filename (str): The filename.
            duration (int): Duration time [s].
        """

    @property
    def shapes(self):
        """Get shapes dict.
        Returns:
            (AbstractShapes): Shapes hardware object.
        """
        return self._shapes

    @property
    def zoom(self):
        """Get zoom object.
        Returns:
            (AbstractZoom): Zoom gardware object.
        """
        return self._zoom

    @property
    def frontlight(self):
        """Get Front light object
        Returns:
            (AbstractLight): Front light hardware object.
        """
        return self._frontlight

    @property
    def backlight(self):
        """Get Back light object.
        Returns:
            (AbstractLight): Back light hardware object.
        """
        return self._backlight

    @abc.abstractmethod
    def start_manual_centring(self, nb_click=3):
        """Starts manual centring procedure"""

    @abc.abstractmethod
    def start_auto_centring(self):
        """Start automatic centring procedure"""

    def cancel_centring(self):
        """Cancels current centring procedure"""
        if self.current_centring_procedure:
            try:
                self.current_centring_procedure.kill(block=True)
            except Exception:
                logging.getLogger("HWR").exception(
                    "Problem aborting the centring method"
                )
            self.current_centring_procedure = None
            self.emit("centringFailed")
            logging.getLogger("HWR").exception("Centring canceled")

    @abc.abstractmethod
    def add_shape(self, shape):
        """Add the shape <shape> to the dictionary of handled shapes.
        Args:
            shape(Shape): Shape to add
        """
        return

    @abc.abstractmethod
    def add_shape_from_mpos(
        self,
        mpos_list,
        screen_coord,
        _type,
        state: ShapeState = "SAVED",
        user_state: ShapeState = "SAVED",
    ):
        """Add a shape of type <t>, with motor positions from mpos_list and
        screen position screen_coord.
        Args:
            mpos_list (list[mpos_list]): List of motor positions
            screen_coord (tuple(x, y): Screen coordinate for shape
            _type (str): Type str for shape, P (Point), L (Line), G (Grid)
            user_state (ShapeState): State of the shape set by the user
        Returns:
            (Shape): Shape of type _type
        """
        return

    @abc.abstractmethod
    def delete_shape(self, sid):
        """Remove the shape with specified id from the list of handled shapes.
        Args:
            sid (str): The id of the shape to remove
        Returns:
            (Shape): The removed shape
        """
        return

    @abc.abstractmethod
    def select_shape(self, sid):
        """Select the shape <shape>.
        Args:
            sid (str): Id of the shape to select.
        """
        return

    @abc.abstractmethod
    def de_select_shape(self, sid):
        """De-select the shape with id <sid>.
        Args:
            sid (str): The id of the shape to de-select.
        """
        return

    @abc.abstractmethod
    def is_selected(self, sid):
        """Check if Shape with specified id is selected.
        Args:
            sid (int): Shape id.
        Returns:
            (Boolean) True if selected, False otherwise.
        """

    @abc.abstractmethod
    def get_selected_shapes(self):
        """Get all selected shapes.
        Returns:
           (list) List of the selected Shapes.
        """
        return

    @abc.abstractmethod
    def de_select_all(self):
        """De-select all shapes."""
        return

    @abc.abstractmethod
    def select_shape_with_cpos(self, cpos):
        """Selects shape with the assocaitaed centred position <cpos>
        Args:
            cpos (CentredPosition): Centred position
        """
        return

    @abc.abstractmethod
    def clear_all(self):
        """
        Clear the shapes, remove all contents.
        """
        return

    @abc.abstractmethod
    def get_shape(self, sid: str):
        """
        Get Shape with id <sid>.

        Args:
            sid (str): id of Shape to retrieve

        Returns:
            (Shape) All the shapes
        """
        return

    @abc.abstractmethod
    def get_grid(self):
        """Get the first of the selected grids, (the one that was selected
        first in a sequence of select operations).
        Returns:
            (dict): The first selected grid as a dictionary.
        """
        return

    @abc.abstractmethod
    def get_points(self):
        """Get all currently handled centred points.
        Returns:
            (list): All points currently handled as list.
        """
        return

    @abc.abstractmethod
    def get_lines(self):
        """Get all the currently handled lines.

        Returns:
            (list): All lines currently handled as list.
        """
        return

    @abc.abstractmethod
    def get_grids(self):
        """Get all currently handled grids.
        Returns:
            (list): All grids currently handled as list.
        """
        return

    @abc.abstractmethod
    def inc_used_for_collection(self, cpos):
        """Increase the counter that keepts on collect made on this shape,
        shape with associated CentredPosition cpos.
        Args:
            cpos (CentredPosition): CentredPosition of shape
        """

################  Concrete methods: #########################

    def get_positions(self) -> dict[str, float]:
        """Get motor positions for the centring motors.

        Returns:
            Centring motor positions as {role: position}
        """
        motors_dict = {}
        for key, val in self.centring_motors.items():
            motors_dict.update({key: val.motor.get_value()})
        return motors_dict

    def get_centred_point_from_coord(self, x, y, return_by_names=None):
        """Get the motor positions form x,y pixel coordinates"""

        beam_pos_x, beam_pos_y = HWR.beamline.beam.get_beam_position_on_screen()
        diffr = HWR.beamline.diffractometer
        pixels_per_mm_x, pixels_per_mm_y = diffr.get_pixels_per_mm()
        if not all([pixels_per_mm_x, pixels_per_mm_y]):
            return 0, 0

        # distance from the point to the beam
        dx = (x - beam_pos_x) / pixels_per_mm_x
        dy = (y - beam_pos_y) / pixels_per_mm_y

        motors_dict = self.get_positions()
        for key, val in motors_dict.items():
            motors_dict.update({key: self.centring_motors[key].direction * val})

        omega_angle = math.radians(motors_dict.get("omega", 0))
        rot_matrix = np.matrix(
            [
                [math.cos(omega_angle), -math.sin(omega_angle)],
                [math.sin(omega_angle), math.cos(omega_angle)],
            ]
        )
        inv_rot_matrix = np.array(rot_matrix.I)
        dsampx, dsampy = np.dot(np.array([0, dy]), inv_rot_matrix)

        chi_angle = math.radians(motors_dict.get("chi", 0))
        chi_rot = np.matrix(
            [
                [math.cos(chi_angle), -math.sin(chi_angle)],
                [math.sin(chi_angle), math.cos(chi_angle)],
            ]
        )
        sx, sy = np.dot(np.array([dsampx, dsampy]), np.array(chi_rot))

        sampx = -motors_dict.get("sampx") + sx
        sampy = motors_dict.get("sampy") + sy
        phiy = motors_dict.get("phiy") + dx

        return {
            "omega": motors_dict.get("omega"),
            "phiy": float(-phiy),
            "phiz": motors_dict.get("phiz"),
            "sampx": float(-sampx),
            "sampy": float(sampy),
        }

    def motor_positions_to_screen(
        self, positions_dict: dict[str, float]
    ) -> tuple[int, int]:
        """Get the x,y pixel value according to the calibration.

        Args:
            positions_dict: Dictionary {role: position}
        """
        if not positions_dict:
            raise RuntimeError("Unknown position")
        try:
            diffr = HWR.beamline.diffractometer
            p_x, p_y = diffr.get_pixels_per_mm()
            if None in (p_x, p_y):
                return 0, 0
            omega_angle = math.radians(-diffr.omega.get_value())
            sampx = positions_dict.get("sampx") - diffr.sampx.get_value()
            sampy = positions_dict.get("sampy") - diffr.sampy.get_value()
            phiy = -(positions_dict.get("phiy") - diffr.phiy.get_value())
            phiz = positions_dict.get("phiz") - diffr.phiz.get_value()

            rot_matrix = np.matrix(
                [
                    [math.cos(omega_angle), -math.sin(omega_angle)],
                    [math.sin(omega_angle), math.cos(omega_angle)],
                ]
            )
            inv_rot_matrix = np.array(rot_matrix.I)
            _, dy = np.dot(np.array([sampx, sampy]), inv_rot_matrix) * p_x

            chi_angle = math.radians(positions_dict.get("chi", 0))
            chi_rot = np.matrix(
                [
                    [math.cos(chi_angle), -math.sin(chi_angle)],
                    [math.sin(chi_angle), math.cos(chi_angle)],
                ]
            )
            sx, sy = np.dot(np.array([0, dy]), np.array(chi_rot))

            beam_position = HWR.beamline.beam.get_beam_position_on_screen()

            x = sx + (phiy * p_x) + beam_position[0]
            y = sy + (phiz * p_y) + beam_position[1]

        except AttributeError as err:
            raise NotImplementedError from err
        return x, y


    def move_to_beam(self, x: float, y: float):
        """Move the sample to the x,y coordinates.
        Args:
            x: Pixels on x axis
            y: Pixels on y axis
        """
        beam_pos_x, beam_pos_y = HWR.beamline.beam.get_beam_position_on_screen()
        diffr = HWR.beamline.diffractometer
        pixels_per_mm_x, pixels_per_mm_y = diffr.get_pixels_per_mm()
        if not all([pixels_per_mm_x, pixels_per_mm_y]):
            logging.getLogger("HWR").exception("Cannot move to beam")

        # here added the calculation for moving to the beam position
        dx = (x - beam_pos_x) / pixels_per_mm_x
        dy = (y - beam_pos_y) / pixels_per_mm_y

        diffr.wait_status_ready(5)
        motors_dict = self.get_positions()
        for key, val in motors_dict.items():
            motors_dict.update({key: self.centring_motors[key].direction * val})
        omega_angle = math.radians(motors_dict.get("omega", 0))

        rot_matrix = np.matrix(
            [
                [math.cos(omega_angle), -math.sin(omega_angle)],
                [math.sin(omega_angle), math.cos(omega_angle)],
            ]
        )
        inv_rot_matrix = np.array(rot_matrix.I)
        dsampx, dsampy = np.dot(np.array([0, dy]), inv_rot_matrix)

        chi_angle = math.radians(motors_dict.get("chi", 0))
        chi_rot = np.matrix(
            [
                [math.cos(chi_angle), -math.sin(chi_angle)],
                [math.sin(chi_angle), math.cos(chi_angle)],
            ]
        )

        sx, sy = np.dot(np.array([dsampx, dsampy]), np.array(chi_rot))

        sampx = -motors_dict.get("sampx") + sx
        sampy = motors_dict.get("sampy") + sy
        phiy = motors_dict.get("phiy") + dx

        self.centring_motors.get("sampx").set_value(-sampx)
        self.centring_motors.get("sampy").set_value(sampy)
        self.centring_motors.get("phiy").set_value(-phiy)
        diffr.save_centring_positions()