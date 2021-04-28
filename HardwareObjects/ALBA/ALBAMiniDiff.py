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
[Name]
ALBAMiniDiff

[Description]
Specific HwObj for M2D2 diffractometer @ ALBA

[Emitted signals]
- pixelsPerMmChanged
- kappaMotorMoved
- phiMotorMoved
- stateChanged
- zoomMotorPredefinedPositionChanged
- minidiffStateChanged
- minidiffPhaseChanged
"""

from __future__ import print_function

import logging
import time
import gevent
import math

import queue_model_objects_v1 as queue_model_objects

from GenericDiffractometer import GenericDiffractometer, DiffractometerState
from taurus.core.tango.enums import DevState

__credits__ = ["ALBA Synchrotron"]
__version__ = "2.3"
__category__ = "General"


class ALBAMiniDiff(GenericDiffractometer):
    """
    Specific diffractometer HwObj for XALOC beamline.
    """

    def __init__(self, *args):
        GenericDiffractometer.__init__(self, *args)
        self.logger = logging.getLogger("HWR.ALBAMiniDiff")
        self.calibration_hwobj = None
        self.centring_hwobj = None
        self.super_hwobj = None
        self.chan_state = None
        self.phi_motor_hwobj = None
        self.phiz_motor_hwobj = None
        self.phiy_motor_hwobj = None
        self.zoom_motor_hwobj = None
        self.focus_motor_hwobj = None
        self.sample_x_motor_hwobj = None
        self.sample_y_motor_hwobj = None
        self.kappa_motor_hwobj = None
        self.kappa_phi_motor_hwobj = None

        self.omegaz_reference = None

    def init(self):
        self.logger.debug("Initializing {0}".format(self.__class__.__name__))
        self.calibration_hwobj = self.getObjectByRole("calibration")

        self.centring_hwobj = self.getObjectByRole('centring')
        self.super_hwobj = self.getObjectByRole('beamline-supervisor')

        if self.centring_hwobj is None:
            self.logger.debug('ALBAMinidiff: Centring math is not defined')

        if self.super_hwobj is not None:
            self.connect(
                self.super_hwobj,
                'stateChanged',
                self.supervisor_state_changed)
            self.connect(
                self.super_hwobj,
                'phaseChanged',
                self.supervisor_phase_changed)

        self.chan_state = self.getChannelObject("State")
        self.connect(self.chan_state, "update", self.state_changed)

        self.phi_motor_hwobj = self.getObjectByRole('phi')
        self.phiz_motor_hwobj = self.getObjectByRole('phiz')
        self.phiy_motor_hwobj = self.getObjectByRole('phiy')
        self.zoom_motor_hwobj = self.getObjectByRole('zoom')
        self.focus_motor_hwobj = self.getObjectByRole('focus')
        self.sample_x_motor_hwobj = self.getObjectByRole('sampx')
        self.sample_y_motor_hwobj = self.getObjectByRole('sampy')
        self.kappa_motor_hwobj = self.getObjectByRole('kappa')
        self.kappa_phi_motor_hwobj = self.getObjectByRole('kappa_phi')

        if self.phi_motor_hwobj is not None:
            self.connect(
                self.phi_motor_hwobj,
                'stateChanged',
                self.phi_motor_state_changed)
            self.connect(self.phi_motor_hwobj, "positionChanged", self.phi_motor_moved)
            self.current_motor_positions["phi"] = self.phi_motor_hwobj.getPosition()
        else:
            self.logger.error('Phi motor is not defined')

        if self.phiz_motor_hwobj is not None:
            self.connect(
                self.phiz_motor_hwobj,
                'stateChanged',
                self.phiz_motor_state_changed)
            self.connect(
                self.phiz_motor_hwobj,
                'positionChanged',
                self.phiz_motor_moved)
            self.current_motor_positions["phiz"] = self.phiz_motor_hwobj.getPosition()
        else:
            self.logger.error('Phiz motor is not defined')

        if self.phiy_motor_hwobj is not None:
            self.connect(
                self.phiy_motor_hwobj,
                'stateChanged',
                self.phiy_motor_state_changed)
            self.connect(
                self.phiy_motor_hwobj,
                'positionChanged',
                self.phiy_motor_moved)
            self.current_motor_positions["phiy"] = self.phiy_motor_hwobj.getPosition()
        else:
            self.logger.error('Phiy motor is not defined')

        if self.zoom_motor_hwobj is not None:
            self.connect(
                self.zoom_motor_hwobj,
                'positionChanged',
                self.zoom_position_changed)
            self.connect(
                self.zoom_motor_hwobj,
                'predefinedPositionChanged',
                self.zoom_motor_predefined_position_changed)
            self.connect(
                self.zoom_motor_hwobj,
                'stateChanged',
                self.zoom_motor_state_changed)
        else:
            self.logger.error('Zoom motor is not defined')

        if self.sample_x_motor_hwobj is not None:
            self.connect(
                self.sample_x_motor_hwobj,
                'stateChanged',
                self.sampleX_motor_state_changed)
            self.connect(
                self.sample_x_motor_hwobj,
                'positionChanged',
                self.sampleX_motor_moved)
            self.current_motor_positions["sampx"] = self.sample_x_motor_hwobj.getPosition()
        else:
            self.logger.error('Sampx motor is not defined')

        if self.sample_y_motor_hwobj is not None:
            self.connect(
                self.sample_y_motor_hwobj,
                'stateChanged',
                self.sampleY_motor_state_changed)
            self.connect(
                self.sample_y_motor_hwobj,
                'positionChanged',
                self.sampleY_motor_moved)
            self.current_motor_positions["sampy"] = self.sample_y_motor_hwobj.getPosition()
        else:
            self.logger.error('Sampx motor is not defined')

        if self.focus_motor_hwobj is not None:
            self.connect(
                self.focus_motor_hwobj,
                'positionChanged',
                self.focus_motor_moved)

        if self.kappa_motor_hwobj is not None:
            self.connect(
                self.kappa_motor_hwobj,
                'stateChanged',
                self.kappa_motor_state_changed)
            self.connect(
                self.kappa_motor_hwobj,
                "positionChanged",
                self.kappa_motor_moved)
            self.current_motor_positions["kappa"] = self.kappa_motor_hwobj.getPosition()
        else:
            self.logger.error('Kappa motor is not defined')

        if self.kappa_phi_motor_hwobj is not None:
            self.connect(
                self.kappa_phi_motor_hwobj,
                'stateChanged',
                self.kappa_phi_motor_state_changed)
            self.connect(
                self.kappa_phi_motor_hwobj,
                "positionChanged",
                self.kappa_phi_motor_moved)
            self.current_motor_positions["kappa_phi"] = self.kappa_phi_motor_hwobj.getPosition()
        else:
            self.logger.error('Kappa-Phi motor is not defined')

        GenericDiffractometer.init(self)

        # overwrite default centring motors configuration from GenericDiffractometer
        # when using sample_centrinig. Fix phiz position to a reference value.
        if self.use_sample_centring:

            if self.getProperty("omegaReference"):
                self.omegaz_reference = eval(self.getProperty("omegaReference"))

                try:
                    self.logger.debug(
                        "Setting omegaz reference position to {0}".format(
                            self.omegaz_reference['position']))
                    self.centring_phiz.reference_position = \
                        self.omegaz_reference['position']
                except BaseException:
                    self.logger.warning(
                        "Invalid value for omegaz reference!")
                    raise

        queue_model_objects.CentredPosition.\
            set_diffractometer_motor_names(
                "phi", "phiy", "phiz", "sampx", "sampy", "kappa", "kappa_phi")

        # TODO: Explicit update would not be necessary, but it is.
        # Added to make sure pixels_per_mm is initialised
        self.update_pixels_per_mm()

    def state_changed(self, state):
        """
        Overwrites method to map Tango ON state to Diffractometer State Ready.

        @state: Taurus state but string for Ready state
        """
        if state == DevState.ON:
            state = DiffractometerState.tostring(DiffractometerState.Ready)

        if state != self.current_state:
            self.logger.debug("State changed %s (was: %s)" %
                (str(state), self.current_state))
            self.current_state = state
            self.emit("minidiffStateChanged", (self.current_state))

    def getCalibrationData(self, offset=None):
        """
        Get pixel size for OAV system in mm

        @offset: Unused
        @return: 2-tuple float
        """
        #self.logger.debug("Getting calibration data")
        # This MUST be equivalent:
        # calibration uses the zoom percentage
        #calibx, caliby = self.calibration_hwobj.get_calibration()
        # zoom motor use zoom index
        #calibx, caliby = self.calibration_hwobj.get_calibration()
        calibx, caliby = self.zoom_motor_hwobj.get_calibration()
        return 1000.0 / caliby, 1000.0 / caliby

    def get_pixels_per_mm(self):
        """
        Returns the pixel/mm for x and y. Overrides GenericDiffractometer method.
        """
        px_x, px_y = self.getCalibrationData()
        return px_x, px_y

    def update_pixels_per_mm(self, *args):
        """
        Emit signal with current pixel/mm values.
        """
        self.pixels_per_mm_x, self.pixels_per_mm_y = self.getCalibrationData()
        self.emit('pixelsPerMmChanged', ((self.pixels_per_mm_x, self.pixels_per_mm_y), ))

    # Overwrite from generic diffractometer
    def update_zoom_calibration(self):
        """
        """
        self.update_pixels_per_mm()

    def get_centred_point_from_coord(self, x, y, return_by_names=None):
        """
        Returns a dictionary with motors name and positions centred.
        It is expected in start_move_to_beam and move_to_beam methods in
        GenericDiffractometer HwObj, 
        Also needed for the calculation of the motor positions after definition of the mesh grid 
            (Qt4_GraphicsManager, update_grid_motor_positions)

        point x,y is relative to the lower left corner on the camera, this functions returns the motor positions for that point,
        where the motors that are changed are phiy and phiz. 
        
        @return: dict
        """
        self.logger.info('get_centred_point_from_coord x %s and y %s and return_by_names %s' % ( x, y, return_by_names ) )
        self.logger.info('get_centred_point_from_coord pixels_per_mm_x %s and pixels_per_mm_y %s' % ( self.pixels_per_mm_x, self.pixels_per_mm_y ) )
        self.logger.info('get_centred_point_from_coord beam_position[0] %s and beam_position[1] %s' % 
                                          ( self.beam_position[0], self.beam_position[1] ) 
                                      )

        self.update_zoom_calibration()
        
        loc_centred_point = {}
        loc_centred_point['phi'] = self.phi_motor_hwobj.getPosition()
        loc_centred_point['kappa'] = self.kappa_motor_hwobj.getPosition()
        loc_centred_point['kappa_phi'] = self.kappa_phi_motor_hwobj.getPosition()
        loc_centred_point['phiy'] = self.phiy_motor_hwobj.getPosition() - ( float( x - self.beam_position[0] ) / self.pixels_per_mm_x )

        # Overwrite phiz, which should remain in the actual position, hopefully the center of rotation
        omegaz_diff = 0
        if self.omegaz_reference != None: 
            loc_centred_point['phiz'] = self.omegaz_reference['position']
            omegaz_diff = self.phiz_motor_hwobj.getPosition() - self.omegaz_reference['position'] 
        else: 
            loc_centred_point['phiz'] = self.phiz_motor_hwobj.getPosition() 

        # Calculate the positions of sampx and sampy that correspond to the camera x,y coordinates
        vertdist = omegaz_diff + float( y - self.beam_position[1] ) / self.pixels_per_mm_y 
        sampxpos = self.sample_x_motor_hwobj.getPosition()
        sampypos = self.sample_y_motor_hwobj.getPosition()
        phi_angle = math.radians(self.centring_phi.direction * \
                    self.centring_phi.getPosition())

        dy = math.cos(phi_angle) * vertdist
        dx = math.sin(phi_angle) * vertdist

        loc_centred_point['sampx'] = sampxpos + dx
        loc_centred_point['sampy'] = sampypos + dy

#        if return_by_names:
#            loc_centred_point = self.convert_from_obj_to_name(loc_centred_point)

        self.logger.info('get_centred_point_from_coord loc_centred_point %s ' % ( loc_centred_point ) )
        
        return loc_centred_point

    def getBeamInfo(self, update_beam_callback):
        """
        Update beam info (position and shape) ans execute callback.

        @update_beam_callback: callback method passed as argument.
        """
        size_x = self.getChannelObject("beamInfoX").getValue() / 1000.0
        size_y = self.getChannelObject("beamInfoY").getValue() / 1000.0

        data = {
            "size_x": size_x,
            "size_y": size_y,
            "shape": "ellipse",
        }

        update_beam_callback(data)

    # TODO:Implement dynamically
    def use_sample_changer(self):
        """
        Overrides GenericDiffractometer method.
        """
        return True

    # TODO:Implement dynamically
    def in_plate_mode(self):
        """
        Overrides GenericDiffractometer method.
        """
        return False

    # We are using the sample_centring module. this is not used anymore
    # Not true, we use it!
    def start_manual_centring(self, *args, **kwargs):
        """
        Start manual centring. Overrides GenericDiffractometer method.
        Prepares diffractometer for manual centring.
        """
        if self.prepare_centring():
            GenericDiffractometer.start_manual_centring(self, *args, **kwargs)
        else:
            self.logger.info("Failed to prepare diffractometer for centring")
            self.invalidate_centring()

    def start_auto_centring(self, *args, **kwargs):
        """
        Start manual centring. Overrides GenericDiffractometer method.
        Prepares diffractometer for manual centring.
        """
        if self.prepare_centring():
            GenericDiffractometer.start_auto_centring(self, *args, **kwargs)
        else:
            self.logger.info("Failed to prepare diffractometer for centring")
            self.invalidate_centring()

    def prepare_centring(self):
        """
        Prepare beamline for to sample_view phase.
        """
        if not self.is_sample_view_phase():
            self.logger.info("Not in sample view phase. Asking supervisor to go")
            success = self.go_sample_view()
            # TODO: workaround to set omega velocity to 60
            try:
                self.phi_motor_hwobj.set_velocity(60)
            except:
                self.logger.debug("Cannot apply workaround for omega velocity")
            if not success:
                self.logger.info("Cannot set SAMPLE VIEW phase")
                return False

        return True

    def is_sample_view_phase(self):
        """
        Returns boolean by comparing the supervisor current phase and SAMPLE view phase.

        @return: boolean
        """
        self.logger.info("is_sample_view_phase?")
        
        return self.super_hwobj.get_current_phase().upper() == "SAMPLE"

    def get_grid_direction(self):
        
        grid_direction = self.getProperty("gridDirection")

        grid_direction = {}
        self.grid_direction['omega_ref'] = 1
        self.grid_direction['fast'] = [ 1, 0 ] # Qt4_GraphicsLib.py line 1184/85 MD2
        self.grid_direction['slow'] = [ 0, -1 ] # Qt4_GraphicsLib.py line 1184/85 MD2
        self.logger.info('diffr_hwobj grid_direction %s' % self.grid_direction)
        
        return self.grid_direction
        
    def go_sample_view(self):
        """
        Go to sample view phase.
        """
        self.super_hwobj.go_sample_view()

        while True:
            super_state = self.super_hwobj.get_state()
            self.logger.debug('Waiting for go_sample_view done (supervisor state is %s)'
                              % super_state)
            if super_state != DevState.MOVING:
                self.logger.debug('Go_sample_view done (%s)' % super_state)
                return True
            time.sleep(0.2)

    def supervisor_state_changed(self, state):
        """
        Emit stateChanged signal according to supervisor current state.
        """
        return
        self.current_state = state
        self.emit('stateChanged', (self.current_state, ))

    # TODO: Review override current_state by current_phase
    def supervisor_phase_changed(self, phase):
        """
        Emit stateChanged signal according to supervisor current phase.
        """
        #self.current_state = phase
        self.emit('minidiffPhaseChanged', (phase, ))

    def phi_motor_moved(self, pos):
        """
        Emit phiMotorMoved signal with position value.
        """
        self.current_motor_positions["phi"] = pos
        self.emit("phiMotorMoved", pos)

    def phi_motor_state_changed(self, state):
        """
        Emit stateChanged signal with state value.
        """
        self.current_motor_states["phi"] = state
        self.emit('stateChanged', (state, ))

    def phiz_motor_moved(self, pos):
        """
        """
        self.current_motor_positions["phiz"] = pos

    def phiz_motor_state_changed(self, state):
        """
        Emit stateChanged signal with state value.
        """
        self.emit('stateChanged', (state, ))

    def phiy_motor_state_changed(self, state):
        """
        Emit stateChanged signal with state value.
        """
        self.emit('stateChanged', (state, ))

    def phiy_motor_moved(self, pos):
        """
        """
        self.current_motor_positions["phiy"] = pos

    def zoom_position_changed(self, value):
        """
        Update positions after zoom changed.

        @value: zoom position.
        """
        #self.logger.debug("zoom position changed")
        self.update_pixels_per_mm()
        self.current_motor_positions["zoom"] = value

    def zoom_motor_predefined_position_changed(self, position_name, offset):
        """
        Update pixel size and emit signal.
        """
        #self.logger.debug("zoom predefined position changed")
        self.update_pixels_per_mm()
        self.emit('zoomMotorPredefinedPositionChanged',
                  (position_name, offset, ))

    def zoom_motor_state_changed(self, state):
        """
        Emit signal for motor zoom changed

        @state: new state value to emit.
        """
        self.emit('stateChanged', (state, ))

    def sampleX_motor_moved(self, pos):
        """
        """
        self.current_motor_positions["sampx"] = pos

    def sampleX_motor_state_changed(self, state):
        """
        Emit stateChanged signal with state value.
        """
        self.current_motor_states["sampx"] = state
        self.emit('stateChanged', (state, ))

    def sampleY_motor_moved(self, pos):
        """
        """
        self.current_motor_positions["sampy"] = pos

    def sampleY_motor_state_changed(self, state):
        """
        Emit stateChanged signal with state value.
        """
        self.current_motor_states["sampy"] = state
        self.emit('stateChanged', (state, ))

    def kappa_motor_moved(self, pos):
        """
        Emit kappaMotorMoved signal with position value.
        """
        self.current_motor_positions["kappa"] = pos
        self.emit("kappaMotorMoved", pos)

    def kappa_motor_state_changed(self, state):
        """
        Emit stateChanged signal with state value.
        """
        self.current_motor_states["kappa"] = state
        self.emit('stateChanged', (state, ))

    def kappa_phi_motor_moved(self, pos):
        """
        Emit kappa_phiMotorMoved signal with position value.
        """
        self.current_motor_positions["kappa_phi"] = pos
        self.emit("kappa_phiMotorMoved", pos)

    def kappa_phi_motor_state_changed(self, state):
        """
        Emit stateChanged signal with state value.
        """
        self.current_motor_states["kappa_phi"] = state
        self.emit('stateChanged', (state, ))

    def focus_motor_moved(self, pos):
        """
        """
        self.current_motor_positions["focus"] = pos

    def start_auto_focus(self):
        pass

    def move_omega(self, pos, velocity=None):
        """
        Move omega to absolute position.

        @pos: target position
        """
        # turn it on
        if velocity is not None:
            self.phi_motor_hwobj.set_velocity(velocity)
        self.phi_motor_hwobj.move(pos)
        time.sleep(0.2)
        # it should wait here

    def move_omega_relative(self, relpos):
        """
        Move omega to relative position.

        @relpos: target relative position
        """
        #TODO:Are all these waiting times really necessary??'
        self.wait_device_ready()
        self.phi_motor_hwobj.syncMoveRelative(relpos)
        time.sleep(0.2)
        self.wait_device_ready()

    # TODO: define phases as enum members.
    def set_phase(self, phase, timeout=None):
        #TODO: implement timeout. Current API to fulfilll the API.
        """
        General function to set phase by using supervisor commands.
        """
        if phase == "Transfer":
            self.super_hwobj.go_transfer()
        elif phase == "Collect":
            self.super_hwobj.go_collect()
        elif phase == "BeamView":
            self.super_hwobj.go_beam_view()
        elif phase == "Centring":
            self.super_hwobj.go_sample_view()
        else:
            self.logger.warning(
                "Diffractometer set_phase asked for un-handled phase: %s" %
                phase)
    
    # Copied from GenericDiffractometer just to improve error loggin
    def wait_device_ready(self, timeout=30):
        """ Waits when diffractometer status is ready:

        :param timeout: timeout in second
        :type timeout: int
        """
        gevent.sleep(1) # wait a bit to see if state does not change inmediately
        with gevent.Timeout(timeout, Exception("Timeout waiting for Diffracometer ready, check bl13/eh/diff. Is omegax close enough to 0??")):
            while not self.is_ready():
                time.sleep(0.01)

    # Overwrites GenericDiffractometer method
    def get_positions(self):
        """
        Descript. :
        """
        # Specific for XALOC: do we lose initial position events for these motors?
        self.current_motor_positions['sampx'] = self.sample_x_motor_hwobj.getPosition()
        self.current_motor_positions['sampy'] = self.sample_y_motor_hwobj.getPosition()

        return GenericDiffractometer.get_positions(self)


def test_hwo(hwo):
    print(hwo.get_phase_list())
