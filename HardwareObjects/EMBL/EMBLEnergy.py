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

"""EMBLEnergy"""

import logging
import gevent

from HardwareRepository.BaseHardwareObjects import Device


__credits__ = ["EMBL Hamburg"]
__license__ = "LGPLv3+"
__category__ = "General"


class EMBLEnergy(Device):
    """
    Defines interface to the Tine energy server
    """

    def __init__(self, name):

        Device.__init__(self, name)

        self.ready_event = None
        self.tunable = False
        self.current_energy = 0
        self.current_wav = 0
        self.moving = None
        self.default_en = 0
        self.en_lims = [None, None]
        self.undulator_gaps = []
        self.ctrl_bytes = None
        self.bragg_break_status = None
        self.do_beam_alignment = True
        self.delta = 0

        self.chan_energy = None
        self.chan_limit_low = None
        self.chan_limit_high = None
        self.chan_status = None
        self.chan_undulator_gaps = None
        self.chan_status_bragg_break = None
        self.cmd_set_energy = None
        self.cmd_energy_ctrl_byte = None
        self.cmd_set_break_bragg = None
        self.cmd_release_break_bragg = None
        self.cmd_reset_perp = None

    def init(self):

        self.moving = False
        self.ready_event = gevent.event.Event()

        self.cmd_set_energy = self.getCommandObject("cmdSetEnergy")
        self.cmd_energy_ctrl_byte = self.getCommandObject("cmdEnergyCtrlByte")
        self.cmd_set_break_bragg = self.getCommandObject("cmdSetBreakBragg")
        self.cmd_release_break_bragg = self.getCommandObject("cmdReleaseBreakBragg")
        self.cmd_reset_perp = self.getCommandObject("cmdResetPerp")

        self.chan_energy = self.getChannelObject("chanEnergy")
        if self.chan_energy is not None:
            self.chan_energy.connectSignal("update", self.energy_position_changed)

        self.chan_limit_low = self.getChannelObject("chanLimitLow", optional=True)
        if self.chan_limit_low is not None:
            self.chan_limit_low.connectSignal("update", self.energy_limits_changed)

        self.chan_limit_high = self.getChannelObject("chanLimitHigh", optional=True)
        if self.chan_limit_high is not None:
            self.chan_limit_high.connectSignal("update", self.energy_limits_changed)

        self.chan_status = self.getChannelObject("chanStatus")
        if self.chan_status is not None:
            self.chan_status.connectSignal("update", self.energy_state_changed)

        self.chan_undulator_gaps = self.getChannelObject(
            "chanUndulatorGap", optional=True
        )
        if self.chan_undulator_gaps is not None:
            self.chan_undulator_gaps.connectSignal(
                "update", self.undulator_gaps_changed
            )

        self.chan_status_bragg_break = self.getChannelObject("chanStatusBraggBreak")
        if self.chan_status_bragg_break is not None:
            self.chan_status_bragg_break.connectSignal(
                "update", self.bragg_break_status_changed
            )

        try:
            self.tunable = self.getProperty("tunableEnergy")
        except BaseException:
            logging.getLogger("HWR").warning("Energy: will set to fixed energy")
        try:
            self.default_en = self.getProperty("defaultEnergy")
        except BaseException:
            logging.getLogger("HWR").warning("Energy: no default energy defined")

        try:
            self.en_lims = eval(self.getProperty("staticLimits"))
        except BaseException:
            self.en_lims = [None, None]
        self.ctrl_bytes = eval(self.getProperty("ctrlBytes"))

        if not self.chan_energy:
            self.energy_position_changed(self.default_en * 1000)

    def can_move_energy(self):
        """
        Returns True if possible to change energy
        """
        return self.tunable

    def is_connected(self):
        """
        Returns True if connected
        """
        return True

    def is_ready(self):
        """
        In this case device is always ready
        """
        return True

    def set_do_beam_alignment(self, state):
        """
        Enables/disable beam alignment after changing the energy
        :param state: boolean
        :return:
        """
        self.do_beam_alignment = state

    def get_current_energy(self):
        """
        Returns current energy in keV
        :return: float
        """

        value = self.default_en
        if self.chan_energy is not None:
            try:
                value = self.chan_energy.getValue()
                return value[0] / 1000
            except BaseException:
                logging.getLogger("HWR").exception(
                    "Energy: could not read current energy"
                )
                return None
        return value

    def get_current_wavelength(self):
        """
        Returns current wavelength in A
        :return: float
        """
        current_en = self.get_current_energy()
        current_wav = None

        if current_en is not None:
            current_wav = 12.3984 / current_en
        return current_wav

    def get_energy_limits(self):
        """
        Returns energy limits as list of two floats
        :return: (float, float)
        """
        if self.chan_limit_low is not None and self.chan_limit_high is not None:
            try:
                self.en_lims[0] = self.chan_limit_low.getValue()
                self.en_lims[1] = self.chan_limit_high.getValue()
            except BaseException:
                logging.getLogger("HWR").exception(
                    "Energy: could not read energy limits"
                )
        return self.en_lims

    def get_wavelength_limits(self):
        """
        Returns wavelength limits as list of two floats
        :return: (float, float)
        """
        lims = None
        self.en_lims = self.get_energy_limits()
        if self.en_lims is not None:
            lims = (12.3984 / self.en_lims[1], 12.3984 / self.en_lims[0])
        return lims

    def move_energy_started(self):
        """
        Emits moveEnergyStarted signal
        :return:
        """
        self.emit("moveEnergyStarted", ())

    def move_energy_failed(self):
        """
        Emits moveEnergyFailedsignal
        :return:
        """
        self.moving = False
        self.emit("moveEnergyFailed", ())

    def move_energy_aborted(self):
        """
        Emits moveEnergyFailed signal
        :return:
        """
        self.moving = False
        self.emit("moveEnergyFailed", ())

    def move_energy_finished(self, result):
        """
        Emits moveEnergyFinished signal
        :param result:
        :return:
        """
        self.moving = False
        logging.getLogger("GUI").info("Energy change finished")
        self.emit("moveEnergyFinished", ())

    def check_limits(self, value):
        """
        Checks given value if it is within limits
        """
        logging.getLogger("HWR").info("Checking the move limits")
        result = False

        if self.en_lims[0] <= value <= self.en_lims[1]:
            logging.getLogger("HWR").info("Limits ok")
            result = True
        else:
            logging.getLogger("GUI").info("Energy: Requested value is out of limits")
        return result

    def start_move_wavelength(self, value, wait=True):
        """
        Starts wavelength change
        :param value: float
        :param wait: boolean
        :return:
        """
        logging.getLogger("HWR").info("Moving wavelength to (%s)" % value)
        return self.move_energy(12.3984 / value, wait)
        # return self.startMoveEnergy(value, wait)

    def cancel_move_energy(self):
        """
        Cancels energy change
        :return:
        """
        logging.getLogger("user_level_log").info("Energy: Cancel move")
        # self.moveEnergy.abort()

    def move_energy(self, energy, wait=True):
        """
        Sets energy in keV
        """
        # gevent.spawn(self.move_energy_task(energy))
        self.move_energy_task(energy)

    def set_value(self, value, timeout=None):
        self.move_energy(value=value, wait=(timeout is not None))

    def move_energy_task(self, energy):
        """
        Actual energy change task
        :param energy: in keV, float
        :return:
        """
        current_en = self.get_current_energy()
        pos = abs(current_en - energy)
        self.delta = pos
        if pos < 0.001:
            self.emit("stateChanged", ("ready",))
        else:
            # if energy <= 6:
            #    self.cmd_energy_ctrl_byte(self.ctrl_bytes[0])
            # else:
            #    self.cmd_energy_ctrl_byte(self.ctrl_bytes[1])
            if self.cmd_energy_ctrl_byte is not None:
                if pos > 0.1:
                    # p13 63, p14 15
                    self.cmd_energy_ctrl_byte(self.ctrl_bytes[1])
                else:
                    self.cmd_energy_ctrl_byte(self.ctrl_bytes[0])

            self.moving = pos
            self.release_break_bragg()
            gevent.sleep(2)

            if self.cmd_set_energy:
                logging.getLogger("GUI").info("Energy: Moving to %.2f keV", energy)
                self.emit("statusInfoChanged", "Moving to %.2f keV" % energy)
                self.cmd_set_energy(energy)
            else:
                # Mockup mode
                self.energy_position_changed([energy * 1000])

    def move_wavelength(self, value, wait=True):
        """
        Changes wavelength (in Angstroms)
        :param value: wavelength in Angstroms (float)
        :param wait: boolean
        :return:
        """
        self.move_energy(12.3984 / value, wait)

    def energy_position_changed(self, pos):
        """
        Event called when energy value has been changed
        :param pos: float
        :return:
        """
        # self.moveEnergyCmdFinished(True)
        if isinstance(pos, (list, tuple)):
            pos = pos[0]
        energy = pos / 1000
        if abs(energy - self.current_energy) > 1e-3:
            self.current_energy = energy
            self.current_wav = 12.3984 / energy
            if self.current_wav is not None:
                self.emit("energyChanged", (self.current_energy, self.current_wav))
                self.emit("valueChanged", (self.current_energy,))

    def energy_limits_changed(self, limits):
        """
        Updates energy limits
        :param limits: (float, float)
        :return:
        """
        limits = self.get_energy_limits()
        self.emit("energyLimitsChanged", (limits,))

    def energy_state_changed(self, state):
        """
        Updates energy status
        :param state: int
        :return:
        """
        # logging.getLogger('HWR').info("Energy: State changed to %s" % str(state))
        self.energy_server_check_for_errors(state)
        state = int(state[0])
        if state == 0:
            if self.moving:
                self.moving = False
                self.set_break_bragg()
                if self.cmd_reset_perp is not None:
                    logging.getLogger("HWR").info("Energy: Perp reset sent")
                    self.cmd_reset_perp()
            self.move_energy_finished(0)
            self.emit("stateChanged", "ready")
            self.emit("statusInfoChanged", "")
            if self.do_beam_alignment and self.delta > 0.1:
                self.emit("beamAlignmentRequested")
            self.delta = 0

        elif state == 1:
            self.move_energy_started()
            self.emit("stateChanged", "busy")

    def wait_ready(self, timeout=20):
        """
        Waits till energy change is done
        :param timeout: sec in int
        :return:
        """
        with gevent.Timeout(timeout, Exception("Energy: Timeout waiting for ready")):
            while self.chan_status.getValue()[0] != 0:
                gevent.sleep(0.1)

    def energy_server_check_for_errors(self, state):
        """
        Displays error message if the energy change fails
        :param state: list of ints
        :return:
        """
        if state[0] == 1.0 or state[1] == 63:
            return
        elems = ["hdm1", "hdm2", "roll", "undulator", "bragg", "perp"]
        message = "Energy: Error, setting energy failed on motors: "
        bits = [int(state[1]) >> i & 1 for i in range(5, -1, -1)]
        # logging.getLogger('GUI').error("%s"%bits)
        for i in range(6):
            if bits[i] == 0:
                message = "%s %s" % (message, elems[i])
        logging.getLogger("GUI").error(message)
        self.emit("statusInfoChanged", message)

    def bragg_break_status_changed(self, status):
        """
        Updates status of bragg breaks
        :param status:
        :return:
        """
        self.bragg_break_status = status

    def get_value(self):
        """
        Returns current energy
        :return:
        """
        return self.get_current_energy()

    def update_values(self):
        """
        Reemits signals
        :return:
        """
        self.emit("energyChanged", (self.current_energy, self.current_wav))
        self.emit("valueChanged", (self.current_energy,))

    def undulator_gaps_changed(self, value):
        """
        Updates undulator gaps
        :param value:
        :return:
        """
        if isinstance(value, (list, tuple)):
            self.undulator_gaps = [value[0]]
        else:
            self.undulator_gaps = [value]

    def get_undulator_gaps(self):
        """
        Returns undulator gaps
        :return:
        """
        if self.chan_undulator_gaps:
            self.undulator_gaps_changed(self.chan_undulator_gaps.getValue())
        return self.undulator_gaps

    def set_break_bragg(self):
        """
        Sets bragg breaks
        :return:
        """
        if self.chan_status_bragg_break.getValue() != 0:
            logging.getLogger("GUI").warning("Energy: Setting bragg brake...")
            self.emit("statusInfoChanged", "Setting Bragg break...")
            gevent.sleep(3)
            self.wait_ready()
            gevent.sleep(1)
            self.wait_ready()
            logging.getLogger("HWR").info("Energy: Set bragg break cmd send")
            self.cmd_set_break_bragg(1)
            gevent.sleep(2)
            if self.chan_status_bragg_break is not None:
                logging.getLogger("HWR").warning(
                    "Energy: Waiting for break set (first try) ..."
                )
                with gevent.Timeout(
                    20, Exception("Energy: Timeout waiting for break set")
                ):
                    while self.chan_status_bragg_break.getValue() != 0:
                        gevent.sleep(0.1)
                gevent.sleep(3)
                logging.getLogger("HWR").warning(
                    "Waiting for break set (second try) ..."
                )
                with gevent.Timeout(20, Exception("Timeout waiting for break set")):
                    while self.chan_status_bragg_break.getValue() != 0:
                        gevent.sleep(0.1)
            else:
                gevent.sleep(10)
            self.emit("statusInfoChanged", "Bragg break set")
            logging.getLogger("GUI").info("Energy: Bragg brake set")
        else:
            logging.getLogger("GUI").info("Energy: Bragg brake already set")

    def release_break_bragg(self):
        """
        Release bragg breaks
        :return:
        """
        if self.chan_status_bragg_break.getValue() != 1:
            logging.getLogger("GUI").warning("Energy: Releasing bragg brake...")
            self.emit("statusInfoChanged", "Releasing Bragg break...")
            self.cmd_release_break_bragg(1)
            gevent.sleep(2)
            if self.chan_status_bragg_break is not None:
                logging.getLogger("GUI").warning(
                    "Energy: Waiting for brake released..."
                )
                with gevent.Timeout(
                    20, Exception("Energy: Timeout waiting for break release")
                ):
                    while self.chan_status_bragg_break.getValue() != 1:
                        gevent.sleep(0.1)

            else:
                logging.getLogger("GUI").info(
                    "Energy: Sleep 10 sec before brake released..."
                )
                gevent.sleep(10)
            logging.getLogger("GUI").info("Energy: Bragg brake released")
            self.emit("statusInfoChanged", "Bragg break released")
        else:
            logging.getLogger("GUI").info("Energy: Bragg brake already released")
            self.emit("statusInfoChanged", "Bragg break released")
