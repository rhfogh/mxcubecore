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
ALBADigitalZoom

[Description]
Hardware Object used to integrate the zoom capabilities of the Zoom system from
ARINAX using the TANGO layer.

[Emitted signals]
- stateChanged
- predefinedPositionChanged
"""

from __future__ import print_function
from enum import IntEnum, unique
import logging

from HardwareRepository import BaseHardwareObjects
from taurus.core.tango.enums import DevState

__credits__ = ["ALBA"]
__version__ = "2.3."
__category__ = "General"


# As in AbstractMotor class
@unique
class DigitalZoomState(IntEnum):
    """
    Defines valid digital zoom states
    """
    UNKNOWN = 0
    READY = 1
    LOW_LIMIT = 2
    HIGH_LIMIT = 3
    DISABLED = 4
    FAULT = -1


class ALBADigitalZoom(BaseHardwareObjects.Device):

    STATE = DigitalZoomState

    def __init__(self, name):
        BaseHardwareObjects.Device.__init__(self, name)
        self.logger = logging.getLogger("HWR.ALBADigitalZoom")
        self.chan_pos = None
        self.chan_state = None
        self.chan_labels = None
        self.chan_blight = None

        self.current_position = 0
        self.current_state = None

    def init(self):
        self.logger.debug("Initializing {0}".format(self.__class__.__name__))
        # TODO: This is not an IOR anymore
        self.chan_pos = self.getChannelObject("position")
        self.chan_state = self.getChannelObject("state")
        # TODO: labels must be harcoded or read as a property
        #self.chan_labels = self.getChannelObject("labels")
        # TODO: This has to be calibrated with zoom values in the [0,1] range
        #self.chan_blight = self.getChannelObject('blight')

        self.chan_pos.connectSignal("update", self.positionChanged)
        self.chan_state.connectSignal("update", self.stateChanged)

        self.current_position = self.getPosition()
        self.current_state = self.getState()

    def getPredefinedPositionsList(self):
        """
        :return: list of strings (['1 z1', '2 z2', ..., 'n zn']
        """
        # labels = self.chan_labels.getValue()
        # labels = labels.split()
        # retlist = []
        # for label in labels:
        #     pos = str(label.replace(":", " "))
        #     retlist.append(pos)
        # # self.logger.debug("Raw zoom-auto positions list: %s" % repr(retlist))
        # new_retlist = []
        # for n, e in enumerate(retlist):
        #     name = e.split()
        #     new_retlist.append("%s %s" % (n + 1, name[0]))
        # self.logger.debug("Zoom-auto positions list: %s" % repr(new_retlist))
        # return new_retlist
        # TODO: should return list of strings
        return list(range(1, 8))

    def moveToPosition(self, posno):
        """
        Move to one of the predefined positions named posno 'n zn'
        :param posno:
        :return:
        """
        #no = posno.split()[0]
        self.logger.debug("Moving to position %s" % posno)
        self.chan_pos.setValue(int(posno))

    def getLimits(self):
        # TODO: this should depend on the current configuration
        # In a digital zoom, this can only be the indexes (no real position)
        _min = self.getPredefinedPositionsList()[0]
        _max = self.getPredefinedPositionsList()[-1]
        return _min, _max

    def getState(self):
        """
        Digital Zoom state (enum) mapping from current Tango state
        Returns: DigitalZoomState

        """
        # TODO: Review type and values returned by Arinax server
        state = self.chan_state.getValue()
        curr_pos = self.getPosition()
        if state == DevState.ON:
            return self.STATE.READY
        elif curr_pos == self.getLimits()[0]:
            return self.STATE.LOW_LIMIT
        elif curr_pos == self.getLimits()[-1]:
            return self.STATE.HIGH_LIMIT
        if state == DevState.UNKNOWN:
            # TODO: servers returns always UNKNWON
            return self.STATE.READY
            #return self.STATE.UNKNOWN
        else:
            return self.STATE.FAULT

    def getPosition(self):
        # not a real position, but the index
        return self.chan_pos.getValue()

    def getCurrentPositionName(self):
        try:
            n = int(self.chan_pos.getValue())
            value = "%s z%s" % (n, n)
            return str(n)
            return value
        except Exception as e:
            self.logger.debug("Cannot get zoom (<value> z<value>) pair \n%s" % str(e))
            return None

    def stateChanged(self, state):
        # TODO: Review state type emitted
        # We should use the state received!
        the_state = self.getState()
        if the_state != self.current_state:
            self.logger.debug(
                "old state = %s, new state = %s" % (self.current_state, the_state))
            self.logger.debug("stateChanged emitted: %s" % the_state)
            self.current_state = the_state
            self.emit('stateChanged', (the_state,))

    def positionChanged(self, position):
        the_position = self.getCurrentPositionName()
        if the_position != self.current_position:
            self.logger.debug("old position = %s, new position = %s" % (
            self.current_position, the_position))
            self.logger.debug("predefinedPositionChanged emitted: %s" % the_position)
            self.current_position = the_position
            self.emit('predefinedPositionChanged', (the_position, 0))
            # TODO: blight range is 0-30
            # Links the zoom position with the backlight value
            #self.chan_blight.setValue(int(self.current_position.split()[0]))

    def isReady(self):
        state = self.getState()
        #TODO: server state is ALWAYS Unknown
        return True
        return state == self.STATE.READY


def test_hwo(zoom):
    print("Zoom position is : ", zoom.getPosition())
    print("Zoom position name is : ", zoom.getCurrentPositionName())
    print("Moving : ", zoom.motorIsMoving())
    print("State : ", zoom.getState())
    print("Positions : ", zoom.getPredefinedPositionsList())
