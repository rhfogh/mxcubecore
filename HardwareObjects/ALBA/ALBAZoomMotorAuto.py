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
ALBAZoomMotorAuto

[Description]
Hardware Object is used to manipulate the zoom of the OAV camera.

[Emitted signals]
- stateChanged
- predefinedPositionChanged
"""

from __future__ import print_function

import logging

from HardwareRepository import BaseHardwareObjects
from taurus.core.tango.enums import DevState

__credits__ = ["ALBA"]
__version__ = "2.3."
__category__ = "General"


class ALBAZoomMotorAuto(BaseHardwareObjects.Device):

    INIT, FAULT, READY, MOVING, ONLIMIT = range(5)

    def __init__(self, name):
        BaseHardwareObjects.Device.__init__(self, name)
        self.logger = logging.getLogger("HWR.ALBAZoomMotorAuto")
        self.chan_position = None
        self.chan_state = None
        self.chan_labels = None
        self.chan_blight = None

        self.current_position = 0
        self.current_state = None

    def init(self):
        self.logger.debug("Initializing {0}".format(self.__class__.__name__))
        self.chan_position = self.getChannelObject("position")
        self.chan_state = self.getChannelObject("state")
        self.chan_labels = self.getChannelObject("labels")
        self.chan_blight = self.getChannelObject('blight')
        
        self.chan_position.connectSignal("update", self.positionChanged)
        self.chan_state.connectSignal("update", self.stateChanged)
        
        self.current_position = self.getPosition()
        self.current_state = self.getState()

    def getPredefinedPositionsList(self):
        """
        :return: list of strings (['1 z1', '2 z2', ..., 'n zn']
        """
        labels = self.chan_labels.getValue()
        labels = labels.split()
        retlist = []
        for label in labels:
            pos = str(label.replace(":", " "))
            retlist.append(pos)
        # self.logger.debug("Raw zoom-auto positions list: %s" % repr(retlist))
        new_retlist = []
        for n, e in enumerate(retlist):
            name = e.split()
            new_retlist.append("%s %s" % (n + 1, name[0]))
        self.logger.debug("Zoom-auto positions list: %s" % repr(new_retlist))
        return new_retlist

    def moveToPosition(self, posno):
        """
        Move to one of the predefined positions named posno 'n zn'
        :param posno:
        :return:
        """
        no = posno.split()[0]
        self.logger.debug("Moving to position %s" % no)
        self.chan_position.setValue(int(no))

    def motorIsMoving(self):
        if self.getState() in [DevState.MOVING, DevState.RUNNING]:
            return True
        else:
            return False

    def getLimits(self):
        return 1, 12

    def getState(self):
        state = self.chan_state.getValue()
        curr_pos = self.getPosition()
        if state == DevState.ON:
            return ALBAZoomMotorAuto.READY
        elif state == DevState.MOVING or state == DevState.RUNNING:
            return ALBAZoomMotorAuto.MOVING
        elif curr_pos in self.getLimits():
            return ALBAZoomMotorAuto.ONLIMIT
        else:
            return ALBAZoomMotorAuto.FAULT

    def getPosition(self):
        try:
            return self.chan_position.getValue()
        except Exception as e:
            return self.current_position

    def getCurrentPositionName(self):
        try:
            n = int(self.chan_position.getValue())
            value = "%s z%s" % (n, n)
            return value
        except Exception as e:
            self.logger.debug("Cannot get zoom (<value> z<value>) pair \n%s" % str(e))
            return None

    def stateChanged(self, state):
        the_state = self.getState()
        if int(the_state) != int(self.current_state):
            self.logger.debug("old state = %s, new state = %s" % (self.current_state, the_state))
            self.logger.debug("stateChanged emitted: %s" % the_state)
            self.current_state = the_state
            self.emit('stateChanged', (the_state, ))

    def positionChanged(self, position):
        the_position = self.getCurrentPositionName()
        if the_position != self.current_position:
            self.logger.debug("old position = %s, new position = %s" % (self.current_position, the_position))
            self.logger.debug("predefinedPositionChanged emitted: %s" % the_position)
            self.current_position = the_position
            self.emit('predefinedPositionChanged', (the_position, 0))
            self.chan_blight.setValue(int(self.current_position.split()[0]))

    def isReady(self):
        state = self.getState()
        return state == ALBAZoomMotorAuto.READY


def test_hwo(zoom):
    print("Zoom position is : ", zoom.getPosition())
    print("Zoom position name is : ", zoom.getCurrentPositionName())
    print("Moving : ", zoom.motorIsMoving())
    print("State : ", zoom.getState())
    print("Positions : ", zoom.getPredefinedPositionsList())
