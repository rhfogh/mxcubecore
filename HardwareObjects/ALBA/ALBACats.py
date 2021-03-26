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
[Name] ALBACats

[Description]
HwObj used to control the CATS sample changer via Tango.

[Signals]
- powerStateChanged
- runningStateChanged

Comments:
In case of failed put push button, CatsMaint calls the reset command in the DS
In case of failed get push button, CatsMaint calls the recoverFailure command in the DS
"""

from __future__ import print_function

import logging
import time
import gevent

from sample_changer.Cats90 import Cats90, SampleChangerState, TOOL_SPINE

__credits__ = ["ALBA Synchrotron"]
__version__ = "2.3"
__category__ = "General"

TIMEOUT = 3
DOUBLE_GRIPPER_DRY_WAIT_TIME = 80 # time the double gripper takes in going from home to midway soak during a dry

TOOL_FLANGE, TOOL_UNIPUCK, TOOL_SPINE, TOOL_PLATE, \
    TOOL_LASER, TOOL_DOUBLE_GRIPPER = (0,1,2,3,4,5)

class ALBACats(Cats90):
    """
    Main class used @ ALBA to integrate the CATS-IRELEC sample changer.
    """

    def __init__(self, *args):
        Cats90.__init__(self, *args)
        self.logger = logging.getLogger("HWR.ALBACats")
        self.detdist_saved = None

        self.shifts_channel = None
        self.phase_channel = None
        self.super_state_channel = None
        self.detdist_position_channel = None
        self._chnPathSafe = None
        self._chnCollisionSensorOK = None
        self._chnIsCatsIdle = None
        self._chnIsCatsHome = None
        self._chnIsCatsRI1 = None
        self._chnIsCatsRI2 = None
        self._chnNBSoakings = None
        self._chnLidSampleOnTool = None
        self._chnNumSampleOnTool = None
        
        self.go_transfer_cmd = None
        self.diff_go_sampleview_cmd = None
        self.super_go_sampleview_cmd = None
        self.super_abort_cmd = None

        self._cmdLoadHT = None
        self._cmdChainedLoadHT = None
        self._cmdUnloadHT = None
        self._cmdClearMemory = None
        self._cmdSetTool = None

        self.auto_prepare_diff = None

    def init(self):
        self.logger.debug("Initializing {0}".format(self.__class__.__name__))
        Cats90.init(self)
        # TODO: Migrate to taurus channels instead of tango channels
        self.shifts_channel = self.getChannelObject("shifts")
        self.phase_channel = self.getChannelObject("phase")
        self.super_state_channel = self.getChannelObject("super_state")
        self.detdist_position_channel = self.getChannelObject("detdist_position")

        self._chnPathSafe = self.getChannelObject("_chnPathSafe")
        self._chnCollisionSensorOK = self.getChannelObject("_chnCollisionSensorOK")
        self._chnIsCatsIdle = self.getChannelObject( "_chnIsCatsIdle" )
        self._chnIsCatsHome = self.getChannelObject( "_chnIsCatsHome" )
        self._chnIsCatsRI1 = self.getChannelObject( "_chnIsCatsRI1" )
        self._chnIsCatsRI2 = self.getChannelObject( "_chnIsCatsRI2" )
        self._chnNBSoakings = self.getChannelObject( "_chnNBSoakings" )
        self._chnLidSampleOnTool = self.getChannelObject( "_chnLidSampleOnTool" )
        self._chnNumSampleOnTool = self.getChannelObject( "_chnNumSampleOnTool" )
        
        self.go_transfer_cmd = self.getCommandObject("go_transfer")
        self.diff_go_sampleview_cmd = self.getCommandObject("diff_go_sampleview")
        self.super_go_sampleview_cmd = self.getCommandObject("super_go_sampleview")
        self.super_abort_cmd = self.getCommandObject("super_abort")

        self._cmdLoadHT = self.getCommandObject("_cmdLoadHT")
        self._cmdChainedLoadHT = self.getCommandObject("_cmdChainedLoadHT")
        self._cmdUnloadHT = self.getCommandObject("_cmdUnloadHT")
        self._cmdClearMemory = self.getCommandObject("_cmdClearMemory")
        self._cmdSetTool = self.getCommandObject("_cmdSetTool")
        self._cmdSetTool2 = self.getCommandObject("_cmdSetTool2")

        self.auto_prepare_diff = self.getProperty("auto_prepare_diff")

        if self._chnPathRunning is not None:
            self._chnPathRunning.connectSignal("update", self._update_running_state)

        if self._chnPowered is not None:
            self._chnPowered.connectSignal("update", self._update_powered_state)

        ret,msg = self._check_coherence()
        if not ret: 
            logging.getLogger('user_level_log').warning( msg )


    def isReady(self):
        """
        Returns a boolean value indicating is the sample changer is ready for operation.

        @return: boolean
        """
        return self.state == SampleChangerState.Ready or \
            self.state == SampleChangerState.Loaded or \
            self.state == SampleChangerState.Charging or \
            self.state == SampleChangerState.StandBy or \
            self.state == SampleChangerState.Disabled

    #TODO: rename this method, it is the supervisor that is sent to transfer
    def diff_send_transfer(self):
        """
        Checks if beamline supervisor is in TRANSFER phase (i.e. sample changer in
        TRANSFER phase too). If is not the case, It sends the sample changer to TRANSFER
        phase. Returns a boolean value indication if the sample changer is in TRANSFER
        phase.

        @return: boolean
        """
        if self.read_super_phase().upper() == "TRANSFER":
            self.logger.error("Supervisor is already in transfer phase")
            return True

        # First wait till the diff is ready to accept a go_transfer_cmd
        t0 = time.time()
        while True:
            state = str(self.super_state_channel.getValue())
            if str(state) == "ON":
                break

            if (time.time() - t0) > TIMEOUT:
                self.logger.error("Supervisor timeout waiting for ON state. Returning")
                return False

            time.sleep(0.1)

        self.go_transfer_cmd()
        ret = self._wait_phase_done('TRANSFER')
        return ret

    # When the double gripper does a dry, it takes a long time, therefore the timeout
    def diff_send_sampleview(self, timeout = DOUBLE_GRIPPER_DRY_WAIT_TIME + 10):
        """
        Do a phased go to sample view. 
        First send the diff to sample view
        Then wait for supervisor to be ready and send supervisor to sample view
        Returns a boolean value indication if the diff is in SAMPLE phase.

        @return: boolean
        """
        self.diff_go_sampleview_cmd()
        
        if self.read_super_phase().upper() == "SAMPLE":
            self.logger.error("Supervisor is already in sample view phase")
            return True

        t0 = time.time()
        while True:
            state = str(self.super_state_channel.getValue())
            if str(state) == "ON":
                break

            if (time.time() - t0) > timeout:
                self.logger.error("Supervisor timeout waiting for ON state. Returning")
                return False

            time.sleep(5)

        self.super_go_sampleview_cmd()
        ret = self._wait_phase_done('SAMPLE')
        return ret


    # TODO: Move to ALBASupervisor 
    def _wait_super_ready(self):
        while True:
            state = str(self.super_state_channel.getValue())
            if state == "ON":
                self.logger.debug("Supervisor is in ON state. Returning")
                break
            time.sleep(0.2)

    def _wait_cats_idle(self):
        while True:
            if self._chnIsCatsIdle.getValue():
                self.logger.debug("_chnIsCatsIdle %s, type %s" % ( str(self._chnIsCatsIdle.getValue()), type(self._chnIsCatsIdle.getValue()) ) )
                self.logger.debug("CATS is idle. Returning")
                break
            time.sleep(0.2)

    def _wait_cats_home(self, timeout):
        t0 = time.time()
        while True:
            if self._chnIsCatsHome.getValue():
                self.logger.debug("CATS is home. Returning")
                break
            time.sleep(0.2)
            if time.time() - t0 > timeout: return False
        
        return True

    def _wait_super_moving(self):
        allokret = True # No problems
        while allokret:
            state = str(self.super_state_channel.getValue())
            if not self._chnCollisionSensorOK.getValue(): 
                self._updateState()
                raise Exception ("The robot had a collision, call your LC or floor coordinator")
            elif state == "MOVING":
                self.logger.debug("Supervisor is in MOVING state. Returning")
                return allokret
            time.sleep(0.1)

        return allokret

    def _wait_phase_done(self, final_phase, timeout = 20 ):
        """
        Method to wait a phase change. When supervisor reaches the final phase, the
        method returns True.

        @final_phase: target phase
        @return: boolean
        """
       
        t0 = time.time()
        while self.read_super_phase().upper() != final_phase:
            state = str(self.super_state_channel.getValue())
            phase = self.read_super_phase().upper()
            if not str(state) in [ "MOVING", "ON" ]:
                self.logger.error("Supervisor is in a funny state %s" % str(state))
                return False

            self.logger.debug("Supervisor waiting to finish phase change")
            time.sleep(0.2)
            if time.time() - t0 > timeout: break

        if self.read_super_phase().upper() != final_phase:
            self.logger.error("Supervisor is not yet in %s phase. Aborting load" %
                              final_phase)
            return False
        else:
            self.logger.info(
                "Supervisor is in %s phase. Beamline ready to start sample loading..." %
                final_phase)
            return True

    def save_detdist_position(self):
        self.detdist_saved = self.detdist_position_channel.getValue()
        self.logger.error("Saving current det.distance (%s)" % self.detdist_saved)

    def restore_detdist_position(self):
        if abs(self.detdist_saved - self.detdist_position_channel.getValue()) >= 0.1:
            self.logger.error(
                "Restoring det.distance to %s" % self.detdist_saved)
            self.detdist_position_channel.setValue(self.detdist_saved)
            time.sleep(0.4)
            self._wait_super_ready()

    def read_super_phase(self):
        """
        Returns supervisor phase (CurrentPhase attribute from Beamline Supervisor
        TangoDS)

        @return: str
        """
        return self.phase_channel.getValue()

    def load(self, sample=None, wait=False, wash=False):
        """
        Loads a sample. Overides to include ht basket.

        @sample: sample to load.
        @wait:
        @wash: wash dring the load opearation.
        @return:
        """

        self.logger.debug(
            "Loading sample %s / type(%s)" %
            (sample, type(sample)))

        ret, msg = self._check_incoherent_sample_info()
        if not ret:
            raise Exception(msg)

        sample_ht = self.is_ht_sample(sample)

        if not sample_ht:
            sample = self._resolveComponent(sample)
            self.assertNotCharging()
            use_ht = False
        else:
            sample = sample_ht
            use_ht = True

        if self.hasLoadedSample():
            if (wash is False) and self.getLoadedSample() == sample:
                raise Exception(
                    "The sample %s is already loaded" % sample.getAddress())
            else:
                # Unload first / do a chained load
                pass

        return self._executeTask(SampleChangerState.Loading,
                                 wait, self._doLoad, sample, None, use_ht)

    def unload(self, sample_slot=None, wait=False):
        """
        Unload the sample. If sample_slot=None, unloads to the same slot the sample was
        loaded from.

        @sample_slot:
        @wait:
        @return:
        """
        sample_slot = self._resolveComponent(sample_slot)

        self.assertNotCharging()

        # In case we have manually mounted we can command an unmount
        if not self.hasLoadedSample():
            raise Exception("No sample is loaded")

        return self._executeTask(SampleChangerState.Unloading,
                                 wait, self._doUnload, sample_slot)

    # TODO: this overides identical method from Cats90
    def isPowered(self):
        return self._chnPowered.getValue()

    # TODO: this overides identical method from Cats90
    def isPathRunning(self):
        return self._chnPathRunning.getValue()

    def _update_running_state(self, value):
        """
        Emits signal with new Running State

        @value: New running state
        """
        self.emit('runningStateChanged', (value, ))

    def _update_powered_state(self, value):
        """
        Emits signal with new Powered State

        @value: New powered state
        """
        self.emit('powerStateChanged', (value, ))

    def _doLoad(self, sample=None, shifts=None, use_ht=False, waitsafe=True):
        """
        Loads a sample on the diffractometer. Performs a simple put operation if the
        diffractometer is empty, and a sample exchange (unmount of old + mount of new
        sample) if a sample is already mounted on the diffractometer.
        Overides Cats90 method.

        @sample: sample to load.
        @shifts: mounting point offsets.
        @use_ht: mount a sample from hot tool.
        """
        if not self._chnPowered.getValue():
            # TODO: implement a wait with timeout method.
            self.logger.debug("CATS power is OFF. Trying to switch the power ON...")
            self._cmdPowerOn()  # try switching power on
            time.sleep(2)

        current_tool = self.get_current_tool()

        self.save_detdist_position()
        ret = self.diff_send_transfer()

        if ret is False:
            self.logger.error(
                "Supervisor cmd transfer phase returned an error.")
            self._updateState()
            raise Exception(
                "Supervisor cannot get to transfer phase. Aborting sample changer operation. Ask LC or floor coordinator to check the supervisor and diff device servers")

        if not self._chnPowered.getValue():
            raise Exception(
                "CATS power is not enabled. Please switch on arm power before "
                "transferring samples.")

        # obtain mounting offsets from diffr
        shifts = self._get_shifts()

        if shifts is None:
            xshift, yshift, zshift = ["0", "0", "0"]
        else:
            xshift, yshift, zshift = map(str, shifts)

        # get sample selection
        selected = self.getSelectedSample()

        self.logger.debug("Selected sample is %s (prev %s)" %
                          (str(selected), str(sample)))

        if not use_ht:
            if sample is not None:
                if sample != selected:
                    self._doSelect(sample)
                    selected = self.getSelectedSample()
            else:
                if selected is not None:
                    sample = selected
                else:
                    raise Exception("No sample selected")
        else:
            selected = None

        # some cancel cases
        if not use_ht and self.hasLoadedSample() and selected == self.getLoadedSample(): # sample on diff is the one loaded
            self._updateState()
            raise Exception("The sample " +
                            str(self.getLoadedSample().getAddress()) +
                            " is already loaded")

        if not self.hasLoadedSample() and self.cats_sample_on_diffr() == 1:
            self.logger.warning(
                "Sample on diffractometer, loading aborted!")
            self._updateState()
            raise Exception("The sample " +
                            str(self.getLoadedSample().getAddress()) +
                            " is already loaded")

        if self.cats_sample_on_diffr() == -1 and self.hasLoadedSample(): # no sample on diff, but cats has sample info
            self._updateState()
            raise Exception(
                "Conflicting info between diffractometer and on-magnet detection."
                "Consider 'Clear'")

        # end some cancel cases

        # if load_ht
        loaded_ht = self.is_loaded_ht()

        #
        # Loading HT sample
        #
        if use_ht:  # loading HT sample

            if loaded_ht == -1:  # has loaded but it is not HT
                # first unmount (non HT)
                self.logger.error("Mixing load/unload dewar vs HT, NOT IMPLEMENTED YET")
                return

            tool = self.tool_for_basket(100)  # basketno)

            if tool != current_tool:
                self.logger.warning("Changing tool from %s to %s" %
                                    (current_tool, tool))
                changing_tool = True
            else:
                changing_tool = False

            argin = ["2", str(sample), "0", "0", xshift, yshift, zshift]
            self.logger.warning("Loading HT sample, %s" % str(argin))
            if loaded_ht == 1:  # has ht loaded
                cmd_ok = self._executeServerTask(self._cmdChainedLoadHT,
                                                 argin, waitsafe=True)
            else:
                cmd_ok = self._executeServerTask(self._cmdLoadHT, argin, waitsafe=False)

        #
        # Loading non HT sample
        #
        else:
            if loaded_ht == 1:  # has an HT sample mounted
                # first unmount HT
                self.logger.warning(
                    "Mixing load/unload dewar vs HT, NOT IMPLEMENTED YET")
                return

            basketno = selected.getBasketNo()
            sampleno = selected.getVialNo()

            lid, sample = self.basketsample_to_lidsample(basketno, sampleno)
            tool = self.tool_for_basket(basketno)
            stype = self.get_cassette_type(basketno)

            if tool != current_tool:
                self.logger.warning("Changing tool from %s to %s" %
                                    (current_tool, tool))
                changing_tool = True
            else:
                changing_tool = False

            # we should now check basket type on diffr to see if tool is different...
            # then decide what to do

            if shifts is None:
                xshift, yshift, zshift = ["0", "0", "0"]
            else:
                xshift, yshift, zshift = map(str, shifts)

            # prepare argin values
            argin = [
                str(tool),
                str(lid),
                str(sample),
                str(stype),
                "0",
                xshift,
                yshift,
                zshift]

            if tool == 2:
                read_barcode = self.read_datamatrix and \
                               self._cmdChainedLoadBarcode is not None
            else:
                if self.read_datamatrix:
                    self.logger.error("Reading barcode only possible with spine pucks")
                read_barcode = False

            if loaded_ht == -1:  # has a loaded but it is not an HT

                if changing_tool:
                    raise Exception(
                        "This operation requires a tool change. You should unload"
                        "sample first")

                if read_barcode:
                    self.logger.warning(
                        "Chained load sample (barcode), sending to cats: %s" % argin)
                    cmd_ok = self._executeServerTask(
                        self._cmdChainedLoadBarcode, argin, waitsafe=True)
                else:
                    self.logger.warning("Chained load sample, sending to cats: %s"
                        % argin)
                    cmd_ok = self._executeServerTask(
                        self._cmdChainedLoad, argin, waitsafe=True)
            elif loaded_ht == 0:
                if read_barcode:
                    self.logger.warning("Load sample (barcode), sending to cats: %s"
                        % argin)
                    cmd_ok = self._executeServerTask(
                        self._cmdLoadBarcode, argin, waitsafe=True)
                else:
                    self.logger.warning("Load sample, sending to cats:  %s" % argin)
                    cmd_ok = self._executeServerTask(
                        self._cmdLoad, argin, waitsafe=True)

        # At this point, due to the waitsafe, we can be sure that the robot has left RI2 and will not return

        if not cmd_ok:
            self.logger.info("Load Command failed on device server")
        elif self.auto_prepare_diff and not changing_tool:
            self.logger.info(
                "AUTO_PREPARE_DIFF (On) sample changer is in safe state... "
                "preparing diff now")
            allok, msg = self._check_coherence()
            if allok:
                self.diff_send_sampleview()
                self.logger.info("Restoring detector distance")
                self.restore_detdist_position()
                self._wait_phase_done('SAMPLE',timeout=10)
            else:
                # Now recover failed put for double gripper
                # : double should be in soak, single should be ??
                #isPathRunning amd cats_idle dont work for tool 5, becuase cats stays in running state after failed put. 
                #_wait_cats_home followed by immediate abort fails because double tool passed through home on the way to soak
                # time.sleep(5) fails because of a possible dry for double 
                # When doing a dry, CATS passes through home, so a double wait_cats_home is necessary, with a time.sleep of a couple of seconds in between so CATS starts drying
                # An alternative is to abort at arriving home, clear memeory and move to soak
                logging.getLogger('user_level_log').error( 'There was a problem loading your sample, please wait for the system to recover' )
                self._wait_cats_home(10) # wait for robot to return from diff
                time.sleep( 5 ) # give it time to move, if it goes for a dry, the _chnNBSoakings is set to 0
                #self.logger.info("self._chnNBSoakings  %d " % self._chnNBSoakings.getValue() )
                if self.get_current_tool() == TOOL_DOUBLE_GRIPPER: 
                    if self._chnNBSoakings.getValue() == 0: 
                        self.logger.info("A dry will now be done, waiting %d seconds" % DOUBLE_GRIPPER_DRY_WAIT_TIME)
                        time.sleep( DOUBLE_GRIPPER_DRY_WAIT_TIME ) # long timeout because of possible dry of the double gripper
                    else: 
                        #self.logger.info("no dry, waiting 3 seconds" )
                        time.sleep( 3 ) # allow the gripper time to move on
                if not self._check_incoherent_sample_info()[0] : # this could be replaced by checking return value of _check_coherence, see TODO there
                    # the behaviour of the SPINE gripper is different when failing put or when failing get. For put, it does a dry, for get, it doesnt
                    if self.get_current_tool() == TOOL_SPINE: 
                        time.sleep( 16 )
                    self.recover_cats_from_failed_put()
                    msg = "Your sample was NOT loaded! Click OK to recover, please make sure your sample is there"
                else:
                    self._doRecoverFailure()
                    msg = "The CATS device indicates there was a problem in unmounting the sample, click ok to recover from a Fix Fail Get"
                self._updateState()
                raise Exception( msg )
                
        else:
            self.logger.info(
                "AUTO_PREPARE_DIFF (Off) sample loading done / or changing tool (%s)" %
                changing_tool)

        # load commands are executed until path is safe. Then we have to wait for
        # path to be finished
        if not self._chnCollisionSensorOK.getValue(): 
            self._updateState()
            raise Exception ("The robot had a collision, call your LC or floor coordinator")
        # Does this do anything? In the trajectory it is already specified a wait safe...
        self._waitDeviceSafe()
        # self._waitDeviceReady()

    def _doUnload(self, sample_slot=None, shifts=None):
        """
        Unloads a sample from the diffractometer.
        Overides Cats90 method.

        @sample_slot:
        @shifts: mounting position
        """
        if not self._chnPowered.getValue():
            try: self._cmdPowerOn()  # try switching power on
            except Exception as e:
                raise Exception(e)

        ret = self.diff_send_transfer()

        if ret is False:
            self.logger.error(
                "Supervisor cmd transfer phase returned an error.")
            return

        shifts = self._get_shifts()

        if sample_slot is not None:
            self._doSelect(sample_slot)

        loaded_ht = self.is_loaded_ht()

        if shifts is None:
            xshift, yshift, zshift = ["0", "0", "0"]
        else:
            xshift, yshift, zshift = map(str, shifts)

        loaded_lid = self._chnLidLoadedSample.getValue()
        loaded_num = self._chnNumLoadedSample.getValue()

        if loaded_lid == -1:
            self.logger.warning("Unload sample, no sample mounted detected")
            return

        loaded_basket, loaded_sample = self.lidsample_to_basketsample(
            loaded_lid, loaded_num)

        tool = self.tool_for_basket(loaded_basket)

        argin = [str(tool), "0", xshift, yshift, zshift]

        self.logger.warning("Unload sample, sending to cats:  %s" %
                            argin)
        if loaded_ht == 1:
            cmd_ret = self._executeServerTask(self._cmdUnloadHT, argin, waitsafe=True)
        else:
            cmd_ret = self._executeServerTask(self._cmdUnload, argin, waitsafe=True)

        # At this point, due to the waitsafe, we can be sure that the robot has left RI2 and will not return

        allok = self._check_coherence()[0]

    def _doAbort(self):
        """
        Aborts a running trajectory on the sample changer.

        :returns: None
        :rtype: None
        """
        if self.super_abort_cmd is not None:
            self.super_abort_cmd()  # stops super
        self._cmdAbort()
        self._updateState()  # remove software flags like Loading.. reflects current hardware state

    def _check_coherence(self):
        
        sampinfobool, sampinfomessage = self._check_incoherent_sample_info()
        unknownsampbool, unknownsampmessage = self._check_unknown_sample_presence()
        msg = sampinfomessage + unknownsampmessage
        return ( sampinfobool and unknownsampbool ), msg

    def _check_unknown_sample_presence(self):
        
        detected = self._chnSampleIsDetected.getValue()
        loaded_lid = self._chnLidLoadedSample.getValue()
        loaded_num = self._chnNumLoadedSample.getValue()
        #self.logger.debug("detected %s, type detected %s, loaded_lid %d, loaded_num %d, loaded_num type %s" % ( str(detected), type(detected), loaded_lid, loaded_num, type(loaded_num)  ) )
        #self.logger.debug("-1 in [loaded_lid, loaded_num] %s, detected %s" % ( -1 in [loaded_lid, loaded_num], detected ) )
        #self.logger.debug("-1 in [loaded_lid, loaded_num] and detected: %s" % ( -1 in [loaded_lid, loaded_num] and detected ) )


        if -1 in [loaded_lid, loaded_num] and detected:
            return False, "Sample detected on Diffract. but there is no info about it"

        return True, ""

    def _check_incoherent_sample_info(self):
        """
          Check for sample info in CATS but no physically mounted sample
           (Fix failed PUT)
          Returns False in case of incoherence, True if all is ok
        """
        #self.logger.debug('self._chnSampleIsDetected %s' % self._chnSampleIsDetected.getValue() )
        detected = self._chnSampleIsDetected.getValue()
        loaded_lid = self._chnLidLoadedSample.getValue()
        loaded_num = self._chnNumLoadedSample.getValue()
        #self.logger.debug("detected %s, loaded_lid %d, loaded_num %d" % ( str(detected), loaded_lid, loaded_num ) )

        if not detected and not ( -1 in [loaded_lid, loaded_num] ):
            return False, "There is info about a sample but it is not detected on the diffract."

        return True, ""


    def _get_shifts(self):
        """
        Get the mounting position from the Diffractometer DS.

        @return: 3-tuple
        """
        if self.shifts_channel is not None:
            shifts = self.shifts_channel.getValue()
        else:
            shifts = None
        return shifts

    # TODO: fix return type
    def is_ht_sample(self, address):
        """
        Returns is sample address belongs to hot tool basket.

        @address: sample address
        @return: int or boolean
        """
        basket, sample = address.split(":")
        try:
            if int(basket) >= 100:
                return int(sample)
            else:
                return False
        except Exception as e:
            self.logger.debug("Cannot identify sample in hot tool")
            return False

    def tool_for_basket(self, basketno):
        """
        Returns the tool corresponding to the basket.

        @basketno: basket number
        @return: int
        """
        if basketno == 100:
            return TOOL_SPINE

        return Cats90.tool_for_basket(self, basketno)

    def is_loaded_ht(self):
        """
           1 : has loaded ht
           0 : nothing loaded
          -1 : loaded but not ht
        """
        sample_lid = self._chnLidLoadedSample.getValue()

        if self.hasLoadedSample():
            if sample_lid == 100:
                return 1
            else:
                return -1
        else:
            return 0

    def _doReset(self):
        """
          Called when user pushes "Fix fail PUT" button
          Overrides the _doReset in CatsMaint, adding checks whether calling this method is justified
        """
        self.recover_cats_from_failed_put()

    def recover_cats_from_failed_put(self):
        """
           Deletes sample info on diff, but should retain info of samples on tools, eg when doing picks
           TODO: tool2 commands are not working, eg SampleNumberInTool2
        """
        self.logger.debug("ALBACats recovering from failed put. Failed put is %s" % str( self._check_incoherent_sample_info() ) )

        if not self._check_incoherent_sample_info()[0]:
            self._cmdAbort()
            savelidsamptool = self._chnLidSampleOnTool.getValue()
            savenumsamptool = self._chnNumSampleOnTool.getValue()
            #savelidsamptool2 = self._chnLidSampleOnTool2() # Not implemented yet
            #savenumsamptool2 = self._chnNumSampleOnTool2() # Not implemented yet
            self._cmdClearMemory()
            if not -1 in [savelidsamptool, savenumsamptool ]:
                basketno, bsampno = self.lidsample_to_basketsample(savelidsamptool,savenumsamptool)
                argin = [ str(savelidsamptool), str(savenumsamptool), str( self.get_cassette_type( basketno ) ) ]
                self.logger.debug("ALBACats recover from failed put. Sending to robot %s" % argin )
                cmdok = self._executeServerTask( self._cmdSetTool, argin )
            #if not -1 in [savelidsamptool2, savenumsamptool2 ]:
            #   basketno, bsampno = self.lidsample_to_basketsample(savelidsamptool2,savenumsamptool2) # Not implemented yet
            #   argin = [ str(savelidsamptool2), str(savenumsamptool2), str(self.get_cassette_type(basketno)) ]
            #   self._executeServerTask( self._cmdSetTool2, argin )
        else: raise Exception("The conditions of the beamline do not fit a failed put situation, "
                                "Fixed failed PUT is not justified. Find another solution.")

    def _doRecoverFailure(self):
        """
          Called when user pushes "Fix fail GET" button
          Overrides the _doRecoverFailure in CatsMaint, adding checks whether calling this method is justified
        """
        self.logger.debug("ALBACats recovering from failed get")
        self.recover_cats_from_failed_get()

    def recover_cats_from_failed_get(self):
        """
           Deletes sample info on diff, but should retain info of samples on tools, eg when doing picks
           TODO: tool2 commands are not working, eg SampleNumberInTool2
        """
        if not self._check_unknown_sample_presence()[0]:
            self._cmdRecoverFailure()
        else: raise Exception("The conditions of the beamline do not fit a failed get situation, "
                                "Fixed failed GET is not justified. Find another solution.")
        
        
    def _executeServerTask(self, method, *args, **kwargs):
        """
        Executes a task on the CATS Tango device server
        ALBA: added collision detection while waiting for safe

        :returns: None
        :rtype: None
        """
        self._waitDeviceReady(3.0)
        try:
            task_id = method(*args)
        except:
            import traceback
            self.logger.debug("ALBACats exception while executing server task")
            self.logger.debug(traceback.format_exc())
            task_id = None
            raise Exception("The command could not be sent to the robot, check its state.")
            #TODO: why not return with an Exception here to inform there is a problem with the CATS?

        waitsafe = kwargs.get('waitsafe',False)
        logging.getLogger("HWR").debug("Cats90. executing method %s / task_id %s / waiting only for safe status is %s" % (str(method), task_id, waitsafe))
        
        # What does the first part of the if do? It's not resetting anything...
        ret=None
        if task_id is None: #Reset
            while self._isDeviceBusy():
                gevent.sleep(0.1)
            return False
        else:
            # introduced wait because it takes some time before the attribute PathRunning is set
            # after launching a transfer
            time.sleep(6.0)
            while True:
                if waitsafe:
                    if self.pathSafe():
                        logging.getLogger("HWR").debug("Cats90. server execution polling finished as path is safe")
                        break
                elif not self.pathRunning():
                        logging.getLogger("HWR").debug("Cats90. server execution polling finished as path is not running")
                        break
                elif not self._chnCollisionSensorOK.getValue(): 
                    self._updateState()
                    raise Exception ("The robot had a collision, call your LC or floor coordinator")

                if not self._check_unknown_sample_presence()[0] and not self._chnIsCatsRI1.getValue():
                    break
                gevent.sleep(0.1)            
            ret = True
        return ret


def test_hwo(hwo):
    hwo._updateCatsContents()
    print("Is path running? ", hwo.isPathRunning())
    print("Loading shifts:  ", hwo._get_shifts())
    print("Sample on diffr :  ", hwo.cats_sample_on_diffr())
    print("Baskets :  ", hwo.basket_presence)
    print("Baskets :  ", hwo.getBasketList())
    if hwo.hasLoadedSample():
        print("Loaded is: ", hwo.getLoadedSample().getCoords())
    print("Is mounted sample: ", hwo.is_mounted_sample((1, 1)))
