"""
[Name] Harvester

[Description]
Harvester mockup is use as a replacement of the Dewar sample storage
This hardware object is use in couple with a Sample changer .
and it is compatible with the Crystal Direct Harvester 3.
It has some functionalities, like Harvest Sample, .

[Commands]

 - getSampleList : Get list of available sample from Harvester
 - Harvest : load sample from Harvester

-----------------------------------------------------------------------
"""
import gevent
import logging

from mxcubecore.BaseHardwareObjects import HardwareObject


class HarvesterMockup(HardwareObject):
    """Harvester functionality

    The Harvester Class consists of methods executing exporter commands
    to communicate with the Crystal Direct Harvester Machine

    """

    __TYPE__ = "Harvester"

    def __init__(self, name):
        super().__init__(name)
        self.timeout = 3  # default timeout

        # Internal variables -----------
        self.calibrate_state = False
        self.room_temperature_mode = False

    def init(self):
        """Init"""
        self.exporter_addr = self.get_property("exporter_address")

    def set_calibrate_state(self, state):
        """Set Calibration state

        Args:
        state (bool) : Whether the a calibration was perform
        """

        self.calibrate_state = state

    def _wait_ready(self, timeout=None):
        """Wait Harvester to be ready

        Args:
        (timeout) : Whether to wait for a amount of time
        None means wait forever timeout <=0 use default timeout
        """
        if timeout is not None and timeout <= 0:
            timeout = self.timeout

        err_msg = "Timeout waiting for Harvester to be ready"

        with gevent.Timeout(timeout, RuntimeError(err_msg)):
            while not self._ready():
                logging.getLogger("user_level_log").info(
                    "Waiting Harvester to be Ready"
                )
                gevent.sleep(3)

    def _wait_sample_transfer_ready(self, timeout=None):
        """Wait Harvester to be ready to transfer a sample

        Args:
        timeout (second) : Whether to wait for a amount of time
        None means wait forever timeout <=0 use default timeout
        """
        if timeout is not None and timeout <= 0:
            timeout = self.timeout

        err_msg = "Timeout waiting for Harvester to be ready to transfer"

        with gevent.Timeout(timeout, RuntimeError(err_msg)):
            while not self._ready_to_transfer():
                logging.getLogger("user_level_log").info(
                    "Waiting Harvester to be ready to transfer"
                )
                gevent.sleep(3)

    def _execute_cmd_exporter(self, cmd, *args, **kwargs):
        """Wait Harvester to be ready to transfer a sample

        Args:
        cmd (string) : command type
        args, kwargs (string): commands arguments, and  command or attribute

        return : respond
        """
        ret = None
        if args:
            args_str = "%s" % "\t".join(map(str, args))
        if kwargs.pop("command", None):
            exp_cmd = self.add_command(
                {
                    "type": "exporter",
                    "exporter_address": self.exporter_addr,
                    "name": "%s" % cmd,
                },
                "%s" % cmd,
            )
            if args:
                ret = exp_cmd(args_str)
            else:
                ret = exp_cmd()
        if kwargs.pop("attribute", None):
            exp_attr = self.add_channel(
                {
                    "type": "exporter",
                    "exporter_address": self.exporter_addr,
                    "name": "%s" % cmd,
                },
                "%s" % cmd[3:],
            )
            if cmd.startswith("get"):
                return exp_attr.get_value()
            if cmd.startswith("set"):
                ret = exp_attr.set_value(args_str)

        return ret

    # ---------------------- State --------------------------------

    def get_state(self):
        """Get the Harvester State

        Return (str):  state "Ready, Running etc.."
        """
        return "Ready"

    def get_status(self):
        """Get the Harvester Status

        Return (str):  Status
        """
        # return self._execute_cmd_exporter("getStatus", attribute=True)
        return "Ready"

    def _ready(self):
        """Same as Get Harvester State

        Return (bool):  True if Harvester is Ready otherwise False
        """
        return True

    def _busy(self):
        """Same as Get Harvester State

        Return (bool):  True if Harvester is not Ready otherwise False
        """
        return not self._ready()

    def _ready_to_transfer(self):
        """Same as Get Harvester Status

        Return (bool):  True if Harvester is Waiting Sample Transfer otherwise False
        """
        # return self._execute_cmd_exporter("getStatus", attribute=True) == "Waiting Sample Transfer"
        return "Waiting Sample Transfer"

    def get_samples_state(self):
        """Get the Harvester Sample State

        Return (List):  list of crystal state "waiting_for_transfer, Running etc.."
        """
        # return self._execute_cmd_exporter("getSampleStates", command=True)
        return "Ready"

    def get_current_crystal(self):
        """Get the Harvester current harvested crystal

        Return (str): crystal uuid
        """
        # return self._execute_cmd_exporter("getCurrentSampleID", attribute=True)
        return None

    def is_crystal_harvested(self, crystal_uuid):
        """Same as Get Harvester Status

        args: the crystal uuid

        Return (bool):  True if the crystal is the current harvested crystal
        """
        res = False
        in_list = crystal_uuid in self.get_crystal_uuids()
        if in_list:
            Current_SampleID = self.get_current_crystal()
            if crystal_uuid == Current_SampleID:
                res = True
        return res

    def current_crystal_state(self, crystal_uuid):
        """Wait Harvester to be ready to transfer a sample

        Args:
        state (str) : Crystal uuid

        Return (str):  State of the crystal uuid
        """
        sample_states = self.get_samples_state()
        crystal_uuids = self.get_crystal_uuids()

        for index, x_tal in enumerate(crystal_uuids):
            if crystal_uuid == x_tal:
                return sample_states[index]

        return None

    def check_crystal_state(self, crystal_uuid):
        """Check wether if a Crystal is in pending_and_current or not

        Args (str) : Crystal uuid

        Return (str):  status of the crystal_uuid pending / current
        """
        sample_states = self.get_samples_state()
        crystal_uuids = self.get_crystal_uuids()

        for index, x_tal in enumerate(crystal_uuids):
            if crystal_uuid == x_tal and sample_states[index] == "waiting_for_transfer":
                return "pending_and_current"
            elif (
                crystal_uuid != x_tal and sample_states[index] == "waiting_for_transfer"
            ):
                return "pending_not_current"
            else:
                return None

    def get_crystal_uuids(self):
        """Get the Harvester Sample List uuid

        Return (List):  list of crystal by uuid from the current processing plan"
        """
        # harvester_crystal_list = self._execute_cmd_exporter("getSampleList",  attribute=True)
        return []

    def get_sample_names(self):
        """Get the Harvester Sample List Name

        Return (List):  list of crystal by names from the current processing plan"
        """
        # harvester_sample_names = self._execute_cmd_exporter("getSampleNames", attribute=True)
        return []

    def get_crystal_images_urls(self, crystal_uuid):
        """Get the Harvester Sample List Images

        Args (str) : Crystal uuid

        Return (List):  list of crystal by image_url from current processing plan"
        """
        # crystal_images_url = self._execute_cmd_exporter("getImageURL", crystal_uuid, command=True)
        return []

    def get_sample_acronyms(self):
        """Get the Harvester Sample List by Acronyms

        Return (List):  list of crystal by Acronyms from the current processing plan"
        """
        # harvester_sample_acronyms = self._execute_cmd_exporter("getSampleAcronyms", attribute=True)
        return []

    # ------------------------------------------------------------------------------------

    def abort(self):
        """Send Abort command
        Abort any current Harvester Actions
        """
        # return self._execute_cmd_exporter("abort", command=True)
        return None

    def harvest_crystal(self, crystal_uuid):
        """Harvester crystal

        Args (str) : Crystal uuid
        """
        # return self._execute_cmd_exporter("harvestCrystal", crystal_uuid, command=True)
        return False

    def transfer_sample(self):
        """Transfer the current Harvested Crystal"""
        # return self._execute_cmd_exporter("startTransfer", command=True)
        return False

    def trash_sample(self):
        """Trash the current Harvested Crystal"""
        # return self._execute_cmd_exporter("trashSample", command=True)
        return False

    # -----------------------------------------------------------------------------

    def load_plate(self, plate_id):
        """Load a plate from Harvester to MD

        Args (str) : Plate ID
        """
        # return self._execute_cmd_exporter("loadPlate", plate_id, command=True)
        pass

    def get_plate_id(self):
        """Wait Harvester to be ready to transfer a sample

        Args:
        Return (str) : current Plate ID
        """
        # return self._execute_cmd_exporter("getPlateID", attribute=True)
        return ""

    def get_image_target_x(self, crystal_uuid):
        """Get the crystal images position x

        Args (str) : Crystal uuid

        Return (float):  Crystal x coordinate in plate
        """
        # return self._execute_cmd_exporter("getImageTargetX", crystal_uuid, command=True)
        return ""

    def get_image_target_y(self, crystal_uuid):
        """Wait Harvester to be ready to transfer a sample

        Args:
        state (timeout) : Whether to wait for a amound of time
        None means wait forever timeout <=0 use default timeout
        """
        # return self._execute_cmd_exporter("getImageTargetY", crystal_uuid, command=True)
        return ""

    def get_room_temperature_mode(self):
        # return self._execute_cmd_exporter("getRoomTemperatureMode", attribute=True)
        return True

    def set_room_temperature_mode(self, value):
        """Set Harvester temperature mode

        Args: (bool) set room temperature when true
        """
        # self._execute_cmd_exporter("setRoomTemperatureMode", value, command=True)
        print("setting HA Room temperature to: %s" % value)
        return self.get_room_temperature_mode()

    # -------------------- Calibrate  Drift Shape offset ----------------------------

    def get_last_sample_drift_offset_x(self):
        """Sample Offset X position when drifted
        Return (float):  last pin drift offset x
        """
        # last_sample_drift_offset_x = self._execute_cmd_exporter("getLastSampleDriftOffsetX", attribute=True)
        pass

    def get_last_sample_drift_offset_y(self):
        """Sample Offset Y position when drifted
        Return (float):  last pin drift offset y
        """
        # last_sample_drift_offset_y = self._execute_cmd_exporter("getLastSampleDriftOffsetY", attribute=True)
        pass

    def get_last_sample_drift_offset_z(self):
        """Sample Offset Z position when drifted
        Return (float):  last pin drift offset z
        """
        # pin_last_drift_offset_z = self._execute_cmd_exporter("getLastSampleDriftOffsetZ", attribute=True)
        pass

    # ---------------------- Calibrate Cut Shape offset----------------------------

    def get_last_pin_cut_shape_offset_x(self):
        """Pin shape Offset x position when
        Return (float):  last pin cut shape offset x
        """
        # pin_last_cut_shape_offset_x = self._execute_cmd_exporter("getLastSampleCutShapeOffsetX", attribute=True)
        pass

    def get_last_pin_cut_shape_offset_y(self):
        """Pin shape Offset Y position when
        Return (float):  last pin cut shape offset y
        """
        # pin_last_cut_shape_offset_y = self._execute_cmd_exporter("getLastSampleCutShapeOffsetY", attribute=True)
        pass

    # =============== Pin / Calibration -----------------------------

    def load_calibrated_pin(self):
        """Start Pin Calibration Procedure"""
        # return self._execute_cmd_exporter("loadCalibratedPin", command=True)
        pass

    def store_calibrated_pin(self, x, y, z):
        """Store x , y , z offsets position after calibration procedure

        Args: (float) x, y, z offsets
        """
        # return self._execute_cmd_exporter("storePinToBeamOffset", x , y , z,  command=True)
        pass

    def get_calibrated_pin_offset(self):
        """Get Stored x , y , z offsets position after calibration procedure

        return: (float) x, y, z offsets
        """
        # pin_to_beam_offset = self._execute_cmd_exporter("getPinToBeamOffset", command=True)
        pass

    def get_number_of_available_pin(self):
        """Get number of available pin

        return: (Integer)
        """
        # return self._execute_cmd_exporter("getNbRemainingPins", command=True)
        return 1

    def get_offsets_for_sample_centering(self):
        pin_to_beam = tuple(self.get_calibrated_pin_offset())

        sample_drift_x = float(self.get_last_sample_drift_offset_x())
        sample_drift_y = float(self.get_last_sample_drift_offset_y())
        sample_drift_z = -float(self.get_last_sample_drift_offset_z())

        pin_cut_shape_x = float(self.get_last_pin_cut_shape_offset_x())
        pin_cut_shape_y = float(self.get_last_pin_cut_shape_offset_y())

        phiy_offset = sample_drift_x - pin_cut_shape_x + float(pin_to_beam[1])

        centringFocus = sample_drift_z + float(pin_to_beam[0])

        centringTableVertical = sample_drift_y - pin_cut_shape_y + float(pin_to_beam[2])

        return (phiy_offset, centringFocus, centringTableVertical)
