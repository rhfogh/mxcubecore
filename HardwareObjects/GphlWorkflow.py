#! /usr/bin/env python
# encoding: utf-8

"""Workflow runner, interfacing to external workflow engine
using Abstract Beamline Interface messages

License:

This file is part of MXCuBE.

MXCuBE is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

MXCuBE is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with MXCuBE.  If not, see <https://www.gnu.org/licenses/>.
"""
from __future__ import division, absolute_import
from __future__ import print_function, unicode_literals

import logging
import uuid
import time
import datetime
import os
import math
import subprocess
import socket
import f90nml

import gevent
import gevent.event
import gevent._threading
from dispatcher import dispatcher

import api

import ConvertUtils
from HardwareRepository.BaseHardwareObjects import HardwareObject

from HardwareRepository.HardwareObjects import queue_model_objects
from HardwareRepository.HardwareObjects import queue_model_enumerables
from HardwareRepository.HardwareObjects.queue_entry import QUEUE_ENTRY_STATUS, QueueAbortedException

from HardwareRepository.HardwareObjects import GphlMessages

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict


__copyright__ = """ Copyright © 2016 - 2019 by Global Phasing Ltd. """
__license__ = "LGPLv3+"
__author__ = "Rasmus H Fogh"

# Used to pass to priorInformation when no wavelengths are set (DiffractCal)
DUMMY_WAVELENGTH = 999.999


class GphlWorkflow(HardwareObject, object):
    """Global Phasing workflow runner.
    """

    STATES = GphlMessages.States

    TEST_SAMPLE_PREFIX = "emulate-"

    def __init__(self, name):
        super(GphlWorkflow, self).__init__(name)
        self._state = self.STATES.OFF

        # Needed to allow methods to put new actions on the queue
        # And as a place to get hold of other objects
        self._queue_entry = None

        # Current data colelction group. Different for characterisation and collection
        self._data_collection_group = None

        # event to handle waiting for parameter input
        self._return_parameters = None

        # Queue to read messages from GphlConnection
        self._workflow_queue = None

        # Message - processing function map
        self._processor_functions = {}

        # Subprocess names to track which subprocess is getting info
        self._server_subprocess_names = {}

        # Rotation axis role names, ordered from holder towards sample
        self.rotation_axis_roles = []

        # Translation axis role names
        self.translation_axis_roles = []

        # Switch for 'move-to-fine-zoom' message for translational calibration
        self._use_fine_zoom = False

        # Configurable file paths
        self.file_paths = {}

        # RF no longer needed
        # #GB: globalize these from setup_data_collection() only to get stuff down to collect_data()
        # self._last_queryed_collection_strategy_parameters = None

    def _init(self):
        super(GphlWorkflow, self)._init()

    def init(self):
        super(GphlWorkflow, self).init()

        # Set up processing functions map
        self._processor_functions = {
            "String": self.echo_info_string,
            "SubprocessStarted": self.echo_subprocess_started,
            "SubprocessStopped": self.echo_subprocess_stopped,
            "RequestConfiguration": self.get_configuration_data,
            "GeometricStrategy": self.setup_data_collection,
            "CollectionProposal": self.collect_data,
            "ChooseLattice": self.select_lattice,
            "RequestCentring": self.process_centring_request,
            "PrepareForCentring": self.prepare_for_centring,
            "ObtainPriorInformation": self.obtain_prior_information,
            "WorkflowAborted": self.workflow_aborted,
            "WorkflowCompleted": self.workflow_completed,
            "WorkflowFailed": self.workflow_failed,
        }

    def setup_workflow_object(self):
        """Necessary as this set-up cannot be done at init,
        when the hwobj are still incomplete. Must be called externally
        TODO This still necessary?"""

        # Set standard configurable file paths
        file_paths = self.file_paths
        ss0 = api.gphl_connection.software_paths["gphl_beamline_config"]
        file_paths["gphl_beamline_config"] = ss0
        file_paths["transcal_file"] = os.path.join(ss0, "transcal.nml")
        file_paths["diffractcal_file"] = os.path.join(ss0, "diffractcal.nml")
        file_paths["instrumentation_file"] = fp0 = os.path.join(
            ss0, "instrumentation.nml"
        )
        instrument_data = f90nml.read(fp0)["sdcp_instrument_list"]
        self.rotation_axis_roles = instrument_data["gonio_axis_names"]
        self.translation_axis_roles = instrument_data["gonio_centring_axis_names"]
        detector = api.detector
        if "Mockup" in detector.__class__.__name__:
            # We are in mock  mode
            # - set detector centre to match instrumentaiton.nml
            # NB htis sould be done with isinstnce, but that seems to fail,
            # probably because of import path mix-ups.
            detector._set_beam_centre(
                (instrument_data["det_org_x"], instrument_data["det_org_y"])
            )

    def shutdown(self):
        """Shut down workflow and connection. Triggered on program quit."""
        workflow_connection = api.gphl_connection
        if workflow_connection is not None:
            workflow_connection.workflow_ended()
            workflow_connection.close_connection()

    def get_available_workflows(self):
        """Get list of workflow description dictionaries."""

        # TODO this could be cached for speed

        result = OrderedDict()
        if self.hasObject("workflow_properties"):
            properties = self["workflow_properties"].getProperties().copy()
        else:
            properties = {}
        if self.hasObject("invocation_properties"):
            invocation_properties = self["invocation_properties"].getProperties().copy()
        else:
            invocation_properties = {}

        if self.hasObject("all_workflow_options"):
            all_workflow_options = self["all_workflow_options"].getProperties().copy()
            if "beamline" in all_workflow_options:
                pass
            elif api.gphl_connection.hasObject("ssh_options"):
                # We are running workflow through ssh - set beamline url
                all_workflow_options["beamline"] = "py4j:%s:" % socket.gethostname()
            else:
                all_workflow_options["beamline"] = "py4j::"
        else:
            all_workflow_options = {}

        acq_workflow_options = all_workflow_options.copy()
        acq_workflow_options.update(self["acq_workflow_options"].getProperties())
        # Add options for target directories:
        process_root = api.session.get_base_process_directory()
        acq_workflow_options["appdir"] = process_root

        mx_workflow_options = acq_workflow_options.copy()
        mx_workflow_options.update(self["mx_workflow_options"].getProperties())

        for wf_node in self["workflows"]:
            name = wf_node.name()
            strategy_type = wf_node.getProperty("strategy_type")
            wf_dict = {
                "name": name,
                "strategy_type": strategy_type,
                "application": wf_node.getProperty("application"),
                "documentation": wf_node.getProperty("documentation", default_value=""),
                "interleaveOrder": wf_node.getProperty(
                    "interleave_order", default_value=""
                ),
            }
            result[name] = wf_dict

            if strategy_type.startswith("transcal"):
                wf_dict["options"] = dd0 = all_workflow_options.copy()
                if wf_node.hasObject("options"):
                    dd0.update(wf_node["options"].getProperties())
                    relative_file_path = dd0.get("file")
                    if relative_file_path is not None:
                        # Special case - this option must be modified before use
                        dd0["file"] = os.path.join(
                            self.file_paths["gphl_beamline_config"], relative_file_path
                        )

            elif strategy_type.startswith("diffractcal"):
                wf_dict["options"] = dd0 = acq_workflow_options.copy()
                if wf_node.hasObject("options"):
                    dd0.update(wf_node["options"].getProperties())

            else:
                wf_dict["options"] = dd0 = mx_workflow_options.copy()
                if wf_node.hasObject("options"):
                    dd0.update(wf_node["options"].getProperties())

            beam_energy_tags = wf_node.getProperty("beam_energy_tags")
            if beam_energy_tags:
                 wf_dict["beam_energy_tags"] = beam_energy_tags.strip().split()

            wf_dict["properties"] = dd0 = properties.copy()
            if wf_node.hasObject("properties"):
                dd0.update(wf_node["properties"].getProperties())
            # Program-specific properties
            devmode = dd0.get("co.gphl.wf.devMode")
            if devmode and devmode[0] not in "fFnN":
                # We are in developer mode. Add parameters
                dd0["co.gphl.wf.stratcal.opt.--strategy_type"] = strategy_type

            wf_dict["invocation_properties"] = dd0 = invocation_properties.copy()
            if wf_node.hasObject("invocation_properties"):
                dd0.update(wf_node["invocation_properties"].getProperties())
        #
        return result

    def get_state(self):
        return self._state

    def set_state(self, value):
        if value in self.STATES:
            self._state = value
            self.emit("stateChanged", (value,))
        else:
            raise RuntimeError("GphlWorkflow set to invalid state: s" % value)

    # # NB This was called only from GphlWorkflowQueueEntry.stop()
    # # Abort from data dialog abort buttons go directly to the execute() queue
    # # Abort from the Queue stop command stop execution and call post_execute
    # # Abort originated in Java workflow send back an abort message.
    # # As of now there is no need for this function
    # def abort(self, message=None):
    #     logging.getLogger("HWR").info("MXCuBE aborting current GPhL workflow")


    def pre_execute(self, queue_entry):

        self._queue_entry = queue_entry

        if self.get_state() == self.STATES.OFF:
            api.gphl_connection.open_connection()
            self.set_state(self.STATES.READY)

    def execute(self):

        # Start execution of a new workflow
        if self.get_state() != self.STATES.READY:
            # TODO Add handling of potential conflicts.
            # NBNB GPhL workflow cannot have multiple users
            # unless they use separate persistence layers
            raise RuntimeError(
                "Cannot execute workflow - GphlWorkflow HardwareObject is not idle"
            )

        if api.gphl_connection is None:
            raise RuntimeError(
                "Cannot execute workflow - GphlWorkflowConnection not found"
            )

        # try:
        self.set_state(self.STATES.BUSY)
        self._workflow_queue = gevent._threading.Queue()

        # Fork off workflow server process
        api.gphl_connection.start_workflow(
            self._workflow_queue, self._queue_entry.get_data_model()
        )

        while True:
            if self._workflow_queue is None:
                # We can only get that value if we have already done post_eecute
                # but the mechanics of aborting means we conme back
                # Stop further processing here
                raise QueueAbortedException("Aborting...", self)

            tt0  = self._workflow_queue.get()
            if tt0 is StopIteration:
                logging.getLogger("HWR").debug(
                    "GPhL queue StopIteration"
                )
                break

            message_type, payload, correlation_id, result_list = tt0
            func = self._processor_functions.get(message_type)
            if func is None:
                logging.getLogger("HWR").error(
                    "GPhL message %s not recognised by MXCuBE. Terminating...",
                    message_type,
                )
                break
            else:
                logging.getLogger("HWR").info(
                    "GPhL queue processing %s", message_type
                )
                response = func(payload, correlation_id)
                if result_list is not None:
                    result_list.append((response, correlation_id))

    def post_execute(self):
        """
        The workflow has finished, sets the state to 'READY'
        """

        self._queue_entry = None
        self._data_collection_group = None
        self.set_state(self.STATES.READY)
        self._server_subprocess_names.clear()
        self._workflow_queue = None
        if api.gphl_connection is not None:
            api.gphl_connection.workflow_ended()

    def _add_to_queue(self, parent_model_obj, child_model_obj):
        api.queue_model.add_child(parent_model_obj, child_model_obj)

    # Message handlers:

    def workflow_aborted(self, payload, correlation_id):
        logging.getLogger("user_level_log").warning("GPhL Workflow aborted.")
        self._workflow_queue.put_nowait(StopIteration)

    def workflow_completed(self, payload, correlation_id):
        logging.getLogger("user_level_log").info("GPhL Workflow completed.")
        self._workflow_queue.put_nowait(StopIteration)

    def workflow_failed(self, payload, correlation_id):
        logging.getLogger("user_level_log").warning("GPhL Workflow failed.")
        self._workflow_queue.put_nowait(StopIteration)

    def echo_info_string(self, payload, correlation_id=None):
        """Print text info to console,. log etc."""
        subprocess_name = self._server_subprocess_names.get(correlation_id)
        if subprocess_name:
            logging.info("%s: %s" % (subprocess_name, payload))
        else:
            logging.info(payload)

    def echo_subprocess_started(self, payload, correlation_id):
        name = payload.name
        if correlation_id:
            self._server_subprocess_names[correlation_id] = name
        logging.info("%s : STARTING", name)

    def echo_subprocess_stopped(self, payload, correlation_id):
        try:
            name = self._server_subprocess_names.pop(correlation_id)
        except KeyError:
            name = "Unknown process"
        logging.info("%s : FINISHED", name)

    def get_configuration_data(self, payload, correlation_id):
        return GphlMessages.ConfigurationData(self.file_paths["gphl_beamline_config"])

    def query_collection_strategy(self, geometric_strategy, initial_energy):
        """Display collection strategy for user approval,
        and query parameters needed"""

        data_model = self._queue_entry.get_data_model()

        allowed_widths = geometric_strategy.allowedWidths
        if allowed_widths:
            default_width_index = geometric_strategy.defaultWidthIdx or 0
        else:
            allowed_widths = [
                float(x) for x in self.getProperty("default_image_widths").split()
            ]
            val = allowed_widths[0]
            allowed_widths.sort()
            default_width_index = allowed_widths.index(val)
            logging.getLogger("HWR").info(
                "No allowed image widths returned by strategy - use defaults"
            )

        # NBNB TODO userModifiable

        # NBNB The geometric strategy is only given for ONE beamsetting
        # The strategy is (for now) repeated identical for all wavelengths
        # When this changes, more info will become available

        axis_names = self.rotation_axis_roles

        orientations = OrderedDict()
        strategy_length = 0
        for sweep in geometric_strategy.get_ordered_sweeps():
            strategy_length += sweep.width
            rotation_id = sweep.goniostatSweepSetting.id_
            sweeps = orientations.setdefault(rotation_id, [])
            sweeps.append(sweep)

        relative_sensitivity = data_model.get_relative_rad_sensitivity()

        test_crystal_data, junk = self.get_emulation_crystal_data()
        if test_crystal_data:
            resolution = test_crystal_data.get("res_limit_def")
        else:
            resolution = api.resolution.get_value()

        full_dose_budget = self.get_dose_budget(
            resolution, relative_sensitivity=relative_sensitivity
        )

        if data_model.lattice_selected or "calibration" in data_model.get_type().lower():
            lines = ["%s strategy" % api.gphl_connection.get_workflow_name()]
            lines.extend(("-"*len(lines[0]), ""))
            # Data collection TODO: Use workflow info to distinguish
            beam_energies = OrderedDict()
            energies = [initial_energy, initial_energy + 0.01, initial_energy - 0.01]
            for ii, tag in enumerate(data_model.get_beam_energy_tags()):
                beam_energies[tag] = energies[ii]
            budget_use_fraction = 1.0

        else:
            # Characterisation
            lines = ["Characterisation strategy"]
            lines.extend(("="*len(lines[0]), ""))
            beam_energies = OrderedDict((("Characterisation", initial_energy),))
            budget_use_fraction = data_model.get_characterisation_budget_fraction()

        total_strategy_length = strategy_length * len(beam_energies)
        if len(beam_energies) > 1:
            lines.append(
                "Experiment length: %s * %6.1f°" % (len(beam_energies), strategy_length)
            )
        else:
            lines.append("Experiment length: %6.1f°" % strategy_length)

        for rotation_id, sweeps in orientations.items():
            goniostatRotation = sweeps[0].goniostatSweepSetting
            axis_settings = goniostatRotation.axisSettings
            scan_axis = goniostatRotation.scanAxis
            ss0 = "\nSweep :     " + ",  ".join(
                "%s= %6.1f°" % (x, axis_settings.get(x))
                for x in axis_names
                if x != scan_axis
            )
            ll1 = []
            for sweep in sweeps:
                start = sweep.start
                width = sweep.width
                ss1 = "%s= %6.1f°,  sweep width= %6.1f°" % (
                    scan_axis,
                    start,
                    width,
                )
                ll1.append(ss1)
            lines.append(ss0 + ",  " + ll1[0])
            spacer = " " * (len(ss0) + 2)
            for ss1 in ll1[1:]:
                lines.append(spacer + ss1)

        info_text = "\n".join(lines)

        acq_parameters = api.beamline_setup.get_default_acquisition_parameters()
        # For now return default values

        default_image_width = float(allowed_widths[default_width_index])
        default_exposure = acq_parameters.exp_time
        exposure_limits = api.detector.get_exposure_time_limits()
        experiment_time = (
                total_strategy_length * default_exposure / default_image_width
        )

        # For calculating dose-budget transmission
        std_dose_rate = self.get_nominal_dose_rate(energy=beam_energies.values()[0])
        if std_dose_rate:
            dose_budget = self.get_dose_budget(
                resolution, relative_sensitivity=relative_sensitivity
            )
            dose_budget -= data_model.get_dose_consumed()
            if dose_budget > 0:
                transmission = 100 * dose_budget * budget_use_fraction / (
                    experiment_time * std_dose_rate
                )
                transmission = min(transmission, 100.0)
            else:
                transmission = 0.0

            def update_function(
                field_widget,
                # experiment_length=total_strategy_length,
                # relative_sensitivity=relative_sensitivity,
            ):
                """Function to update rotation_rate and budget_used fields
                In parameter popup"""
                parameters = field_widget.get_parameters_map()
                exposure_time = float(parameters.get("exposure", 0))
                image_width = float(parameters.get("imageWidth", 0))
                resolution = float(parameters.get("resolution", 0))
                budget_use_fraction = (
                    float(parameters.get("budget_use_fraction", 0)) / 100.0
                )
                energy = float(parameters.get(list(beam_energies)[0], 0))
                if not energy:
                    energy = api.energy.get_value()

                transmission = dose_budget = 0.0
                if image_width and exposure_time:
                    rotation_rate = image_width / exposure_time
                    experiment_time = total_strategy_length / rotation_rate

                    if resolution:
                        std_dose_rate = self.get_nominal_dose_rate(energy=energy)

                        dose_budget = self.get_dose_budget(
                            resolution, relative_sensitivity=relative_sensitivity
                        )
                        use_dose_budget  = (
                            dose_budget * budget_use_fraction
                            - data_model.get_dose_consumed()
                        )
                        if use_dose_budget > 0:
                            transmission = 100 * use_dose_budget / (
                                experiment_time * std_dose_rate
                            )
                            transmission = min(transmission, 100.0)
                        else:
                            transmission = 0.0


                dd0 = {
                    "rotation_rate": rotation_rate,
                    "experiment_time": experiment_time,
                    "dose_budget": dose_budget,
                    "transmission": transmission,
                }
                field_widget.set_values(dd0)

        else:
            update_function = None
            transmission = acq_parameters.transmission

        field_list = [
            {
                "variableName": "_info",
                "uiLabel": "Data collection plan",
                "type": "textarea",
                "defaultValue": info_text,
            },
            {
                "variableName": "resolution",
                "uiLabel": "Detector resolution (A)",
                "type": "floatstring",
                "defaultValue": resolution,
                "lowerBound": 0.0,
                "upperBound": 9.0,
                "decimals": 3,
                "update_function": update_function,
            },
            {
                "variableName": "imageWidth",
                "uiLabel": "Oscillation range",
                "type": "combo",
                "defaultValue": str(default_image_width),
                "textChoices": [str(x) for x in allowed_widths],
                "update_function": update_function,
            },
            {
                "variableName": "exposure",
                "uiLabel": "Exposure Time (s)",
                "type": "floatstring",
                "defaultValue": default_exposure,
                # NBNB TODO fill in from config ??
                "lowerBound": exposure_limits[0],
                "upperBound": exposure_limits[1],
                "decimals": 4,
                "update_function": update_function,
            },
            {
                "variableName": "budget_use_fraction",
                "uiLabel": "% of dose budget to use",
                "type": "floatstring",
                "defaultValue": budget_use_fraction * 100.0,
                "lowerBound": 0.0,
                "upperBound": 300.0,
                "update_function": update_function,
                "decimals": 1,
            },
        ]
        if (
            data_model.lattice_selected
            or "calibration" in data_model.get_type().lower()
        ):
            field_list.append(
                {
                    "variableName": "snapshot_count",
                    "uiLabel": "Number of snapshots",
                    "type": "combo",
                    "defaultValue": str(data_model.get_snapshot_count()),
                    "textChoices": ["0", "1", "2", "4"],
                }
            )

        field_list[-1]["NEW_COLUMN"] = "True"

        ll0 = []
        for tag, val in beam_energies.items():
            ll0.append(
                {
                    "variableName": tag,
                    "uiLabel": "%s beam energy (keV)" % tag,
                    "type": "floatstring",
                    "defaultValue": val,
                    "lowerBound": 4.0,
                    "upperBound": 20.0,
                    "decimals": 4,
                }
            )
        if self.getProperty("starting_beamline_energy") == "frozen":
            # Use current energy and disallow changes
            ll0[0]["defaultValue"] = api.energy.getCurrentEnergy()
            ll0[0]["readOnly"] = True
        else:
            ll0[0]["update_function"] = update_function
        field_list.extend(ll0)

        if (
            data_model.lattice_selected
            and data_model.get_interleave_order()
        ):
            # NB We do not want the wedgeWdth widget for Diffractcal
            field_list.append(
                {
                    "variableName": "wedgeWidth",
                    "uiLabel": "Wedge width (deg)",
                    "type": "text",
                    "defaultValue": (
                        "%s" % self.getProperty("default_wedge_width", 15)
                    ),
                    "lowerBound": 0,
                    "upperBound": 7200,
                    "decimals": 1,
                }
            )

        if (
            data_model.lattice_selected
            or "calibration" in data_model.get_type().lower()
        ):


            if len(orientations) > 1:
                field_list.append(
                    {
                        "variableName": "centre_at_start",
                        "uiLabel": "(Re)centre all orientations before acquisition start?",
                        "type": "boolean",
                        "defaultValue": bool(self.getProperty("centre_at_start")),
                    }
                )
                field_list.append(
                    {
                        "variableName": "centre_before_sweep",
                        "uiLabel": "(Re)centre crystal when orientation changes?",
                        "type": "boolean",
                        "defaultValue": bool(self.getProperty("centre_before_sweep")),
                    }
                )
            else:
                defval = (
                    bool(self.getProperty("centre_at_start"))
                    or bool(self.getProperty("centre_before_sweep"))

                )
                field_list.append(
                    {
                        "variableName": "centre_at_start",
                        "uiLabel": "(Re)centre crystal before acquisition start?",
                        "type": "boolean",
                        "defaultValue":defval,
                    }
                )

            if data_model.get_interleave_order():
                field_list.append(
                    {
                        "variableName": "centre_before_scan",
                        "uiLabel": "(Re)centre crystal at the start of each scan?",
                        "type": "boolean",
                        "defaultValue": bool(self.getProperty("centre_before_scan")),
                    }
                )

        elif len(orientations) == 1:
            # Characterisation only
            field_list.append(
                {
                    "variableName": "recentre_before_start",
                    "uiLabel": "Recentre crystal before starting?",
                    "type": "boolean",
                    "defaultValue": bool(self.getProperty("recentre_before_start")),
                }
            )

        # Add third column of non-edited values
        field_list[-1]["NEW_COLUMN"] = "True"
        field_list.extend(
            [
                # NB Transmission is in % in UI, but in 0-1 in workflow
                {
                    "variableName": "experiment_lengh",
                    "uiLabel": "Experiment length (°)",
                    "type": "text",
                    "defaultValue": str(int(total_strategy_length)),
                    "readOnly": True,
                },
                {
                    "variableName": "experiment_time",
                    "uiLabel": "Experiment duration (s)",
                    "type": "floatstring",
                    "defaultValue": experiment_time,
                    "decimals": 1,
                    "readOnly": True,
                },
                {
                    "variableName": "rotation_rate",
                    "uiLabel": "Rotation speed (°/s)",
                    "type": "floatstring",
                    "defaultValue": (float(default_image_width / default_exposure)),
                    "decimals": 1,
                    "readOnly": True,
                },
                {
                    "variableName": "dose_budget",
                    "uiLabel": "Nominal dose budget (MGy)",
                    "type": "floatstring",
                    "defaultValue": full_dose_budget,
                    "lowerBound": 0.0,
                    "decimals": 1,
                    "readOnly": True,
                },
                {
                    "variableName": "transmission",
                    "uiLabel": "Transmission (%)",
                    "type": "floatstring",
                    "defaultValue": transmission,
                    "lowerBound": 0.0,
                    "upperBound": 100.0,
                    "decimals": 1,
                    "readOnly": True,
                },
            ]
        )

        self._return_parameters = gevent.event.AsyncResult()
        responses = dispatcher.send(
            "gphlParametersNeeded", self, field_list, self._return_parameters
        )
        if not responses:
            self._return_parameters.set_exception(
                RuntimeError("Signal 'gphlParametersNeeded' is not connected")
            )

        params = self._return_parameters.get()
        self._return_parameters = None

        if params is StopIteration:
            result = StopIteration

        else:
            result = {}
            tag = "imageWidth"
            value = params.get(tag)
            if value:
                image_width = result[tag] = float(value)
            else:
                image_width = self.getProperty("default_image_width", 15)
            tag = "exposure"
            value = params.get(tag)
            if value:
                result[tag] = float(value)
            tag = "transmission"
            value = params.get(tag)
            if value:
                # Convert from % to fraction
                result[tag] = float(value) / 100
            tag = "wedgeWidth"
            value = params.get(tag)
            if value:
                result[tag] = int(float(value) / image_width)
            else:
                # If not set is likely not used, but we want a detault value anyway
                result[tag] = 150
            tag = "resolution"
            value = params.get(tag)
            if value:
                result[tag] = float(value)

            tag = "snapshot_count"
            value = params.get(tag)
            if value:
                result[tag] = int(value)

            if geometric_strategy.isInterleaved:
                result["interleaveOrder"] = data_model.get_interleave_order()

            for tag in beam_energies:
                beam_energies[tag] = float(params.get(tag, 0))
            result["beam_energies"] = beam_energies

            for tag in (
                "centre_before_sweep",
                "centre_at_start",
                "centre_before_scan",
            ):
                # This defaults to False if parameter is not queried
                result[tag] = bool(params.get(tag))
            result["recentre_before_start"] =  bool(params.get(tag, True))

            # Register the dose (about to be) consumed
            energy = list(beam_energies.values())[0]
            std_dose_rate = self.get_nominal_dose_rate(energy=energy)
            dose_consumed = (
                float(params.get("transmission"))
                * float(params.get("experiment_time"))
                * std_dose_rate
                / 100
            ) + data_model.get_dose_consumed()
            data_model.set_dose_consumed(dose_consumed)
        #
        return result

    def setup_data_collection(self, payload, correlation_id):
        geometric_strategy = payload

        gphl_workflow_model = self._queue_entry.get_data_model()

        # enqueue data collection group
        if gphl_workflow_model.lattice_selected:
            # Data collection TODO: Use workflow info to distinguish
            new_dcg_name = "GPhL Data Collection"
        else:
            strategy_type = (
                gphl_workflow_model.get_workflow_parameters()["strategy_type"]
            )
            if strategy_type.startswith('diffractcal'):
                new_dcg_name = "GPhL DiffractCal"
            else:
                new_dcg_name = "GPhL Characterisation"
        logging.getLogger("HWR").debug("setup_data_collection %s" % new_dcg_name)
        new_dcg_model = queue_model_objects.TaskGroup()
        new_dcg_model.set_enabled(True)
        new_dcg_model.set_name(new_dcg_name)
        new_dcg_model.set_number(
            gphl_workflow_model.get_next_number_for_name(new_dcg_name)
        )
        self._data_collection_group = new_dcg_model
        self._add_to_queue(gphl_workflow_model, new_dcg_model)

        # Preset energy, detector setting and resolution before opening UI
        # Preset energy
        bst = geometric_strategy.defaultBeamSetting
        if bst and self.getProperty("starting_beamline_energy") == "configured":
            # First set beam_energy and give it time to settle,
            # so detector distance will trigger correct resolution later
            initial_energy = ConvertUtils.H_OVER_E / bst.wavelength
            # TODO NBNB put in wait-till ready to make sure value settles
            api.energy.move_energy(initial_energy)
        else:
            initial_energy = api.energy.getCurrentEnergy()

        # # Preset detector distance and resolution
        # For now only needed to reuse the ID if the value does not change
        # detectorSetting = geometric_strategy.defaultDetectorSetting
        # if detectorSetting:
        #     # NBNB If this is ever set to editable, distance and resolution
        #     # must be varied in sync
        #
        #     #GB: it should not be done here, but if so than using api.detector_distance.move()
        #     #api.detector.set_distance(detectorSetting.axisSettings.get("Distance"))
        #     pass
        #
        # # TODO NBNB put in wait-till-ready to make sure value settles
        #
        # """GB: if this was ment to synchronize detector distance motor move,
        # I guess it should wait_ready on detecor_distance motor, not on the detector itself
        # api.detector.wait_ready()
        # """

        # NB - now pre-setting of detector has been removed, this gets
        # the current resolution setting, whtever it is
        initial_resolution = api.resolution.get_value()
        # Put resolution value in workflow model object
        gphl_workflow_model.set_detector_resolution(initial_resolution)

        # Get modified parameters from UI and confirm acquisition
        # Run before centring, as it also does confirm/abort
        parameters = self.query_collection_strategy(geometric_strategy, initial_energy)
        if parameters is StopIteration:
            return StopIteration
        #' RF no longer needed
        # self._last_queryed_collection_strategy_parameters = parameters.copy()
        user_modifiable = geometric_strategy.isUserModifiable
        if user_modifiable:
            # Query user for new rotationSetting and make it,
            logging.getLogger("HWR").warning(
                "User modification of sweep settings not implemented. Ignored"
            )

        # Set transmission, detector_disance/resolution to final (unchanging) values
        # Also set energy to first energy value, necessary to get resolution consistent

        # Set beamline to match parameters
        # get wavelengths
        h_over_e = ConvertUtils.H_OVER_E
        beam_energies = parameters.pop("beam_energies")
        wavelengths = list(
            GphlMessages.PhasingWavelength(wavelength=h_over_e / val, role=tag)
            for tag, val in beam_energies.items()
        )
        # set to wavelength of first energy
        # necessary so that resolution setting below gives right detector distance
        new_energy = list(beam_energies.items())[0][1]
        if new_energy!= initial_energy:
            logging.getLogger("GUI").info(
                "GphlWorkflow: resetting energy from %7.3f to %7.3f keV"
                % (initial_energy, new_energy)
            )

            api.energy.move_wavelength(wavelengths[0].wavelength)
        # TODO ensure that move is finished before resolution is set

        # # get BcsDetectorSetting
        # new_resolution = parameters.pop("resolution")
        # if new_resolution == initial_resolution:
        #     # Reuse object and value
        #     id_ = detectorSetting.id_
        # else:
        #     id_ = None
            # TODO Clarify if set_position does not have a built-in wait
            # TODO whether you need towait for somethign else too, ...

            #GB api.resolution.move(new_resolution)
            # TODO it should be set_position, fix TineMotor (resolution at EMBL)
            # api.resolution.move(new_resolution)

            #GB if this is used, consider synchronizing api.resolution which is moved, not the detector
            #GB api.detector.wait_ready() 
            # NBNB Wait till value has settled
        #GB moved to point C
        #GB orgxy = collect_hwobj.get_beam_centre_pix() #GB: behind this is a series of hacks hardcoding eiger pixel ...  
        #GB orgxy = collect_hwobj.get_beam_centre()
        #GB detectorSetting = GphlMessages.BcsDetectorSetting(
        #GB   new_resolution, id_=id_, orgxy=orgxy, Distance=api.detector.get_distance()
        #GB )
        # 
        # # Set transmission - and get exact value for return message
        # # This value will not be modified by the collection queue
        # transmission = parameters["transmission"]
        # # NBNB allow time to settle
        # 
        # #GB api.transmission.set_value(100 * transmission)
        # #GB parameters["transmission"] = 0.01 * api.transmission.get_value()

        # Set up centring and recentring
        goniostatTranslations = []
        recen_parameters = {}
        queue_entries = []
        transcal_parameters = self.load_transcal_parameters()

        snapshot_count = parameters.pop("snapshot_count", None)
        if snapshot_count is not None:
            gphl_workflow_model.set_snapshot_count(snapshot_count)

        # Decide whether to centre before individual sweeps
        centre_at_start = parameters.pop("centre_at_start", False)
        crystal_pre_centred = not(parameters.pop("recentre_before_start", False))
        centre_before_sweep = parameters.pop("centre_before_sweep", False)
        centre_before_scan = parameters.pop("centre_before_scan", False)
        gphl_workflow_model.set_centre_before_sweep(centre_before_sweep)
        gphl_workflow_model.set_centre_before_scan(centre_before_scan)
        if not (
            centre_before_sweep
            or centre_before_scan
            or transcal_parameters
            or crystal_pre_centred
        ):
            centre_at_start = True

        found_sweep_setting_ids = set()
        for sweep in geometric_strategy.get_ordered_sweeps():
            sweepSetting = sweep.goniostatSweepSetting
            if crystal_pre_centred and not found_sweep_setting_ids:
                # First orientation, and we want to use the existing centring
                motor_positions = api.diffractometer.get_motor_positions()
                rot = dict(
                    (role, motor_positions.get(role))
                    for role in self.rotation_axis_roles
                )
                new_rotation = GphlMessages.GoniostatRotation(**rot)
                tra = dict(
                    (role, motor_positions.get(role))
                    for role in self.translation_axis_roles
                )
                translation = GphlMessages.GoniostatTranslation(
                    rotation=new_rotation,
                    requestedRotationId=sweepSetting.id_,
                    **tra
                )
                goniostatTranslations.append(translation)
                logging.getLogger("HWR").debug(
                    "Using pre-existing centring for first orientation"
                )
                recen_parameters["ref_xyz"] = tuple(
                    translation.axisSettings[x]
                    for x in self.translation_axis_roles
                )
                recen_parameters["ref_okp"] = tuple(
                    new_rotation.axisSettings[x] for x in self.rotation_axis_roles
                )
                logging.getLogger("HWR").debug(
                    "Recentring set-up. Parameters are: %s",
                    sorted(recen_parameters.items()),
                )

            elif sweepSetting.id_ not in found_sweep_setting_ids:
                # Handle centring on first appearance of SweepSetting
                found_sweep_setting_ids.add(sweepSetting.id_)

                # Get initial settings
                settings = dict(sweepSetting.axisSettings)
                # HACK - for some reason there is no value for 'phi' in the dict
                # TODO fix this
                scan_axis = sweepSetting.scanAxis
                if scan_axis not in settings:
                    settings[scan_axis] = 0.0
                known_translation = None
                if gphl_workflow_model.lattice_selected:
                    # We only trust translation settings for data collection,
                    # since they come from prior characterisation centring, via stratcal
                    known_translation = sweepSetting.translation
                    if known_translation is not None:
                        settings.update(known_translation.axisSettings)

                if known_translation is None and transcal_parameters:
                    # Either characterisation or diffractcal
                    if recen_parameters:
                        # We have already done a centring in this loop
                        # Calculate new centring
                        okp = tuple(settings[x] for x in self.rotation_axis_roles)
                        dd0 = self.calculate_recentring(okp, **recen_parameters)
                        logging.getLogger("HWR").debug(
                            "GPHL Recentring. okp, motors, %s, %s"
                            % (okp, sorted(dd0.items()))
                        )
                        if centre_at_start:
                            motor_settings = settings.copy()
                            motor_settings.update(dd0)
                            qe = self.enqueue_sample_centring(motor_settings=motor_settings)
                            queue_entries.append((qe, sweepSetting, motor_settings))
                        else:
                            # Use calculated (re)centring
                            # Creating the Translation adds it to the Rotation
                            translation = GphlMessages.GoniostatTranslation(
                                rotation=sweepSetting, **dd0
                            )
                            logging.getLogger("HWR").debug(
                                "Recentring. okp=%s, %s", okp, sorted(dd0.items())
                            )
                            goniostatTranslations.append(translation)
                    else:
                        # first centring in this loop - do it now regardless
                        # Use recentring parameters for successive sweeps
                        # NB This means you get two successive centrings in diffractcal
                        # TODO check if/how this can be avoided
                        recen_parameters = transcal_parameters
                        qe = self.enqueue_sample_centring(motor_settings=settings)
                        translation = self.execute_sample_centring(qe, sweepSetting)

                        okp = tuple(
                            int(settings[x]) for x in self.rotation_axis_roles
                        )
                        self.collect_centring_snapshots('%s_%s_%s' % okp)
                        goniostatTranslations.append(translation)
                        recen_parameters["ref_xyz"] = tuple(
                            translation.axisSettings[x]
                            for x in self.translation_axis_roles
                        )
                        recen_parameters["ref_okp"] = tuple(
                            settings[x] for x in self.rotation_axis_roles
                        )
                        logging.getLogger("HWR").debug(
                            "Recentring set-up. Parameters are: %s",
                            sorted(recen_parameters.items()),
                        )
                else:
                    # Either no transcal parameters so no recentring
                    # Or rely on pre-existing centring from stratcal (in practice)
                    if centre_at_start:
                        qe = self.enqueue_sample_centring(motor_settings=settings)
                        queue_entries.append((qe, sweepSetting, settings))

        for qe, goniostatRotation, settings in queue_entries:
            goniostatTranslations.append(
                self.execute_sample_centring(qe, goniostatRotation)
            )
            okp = tuple(int(settings[x]) for x in self.rotation_axis_roles)
            self.collect_centring_snapshots('%s_%s_%s' % okp)

        #GB, RF
        transmission = parameters["transmission"]
        logging.getLogger("GUI").info(
            "GphlWorkflow: setting transmission to %7.3f %%" % (100. * transmission)
        )
        api.transmission.set_value(100 * transmission)
        new_resolution = parameters.pop("resolution")
        if new_resolution != initial_resolution:
            logging.getLogger("GUI").info(
                "GphlWorkflow: setting detector distance for resolution %7.3f A"
                % new_resolution
            )
            # timeout in seconds: max move is ~2 meters, velocity 4 cm/sec
            api.resolution.move(new_resolution, timeout=60)
        orgxy = api.detector.get_beam_centre_pix()
        resolution=api.resolution.get_value()
        distance = api.detector_distance.get_position()
        dds = geometric_strategy.defaultDetectorSetting
        if distance == dds.axisSettings.get("Distance"):
            id_ = dds._id
        else:
            id_ = None
        detectorSetting = GphlMessages.BcsDetectorSetting(
            resolution, id_=id_, orgxy=orgxy, Distance=distance
        )
        # Do this at the end, for maxiumum time to settle
        api.transmission.wait_ready(20)
        parameters["transmission"] = 0.01 * api.transmission.get_value()

        # Return SampleCentred message
        sampleCentred = GphlMessages.SampleCentred(
            goniostatTranslations=goniostatTranslations,
            wavelengths=wavelengths,
            detectorSetting=detectorSetting,
            **parameters
        )
        return sampleCentred

    def load_transcal_parameters(self):
        """Load home_position and cross_sec_of_soc from transcal.nml"""
        fp0 = self.file_paths.get("transcal_file")
        if os.path.isfile(fp0):
            try:
                transcal_data = f90nml.read(fp0)["sdcp_instrument_list"]
            except BaseException:
                logging.getLogger("HWR").error(
                    "Error reading transcal.nml file: %s", fp0
                )
            else:
                result = {}
                result["home_position"] = transcal_data.get("trans_home")
                result["cross_sec_of_soc"] = transcal_data.get("trans_cross_sec_of_soc")
                if None in result.values():
                    logging.getLogger("HWR").warning("load_transcal_parameters failed")
                else:
                    return result
        else:
            logging.getLogger("HWR").warning("transcal.nml file not found: %s", fp0)
        # If we get here reading failed
        return {}

    def calculate_recentring(
        self, okp, home_position, cross_sec_of_soc, ref_okp, ref_xyz
    ):
        """Add predicted traslation values using recen
        okp is the omega,gamma,phi tuple of the target position,
        home_position is the translation calibration home position,
        and cross_sec_of_soc is the cross-section of the sphere of confusion
        ref_okp and ref_xyz are the reference omega,gamma,phi and the
        corresponding x,y,z translation position"""

        # Make input file
        infile = os.path.join(
            api.gphl_connection.software_paths["GPHL_WDIR"], "temp_recen.in"
        )
        recen_data = OrderedDict()
        indata = {"recen_list": recen_data}

        fp0 = self.file_paths.get("instrumentation_file")
        instrumentation_data = f90nml.read(fp0)["sdcp_instrument_list"]
        diffractcal_data = instrumentation_data

        fp0 = self.file_paths.get("diffractcal_file")
        try:
            diffractcal_data = f90nml.read(fp0)["sdcp_instrument_list"]
        except BaseException:
            logging.getLogger("HWR").debug(
                "diffractcal file not present - using instrumentation.nml %s", fp0
            )
        ll0 = diffractcal_data["gonio_axis_dirs"]
        recen_data["omega_axis"] = ll0[:3]
        recen_data["kappa_axis"] = ll0[3:6]
        recen_data["phi_axis"] = ll0[6:]
        ll0 = instrumentation_data["gonio_centring_axis_dirs"]
        recen_data["trans_1_axis"] = ll0[:3]
        recen_data["trans_2_axis"] = ll0[3:6]
        recen_data["trans_3_axis"] = ll0[6:]
        recen_data["cross_sec_of_soc"] = cross_sec_of_soc
        recen_data["home"] = home_position
        #
        f90nml.write(indata, infile, force=True)

        # Get program locations
        recen_executable = api.gphl_connection.get_executable("recen")
        # Get environmental variables
        envs = {"BDG_home": api.gphl_connection.software_paths["BDG_home"]}
        # Run recen
        command_list = [
            recen_executable,
            "--input",
            infile,
            "--init-xyz",
            "%s %s %s" % ref_xyz,
            "--init-okp",
            "%s %s %s" % ref_okp,
            "--okp",
            "%s %s %s" % okp,
        ]
        # NB the universal_newlines has the NECESSARY side effect of converting
        # output from bytes to string (with default encoding),
        # avoiding an explicit decoding step.
        result = {}
        logging.getLogger("HWR").debug(
            "Running Recen command: %s", " ".join(command_list)
        )
        try:
            output = subprocess.check_output(
                command_list,
                env=envs,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
            )
        except subprocess.CalledProcessError as err:
            logging.getLogger("HWR").error(
                "Recen failed with returncode %s. Output was:\n%s",
                err.returncode,
                err.output,
            )
            return result

        terminated_ok = False
        for line in reversed(output.splitlines()):
            ss0 = line.strip()
            if terminated_ok:
                if "X,Y,Z" in ss0:
                    ll0 = ss0.split()[-3:]
                    for ii, tag in enumerate(self.translation_axis_roles):
                        result[tag] = float(ll0[ii])
                    break

            elif ss0 == "NORMAL termination":
                terminated_ok = True
        else:
            logging.getLogger("HWR").error(
                "Recen failed with normal termination=%s. Output was:\n" % terminated_ok
                + output
            )
        #
        return result

    def collect_data(self, payload, correlation_id):
        collection_proposal = payload
        queue_manager = self._queue_entry.get_queue_controller()

        gphl_workflow_model = self._queue_entry.get_data_model()
        master_path_template = gphl_workflow_model.path_template
        relative_image_dir = collection_proposal.relativeImageDir

        sample = gphl_workflow_model.get_sample_node()
        # There will be exactly one for the kinds of collection we are doing
        crystal = sample.crystals[0]
        if (
            gphl_workflow_model.lattice_selected
            or "calibration" in gphl_workflow_model.get_type().lower()
        ):
            snapshot_count = gphl_workflow_model.get_snapshot_count()
        else:
            # Do not make snapshots during chareacterisation
            snapshot_count = 0
        centre_before_scan = bool(gphl_workflow_model.get_centre_before_scan())
        centre_before_sweep = bool(gphl_workflow_model.get_centre_before_sweep())
        data_collections = []
        snapshot_counts = dict()
        found_orientations = set()
        scans = collection_proposal.scans

        # RF: This work is done lower down, around ine 1304
        # energy = api.energy.getCurrentEnergy()
        # parameters = self._last_queryed_collection_strategy_parameters
        # resolution = parameters.pop("resolution")
        # transmission = parameters.pop("transmission")
        # distance = api.detector_distance.get_value()
        ###GB memo for wedges: nexprame: number of frames per wedge; nimages: number of wedges

        for scan in scans:
            sweep = scan.sweep
            acq = queue_model_objects.Acquisition()

            # Get defaults, even though we override most of them
            acq_parameters = api.beamline_setup.get_default_acquisition_parameters()
            acq.acquisition_parameters = acq_parameters

            acq_parameters.first_image = scan.imageStartNum
            acq_parameters.num_images = scan.width.numImages
            acq_parameters.osc_start = scan.start
            acq_parameters.osc_range = scan.width.imageWidth
            logging.getLogger("HWR").info(
                "Scan: %s images of %s deg. starting at %s (%s deg)",
                acq_parameters.num_images,
                acq_parameters.osc_range,
                acq_parameters.first_image,
                acq_parameters.osc_start,
            )
            # acq_parameters.kappa = self._get_kappa_axis_position()
            # acq_parameters.kappa_phi = self._get_kappa_phi_axis_position()
            # acq_parameters.overlap = overlap
            acq_parameters.exp_time = scan.exposure.time
            acq_parameters.num_passes = 1

            # HACK! value 0.0 is treated as 'do not set' when setting up queue
            # These have been set to the correct value earlier (setup_data_collection)
 
            ##
            wavelength = sweep.beamSetting.wavelength
            acq_parameters.wavelength = wavelength
            detdistance = sweep.detectorSetting.axisSettings["Distance"]
            # not needed when detdistance is set :
            # acq_parameters.resolution = resolution
            acq_parameters.detdistance = detdistance
            # transmission is not passed from the workflow (yet)
            # it defaults to current value (?), so no need to set it
            # acq_parameters.transmission = transmission*100.0
            

            # acq_parameters.shutterless = self._has_shutterless()
            # acq_parameters.detector_mode = self._get_roi_modes()
            acq_parameters.inverse_beam = False
            # acq_parameters.take_dark_current = True
            # acq_parameters.skip_existing_images = False

            # Edna also sets screening_id
            # Edna also sets osc_end

            # Path_template
            # path_template = queue_model_objects.PathTemplate()
            path_template = api.beamline_setup.get_default_path_template()
            # Naughty, but we want a clone, right?
            # NBNB this ONLY works because all the attributes are immutable values
            path_template.__dict__.update(master_path_template.__dict__)
            if relative_image_dir:
                path_template.directory = os.path.join(
                    api.session.get_base_image_directory(),
                    relative_image_dir
                )
                path_template.process_directory = os.path.join(
                    api.session.get_base_process_directory(),
                    relative_image_dir
                )
            acq.path_template = path_template
            filename_params = scan.filenameParams
            subdir = filename_params.get("subdir")
            if subdir:
                path_template.directory = os.path.join(path_template.directory, subdir)
                path_template.process_directory = os.path.join(
                    path_template.process_directory, subdir
                )
            ss0 = filename_params.get("run")
            path_template.run_number = int(ss0) if ss0 else 1
            prefix = filename_params.get("prefix", "")
            ib_component = filename_params.get("inverse_beam_component_sign", "")
            ll0 = []
            if prefix:
                ll0.append(prefix)
            if ib_component:
                ll0.append(ib_component)
            path_template.base_prefix = "_".join(ll0)
            beam_setting_index = filename_params.get("beam_setting_index") or ""
            path_template.mad_prefix = beam_setting_index
            path_template.wedge_prefix = (
                filename_params.get("gonio_setting_index") or ""
            )
            path_template.start_num = acq_parameters.first_image
            path_template.num_files = acq_parameters.num_images

            goniostatRotation = sweep.goniostatSweepSetting
            if (
                centre_before_sweep and goniostatRotation.id_ not in found_orientations
            ) or centre_before_scan:
                # Put centring on queue and collect using the resulting position
                # NB this means that the actual translational axis positions
                # will NOT be known to the workflow
                self.enqueue_sample_centring(
                    motor_settings=sweep.get_initial_settings(), in_queue=True
                )
            else:
                # Collect using precalculated centring position
                dd0 = sweep.get_initial_settings()
                dd0[goniostatRotation.scanAxis] = scan.start
                acq_parameters.centred_position = queue_model_objects.CentredPosition(
                    dd0
                )
            found_orientations.add(goniostatRotation.id_)

            count = snapshot_counts.get(sweep, snapshot_count)
            acq_parameters.take_snapshots = count
            if (
                ib_component
                or beam_setting_index
                or not gphl_workflow_model.lattice_selected
            ):
                # Only snapshots first time a sweep is encountered
                # When doing inverse beam or wavelength interleaving
                # or canned strategies
                snapshot_counts[sweep] = 0


            data_collection = queue_model_objects.DataCollection([acq], crystal)
            data_collections.append(data_collection)

            data_collection.set_enabled(True)
            data_collection.set_name(path_template.get_prefix())
            data_collection.set_number(path_template.run_number)
            self._add_to_queue(self._data_collection_group, data_collection)
            if scan is not scans[-1]:
                dc_entry = queue_manager.get_entry_with_model(data_collection)
                dc_entry.in_queue = True

        data_collection_entry = queue_manager.get_entry_with_model(
            self._data_collection_group
        )
        queue_manager.execute_entry(data_collection_entry)
        self._data_collection_group = None

        if data_collection_entry.status == QUEUE_ENTRY_STATUS.FAILED:
            # TODO NBNB check if these status codes are corerct
            status = 1
        else:
            status = 0

        # NB, uses last path_template,
        # but directory should be the same for all
        return GphlMessages.CollectionDone(
            status=status,
            proposalId=collection_proposal.id_,
            # Only if you want to override prior information rootdir, which we do not
            # imageRoot=path_template.directory
        )

    def select_lattice(self, payload, correlation_id):
        choose_lattice = payload

        solution_format = choose_lattice.lattice_format

        # Must match bravaisLattices column
        lattices = choose_lattice.lattices

        # First letter must match first letter of BravaisLattice
        crystal_system = choose_lattice.crystalSystem

        # Color green (figuratively) if matches lattices,
        # or otherwise if matches crystalSystem

        dd0 = self.parse_indexing_solution(solution_format, choose_lattice.solutions)

        field_list = [
            {
                "variableName": "_cplx",
                "uiLabel": "Select indexing solution:",
                "type": "selection_table",
                "header": dd0["header"],
                "colours": None,
                "defaultValue": (dd0["solutions"],),
            }
        ]

        # colour matching lattices green
        colour_check = lattices
        if crystal_system and not colour_check:
            colour_check = (crystal_system,)
        if colour_check:
            colours = [None] * len(dd0["solutions"])
            for ii, line in enumerate(dd0["solutions"]):
                if any(x in line for x in colour_check):
                    colours[ii] = "LIGHT_GREEN"
            field_list[0]["colours"] = colours

        self._return_parameters = gevent.event.AsyncResult()
        responses = dispatcher.send(
            "gphlParametersNeeded", self, field_list, self._return_parameters
        )
        if not responses:
            self._return_parameters.set_exception(
                RuntimeError("Signal 'gphlParametersNeeded' is not connected")
            )

        params = self._return_parameters.get()
        if params is StopIteration:
            return StopIteration

        ll0 = ConvertUtils.text_type(params["_cplx"][0]).split()
        if ll0[0] == "*":
            del ll0[0]
        #
        self._queue_entry.get_data_model().lattice_selected = True
        return GphlMessages.SelectedLattice(
            lattice_format=solution_format, solution=ll0
        )

    def parse_indexing_solution(self, solution_format, text):

        # Solution table. for format IDXREF will look like
        """
*********** DETERMINATION OF LATTICE CHARACTER AND BRAVAIS LATTICE ***********

 The CHARACTER OF A LATTICE is defined by the metrical parameters of its
 reduced cell as described in the INTERNATIONAL TABLES FOR CRYSTALLOGRAPHY
 Volume A, p. 746 (KLUWER ACADEMIC PUBLISHERS, DORDRECHT/BOSTON/LONDON, 1989).
 Note that more than one lattice character may have the same BRAVAIS LATTICE.

 A lattice character is marked "*" to indicate a lattice consistent with the
 observed locations of the diffraction spots. These marked lattices must have
 low values for the QUALITY OF FIT and their implicated UNIT CELL CONSTANTS
 should not violate the ideal values by more than
 MAXIMUM_ALLOWED_CELL_AXIS_RELATIVE_ERROR=  0.03
 MAXIMUM_ALLOWED_CELL_ANGLE_ERROR=           1.5 (Degrees)

  LATTICE-  BRAVAIS-   QUALITY  UNIT CELL CONSTANTS (ANGSTROEM & DEGREES)
 CHARACTER  LATTICE     OF FIT      a      b      c   alpha  beta gamma

 *  44        aP          0.0      56.3   56.3  102.3  90.0  90.0  90.0
 *  31        aP          0.0      56.3   56.3  102.3  90.0  90.0  90.0
 *  33        mP          0.0      56.3   56.3  102.3  90.0  90.0  90.0
 *  35        mP          0.0      56.3   56.3  102.3  90.0  90.0  90.0
 *  34        mP          0.0      56.3  102.3   56.3  90.0  90.0  90.0
 *  32        oP          0.0      56.3   56.3  102.3  90.0  90.0  90.0
 *  14        mC          0.1      79.6   79.6  102.3  90.0  90.0  90.0
 *  10        mC          0.1      79.6   79.6  102.3  90.0  90.0  90.0
 *  13        oC          0.1      79.6   79.6  102.3  90.0  90.0  90.0
 *  11        tP          0.1      56.3   56.3  102.3  90.0  90.0  90.0
    37        mC        250.0     212.2   56.3   56.3  90.0  90.0  74.6
    36        oC        250.0      56.3  212.2   56.3  90.0  90.0 105.4
    28        mC        250.0      56.3  212.2   56.3  90.0  90.0  74.6
    29        mC        250.0      56.3  125.8  102.3  90.0  90.0  63.4
    41        mC        250.0     212.3   56.3   56.3  90.0  90.0  74.6
    40        oC        250.0      56.3  212.2   56.3  90.0  90.0 105.4
    39        mC        250.0     125.8   56.3  102.3  90.0  90.0  63.4
    30        mC        250.0      56.3  212.2   56.3  90.0  90.0  74.6
    38        oC        250.0      56.3  125.8  102.3  90.0  90.0 116.6
    12        hP        250.1      56.3   56.3  102.3  90.0  90.0  90.0
    27        mC        500.0     125.8   56.3  116.8  90.0 115.5  63.4
    42        oI        500.0      56.3   56.3  219.6 104.8 104.8  90.0
    15        tI        500.0      56.3   56.3  219.6  75.2  75.2  90.0
    26        oF        625.0      56.3  125.8  212.2  83.2 105.4 116.6
     9        hR        750.0      56.3   79.6  317.1  90.0 100.2 135.0
     1        cF        999.0     129.6  129.6  129.6 128.6  75.7 128.6
     2        hR        999.0      79.6  116.8  129.6 118.9  90.0 109.9
     3        cP        999.0      56.3   56.3  102.3  90.0  90.0  90.0
     5        cI        999.0     116.8   79.6  116.8  70.1  39.8  70.1
     4        hR        999.0      79.6  116.8  129.6 118.9  90.0 109.9
     6        tI        999.0     116.8  116.8   79.6  70.1  70.1  39.8
     7        tI        999.0     116.8   79.6  116.8  70.1  39.8  70.1
     8        oI        999.0      79.6  116.8  116.8  39.8  70.1  70.1
    16        oF        999.0      79.6   79.6  219.6  90.0 111.2  90.0
    17        mC        999.0      79.6   79.6  116.8  70.1 109.9  90.0
    18        tI        999.0     116.8  129.6   56.3  64.3  90.0 118.9
    19        oI        999.0      56.3  116.8  129.6  61.1  64.3  90.0
    20        mC        999.0     116.8  116.8   56.3  90.0  90.0 122.4
    21        tP        999.0      56.3  102.3   56.3  90.0  90.0  90.0
    22        hP        999.0      56.3  102.3   56.3  90.0  90.0  90.0
    23        oC        999.0     116.8  116.8   56.3  90.0  90.0  57.6
    24        hR        999.0     162.2  116.8   56.3  90.0  69.7  77.4
    25        mC        999.0     116.8  116.8   56.3  90.0  90.0  57.6
    43        mI        999.0      79.6  219.6   56.3 104.8 135.0  68.8

 For protein crystals the possible space group numbers corresponding  to"""

        # find headers lines
        solutions = []
        if solution_format == "IDXREF":
            lines = text.splitlines()
            for indx, line in enumerate(lines):
                if "BRAVAIS-" in line:
                    # Used as marker for first header line
                    header = ["%s\n%s" % (line, lines[indx + 1])]
                    break
            else:
                raise ValueError("Substring 'BRAVAIS-' missing in %s indexing solution")

            for line in lines[indx:]:
                ss0 = line.strip()
                if ss0:
                    # we are skipping blank line at the start
                    if solutions or ss0[0] == "*":
                        # First real line will start with a '*
                        # Subsequent non-empty lines will also be used
                        solutions.append(line)
                elif solutions:
                    # we have finished - empty non-initial line
                    break

            #
            return {"header": header, "solutions": solutions}
        else:
            raise ValueError(
                "GPhL: Indexing format %s is not known" % repr(solution_format)
            )

    def process_centring_request(self, payload, correlation_id):
        # Used for transcal only - anything else is data collection related
        request_centring = payload

        logging.getLogger("user_level_log").info(
            "Start centring no. %s of %s",
            request_centring.currentSettingNo,
            request_centring.totalRotations,
        )

        # Rotate sample to RotationSetting
        goniostatRotation = request_centring.goniostatRotation
        goniostatTranslation = goniostatRotation.translation
        #

        if self._data_collection_group is None:
            gphl_workflow_model = self._queue_entry.get_data_model()
            new_dcg_name = "GPhL Translational calibration"
            new_dcg_model = queue_model_objects.TaskGroup()
            new_dcg_model.set_enabled(True)
            new_dcg_model.set_name(new_dcg_name)
            new_dcg_model.set_number(
                gphl_workflow_model.get_next_number_for_name(new_dcg_name)
            )
            self._data_collection_group = new_dcg_model
            self._add_to_queue(gphl_workflow_model, new_dcg_model)

        if request_centring.currentSettingNo < 2:
            # Start without fine zoom setting
            self._use_fine_zoom = False
        elif not self._use_fine_zoom and goniostatRotation.translation is not None:
            # We are moving to having recentered positions -
            # Set or prompt for fine zoom
            self._use_fine_zoom = True
            zoom_motor = api.beamline_setup.getObjectByRole("zoom")
            if zoom_motor:
                # Zoom to the last predefined position
                # - that should be the largest magnification
                ll0 = zoom_motor.getPredefinedPositionsList()
                if ll0:
                    logging.getLogger("user_level_log").info(
                        "Sample re-centering now active - Zooming in."
                    )
                    zoom_motor.moveToPosition(ll0[-1])
                else:
                    logging.getLogger("HWR").warning(
                        "No predefined positions for zoom motor."
                    )
            else:
                # Ask user to zoom
                info_text = """Automatic sample re-centering is now active
    Switch to maximum zoom before continuing"""
                field_list = [
                    {
                        "variableName": "_info",
                        "uiLabel": "Data collection plan",
                        "type": "textarea",
                        "defaultValue": info_text,
                    }
                ]
                self._return_parameters = gevent.event.AsyncResult()
                responses = dispatcher.send(
                    "gphlParametersNeeded", self, field_list, self._return_parameters
                )
                if not responses:
                    self._return_parameters.set_exception(
                        RuntimeError("Signal 'gphlParametersNeeded' is not connected")
                    )

                # We do not need the result, just to end the waiting
                response = self._return_parameters.get()
                self._return_parameters = None
                if response is StopIteration:
                    return StopIteration

        settings = goniostatRotation.axisSettings.copy()
        if goniostatTranslation is not None:
            settings.update(goniostatTranslation.axisSettings)
        centring_queue_entry = self.enqueue_sample_centring(motor_settings=settings)
        goniostatTranslation = self.execute_sample_centring(
            centring_queue_entry, goniostatRotation
        )

        if request_centring.currentSettingNo >= request_centring.totalRotations:
            returnStatus = "DONE"
        else:
            returnStatus = "NEXT"
        #
        return GphlMessages.CentringDone(
            returnStatus,
            timestamp=time.time(),
            goniostatTranslation=goniostatTranslation,
        )

    def enqueue_sample_centring(self, motor_settings, in_queue=False):

        queue_manager = self._queue_entry.get_queue_controller()

        centring_model = queue_model_objects.SampleCentring(
            name="Centring (GPhL)", motor_positions=motor_settings
        )
        self._add_to_queue(self._data_collection_group, centring_model)
        centring_entry = queue_manager.get_entry_with_model(centring_model)
        centring_entry.in_queue = in_queue

        return centring_entry

    def collect_centring_snapshots(self, file_name_prefix='snapshot'):
        """

        :param file_prefix: str
        :return:
        """


        gphl_workflow_model = self._queue_entry.get_data_model()
        number_of_snapshots = gphl_workflow_model.get_snapshot_count()
        if number_of_snapshots:
            filename_template = "%s_%s_%s.jpeg"
            snapshot_directory = os.path.join(
                gphl_workflow_model.path_template.get_archive_directory(),
                "centring_snapshots",
            )

            logging.getLogger("user_level_log").info(
                "Post-centring: Taking %d sample snapshot(s)", number_of_snapshots
            )
            collect_hwobj = api.collect
            timestamp = datetime.datetime.now().isoformat().split(".")[0]
            summed_angle = 0.0
            for snapshot_index in range(number_of_snapshots):
                if snapshot_index:
                    api.diffractometer.move_omega_relative(90)
                    summed_angle += 90
                snapshot_filename = (
                    filename_template
                    % (file_name_prefix, timestamp, snapshot_index + 1)
                )
                snapshot_filename = os.path.join(snapshot_directory, snapshot_filename)
                logging.getLogger("HWR").debug(
                    "Centring snapshot stored at %s", snapshot_filename
                )
                collect_hwobj._take_crystal_snapshot(snapshot_filename)
            if summed_angle:
                api.diffractometer.move_omega_relative(-summed_angle)

    def execute_sample_centring(
        self, centring_entry, goniostatRotation, requestedRotationId=None
    ):

        queue_manager = self._queue_entry.get_queue_controller()
        queue_manager.execute_entry(centring_entry)

        centring_result = centring_entry.get_data_model().get_centring_result()
        if centring_result:
            positionsDict = centring_result.as_dict()
            dd0 = dict((x, positionsDict[x]) for x in self.translation_axis_roles)
            return GphlMessages.GoniostatTranslation(
                rotation=goniostatRotation,
                requestedRotationId=requestedRotationId,
                **dd0
            )
        else:
            self.abort("No Centring result found")

    def prepare_for_centring(self, payload, correlation_id):

        # TODO Add pop-up confirmation box ('Ready for centring?')

        return GphlMessages.ReadyForCentring()

    def obtain_prior_information(self, payload, correlation_id):

        workflow_model = self._queue_entry.get_data_model()
        sample_model = workflow_model.get_sample_node()

        cell_params = workflow_model.get_cell_parameters()
        if cell_params:
            unitCell = GphlMessages.UnitCell(*cell_params)
        else:
            unitCell = None

        obj = queue_model_enumerables.SPACEGROUP_MAP.get(
            workflow_model.get_space_group()
        )
        space_group = obj.number if obj else None

        crystal_system = workflow_model.get_crystal_system()
        if crystal_system:
            crystal_system = crystal_system.upper()

        # NB Expected resolution is deprecated.
        # It is set to the current resolution value, for now
        userProvidedInfo = GphlMessages.UserProvidedInfo(
            scatterers=(),
            lattice=crystal_system,
            pointGroup=workflow_model.get_point_group(),
            spaceGroup=space_group,
            cell=unitCell,
            expectedResolution=api.collect.get_resolution(),
            isAnisotropic=None,
        )
        ll0 = ["PriorInformation"]
        for tag in (
            "expectedResolution",
            "isAnisotropic",
            "lattice",
            "pointGroup",
            "scatterers",
            "spaceGroup",
        ):
            val = getattr(userProvidedInfo, tag)
            if val:
                ll0.append("%s=%s" % (tag, val))
        if cell_params:
            ll0.append("cell_parameters=%s" % (cell_params,))
        logging.getLogger("HWR").debug(", ".join(ll0))

        # Look for existing uuid
        for text in sample_model.lims_code, sample_model.code, sample_model.name:
            if text:
                try:
                    sampleId = uuid.UUID(text)
                except BaseException:
                    # The error expected if this goes wrong is ValueError.
                    # But whatever the error we want to continue
                    pass
                else:
                    # Text was a valid uuid string. Use the uuid.
                    break
        else:
            sampleId = uuid.uuid1()

        image_root = api.session.get_base_image_directory()

        if not os.path.isdir(image_root):
            # This direstory must exist by the time the WF software checks for it
            try:
                os.makedirs(image_root)
            except BaseException:
                # No need to raise error - program will fail downstream
                logging.getLogger("HWR").error(
                    "Could not create image root directory: %s", image_root
                )

        priorInformation = GphlMessages.PriorInformation(
            sampleId=sampleId,
            sampleName=workflow_model.path_template.base_prefix,
            # Changed to use MXCuBE prefix for naming purposes
            # sampleName=(
            #     sample_model.name
            #     or sample_model.code
            #     or sample_model.lims_code
            #     or workflow_model.path_template.get_prefix()
            #     or ConvertUtils.text_type(sampleId)
            # ),
            rootDirectory=image_root,
            userProvidedInfo=userProvidedInfo,
        )
        #
        return priorInformation

    # Utility functions

    def get_dose_budget(self, resolution, decay_limit=None, relative_sensitivity=1.0):
        """Get resolution-dependent dose budget using configured values"""
        decay_limit = decay_limit or self.getProperty("default_decay_limit", 0.25)
        max_budget = self.getProperty("", 20)
        result = -2 * math.log(decay_limit) * resolution * resolution
        #
        return min(result, max_budget) / relative_sensitivity

    def get_nominal_dose_rate(self, energy=None):
        """
        Get dose rate in MGy/s for a standard crystal at current settings.
        Assumes square, top-hat beam so that the flux is evenly spread
        over the rectangulat area of the beam.

        :param energy: float Energy for calculation of dose rate, in keV.
        :return: float
        """

        energy = energy or api.energy.get_value()()

        # NB   Calculation assumes beam sizes in mm
        beam_size = api.beam_info.get_beam_size()

        # Result in kGy/s
        result = (
                api.flux.dose_rate_per_photon_per_mmsq(energy)
                * api.flux.get_flux()
                / beam_size[0]
                / beam_size[1]
                / 1000000.  # Converts to MGy/s
        )
        return result

    def get_emulation_crystal_data(self):
        """If sample is a test data set for emulation, get crystal data

        Returns:
            Optional[dict]
        """

        crystal_data = None
        hklfile = None
        sample = api.sample_changer.getLoadedSample()
        if sample:
            sample_name = sample.getName()
            if sample_name and sample_name.startswith(self.TEST_SAMPLE_PREFIX):
                sample_name = sample_name[len(self.TEST_SAMPLE_PREFIX):]

                sample_dir = api.gphl_connection.software_paths.get(
                    "gphl_test_samples"
                )
                if not sample_dir:
                    raise ValueError(
                        "Test sample requires gphl_test_samples dir specified"
                    )
                sample_dir = os.path.join(sample_dir, sample_name)
                if not os.path.isdir(sample_dir):
                    raise ValueError(
                        "Sample data directory %s does not exist" % sample_dir
                    )
                crystal_file = os.path.join(sample_dir, "crystal.nml")
                if not os.path.isfile(crystal_file):
                    raise ValueError(
                        "Emulator crystal data file %s does not exist" % crystal_file
                    )
                # in spite of the simcal_crystal_list name this returns an OrderdDict
                crystal_data = f90nml.read(crystal_file)["simcal_crystal_list"]
                if isinstance(crystal_data, list):
                    crystal_data = crystal_data[0]
                hklfile = os.path.join(sample_dir, "sample.hkli")
                if not os.path.isfile(hklfile):
                    raise ValueError("Emulator hkli file %s does not exist" % hklfile)
        #
        return crystal_data, hklfile
