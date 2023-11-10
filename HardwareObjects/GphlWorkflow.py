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
along with MXCuBE. If not, see <https://www.gnu.org/licenses/>.
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
import json
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
from HardwareRepository.HardwareObjects.queue_entry import (
    QUEUE_ENTRY_STATUS,
    QueueAbortedException,
)

from HardwareRepository.HardwareObjects import GphlMessages
from HardwareRepository.HardwareObjects.Gphl.Transcal2MiniKappa import make_home_data
from HardwareRepository.HardwareObjects.Gphl import crystal_symmetry

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict


__copyright__ = """ Copyright © 2016 - 2019 by Global Phasing Ltd. """
__license__ = "LGPLv3+"
__author__ = "Rasmus H Fogh"

# Additional sample/diffraction plan data for GPhL emulation samples.
EMULATION_DATA = {"3n0s": {"radiationSensitivity": 0.9}}

# Centring modes for use in centring mode pulldown.
# The dictionary keys are labels (changeable),
# the values are passed to the program (not changeable)
# The first value is the default
RECENTRING_MODES = OrderedDict(
    (
        ("Re-centre when orientation changes", "sweep"),
        ("Re-centre at the start of each wedge", "scan"),
        ("Re-centre all before acquisition start", "start"),
        ("No manual re-centring, rely on calculated values", "none"),
    )
)

# Lattice to point groups,
# Used for GPhL UI pulldowns, hence the combined point groups, like '4|422'
# The list of keys, plus "", defines the GPhL lattices pulldown.
lattice2point_group_tags = OrderedDict(
    (
        ("aP",("1",)),
        ("Triclinic",("2",)),
        ("mP",("2",)),
        ("mC",("2",)),
        ("mI",("2",)),
        ("Monoclinic",("2",)),
        ("oP",("222",)),
        ("oC",("222",)),
        ("oF",("222",)),
        ("oI",("222",)),
        ("Orthorhombic",("222",)),
        ("tP",("4", "422", "4|422")),
        ("tI",("4", "422", "4|422")),
        ("Tetragonal",("4", "422", "4|422")),
        ("hP",("3", "312", "321", "3|32", "6", "622", "6|622", "3|32|6|622")),
        ("hR",("3", "32", "3|32")),
        ("Hexagonal",
             (
                "3",
                "312",
                "321",
                "32",
                "3|32",
                "6",
                "622",
                "6|622",
                "3|32|6|622",
            )
        ),
        ("cP",("23", "432", "23|432")),
        ("cF",("23", "432", "23|432")),
        ("cI",("23", "432", "23|432")),
        ("Cubic",("23", "432", "23|432")),
    )
)
all_point_group_tags = []
for tag in (
    "Triclinic",
    "Monoclinic",
    "Orthorhombic",
    "Tetragonal",
    "Hexagonal",
    "Cubic",
):
    all_point_group_tags += lattice2point_group_tags[tag]

# Allowed altervative lattices for a given lattice
alternative_lattices = {}
for ll0 in (
    ["aP", "Triclinic"],
    ["mP", "mC", "mI", "Monoclinic"],
    ["oP", "oC", "oF", "oI", "Orthorhombic"],
    ["tP", "tI", "Tetragonal"],
    ["hP", "hR", "Hexagonal"],
    ["cP", "cF", "cI", "Cubic"],
):
    for tag in ll0:
        alternative_lattices[tag] = ll0


class GphlWorkflow(HardwareObject, object):
    """Global Phasing workflow runner."""

    STATES = GphlMessages.States

    TEST_SAMPLE_PREFIX = "emulate"

    def __init__(self, name):
        super(GphlWorkflow, self).__init__(name)

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

        self.recentring_file = None

        # HACK
        self.strategyWavelength = None

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
        self._state = self.STATES.OFF

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
            # NB this sould be done with isinstance, but that seems to fail,
            # probably because of import path mix-ups.
            detector._set_beam_centre(
                (instrument_data["det_org_x"], instrument_data["det_org_y"])
            )

        recentring_file = os.path.join(
            api.session.get_base_process_directory(), "recen.nml"
        )
        if os.path.isfile(recentring_file):
            self.recentring_file = recentring_file

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

        if api.gphl_connection.hasObject("ssh_options"):
            # We are running workflow through ssh - set beamline url
            all_workflow_options = {"beamline": "py4j:%s:" % socket.gethostname()}
        else:
            all_workflow_options = {"beamline": "py4j::"}

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
            variants = wf_node.getProperty("variants").strip().split()
            wf_dict = {
                "name": name,
                "strategy_type": strategy_type,
                "variants": variants,
                "application": wf_node.getProperty("application"),
                "documentation": wf_node.getProperty("documentation", default_value=""),
                "interleaveOrder": wf_node.getProperty(
                    "interleave_order", default_value=""
                ),
            }
            result[name] = wf_dict

            if strategy_type == "transcal":
                wf_dict["options"] = dd0 = all_workflow_options.copy()
                if wf_node.hasObject("options"):
                    dd0.update(wf_node["options"].getProperties())
                    relative_file_path = dd0.get("file")
                    if relative_file_path is not None:
                        # Special case - this option must be modified before use
                        dd0["file"] = os.path.join(
                            self.file_paths["gphl_beamline_config"], relative_file_path
                        )

            elif strategy_type == "diffractcal":
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
            # devmode = dd0.get("co.gphl.wf.devMode")
            # if devmode and devmode[0] not in "fFnN":
            #     # We are in developer mode. Add parameters
            #     dd0["co.gphl.wf.stratcal.opt.--strategy_type"] = strategy_type
            #     if variant:
            #         dd0["co.gphl.wf.stratcal.opt.--variant"] = variant
            #     angular_tolerance = self.getProperty("angular_tolerance")
            #     if angular_tolerance:
            #         dd0["co.gphl.wf.stratcal.opt.--angular-tolerance"] = float(
            #             angular_tolerance
            #         )

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

        default_exposure_time = self.getProperty("default_exposure_time", 0)
        default_exposure_time = max(
            default_exposure_time,  api.detector.get_exposure_time_limits()[0]
        )
        queue_entry.get_data_model().set_default_exposure_time(default_exposure_time)

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

        self.set_state(self.STATES.BUSY)
        self._workflow_queue = gevent._threading.Queue()

        # Fork off workflow server process
        api.gphl_connection.start_workflow(
            self._workflow_queue, self._queue_entry.get_data_model()
        )

        while True:
            if self._workflow_queue is None:
                # We can only get that value if we have already done post_execute
                # but the mechanics of aborting means we conme back
                # Stop further processing here
                raise QueueAbortedException("Aborting...", self)

            tt0 = self._workflow_queue.get()
            if tt0 is StopIteration:
                logging.getLogger("HWR").debug("GPhL queue StopIteration")
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
                logging.getLogger("HWR").info("GPhL queue processing %s", message_type)
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

        # Number of decimals for rounding use_dose values
        use_dose_decimals = 4

        data_model = self._queue_entry.get_data_model()
        wf_parameters = data_model.get_workflow_parameters()

        orientations = OrderedDict()
        strategy_length = 0
        axis_setting_dicts = OrderedDict()
        for sweep in geometric_strategy.get_ordered_sweeps():
            strategy_length += sweep.width
            rotation_id = sweep.goniostatSweepSetting.id_
            if rotation_id in orientations:
                orientations[rotation_id].append(sweep)
            else:
                orientations[rotation_id] = [sweep]
                axis_settings = sweep.goniostatSweepSetting.axisSettings.copy()
                axis_settings.pop(sweep.goniostatSweepSetting.scanAxis, None)
                axis_setting_dicts[rotation_id] = axis_settings

        # Make info_text and do some setting up
        axis_names = self.rotation_axis_roles
        if (
            data_model.lattice_selected
            or wf_parameters.get("strategy_type") == "diffractcal"
        ):
            lines = [
                "%s strategy, variant '%s'"
                % (data_model.get_type(), data_model.get_variant())
            ]
            if data_model.lattice_selected:
                dummy, point_group = crystal_symmetry.strategy_laue_group(
                    data_model.get_crystal_classes()
                )
                lines[0] += (", for point group '%s'" % point_group)
            lines.extend(("-" * len(lines[0]), ""))
            # Data collection TODO: Use workflow info to distinguish
            beam_energies = OrderedDict()
            energies = [initial_energy, initial_energy + 0.01, initial_energy - 0.01]
            for ii, tag in enumerate(data_model.get_beam_energy_tags()):
                beam_energies[tag] = energies[ii]
            dose_label = "Dose/repetition (MGy)"

        else:
            # Characterisation
            lines = ["Characterisation strategy"]
            lines.extend(("=" * len(lines[0]), ""))
            beam_energies = OrderedDict((("Characterisation", initial_energy),))
            dose_label = "Characterisation dose (MGy)"
            if not self.getProperty("recentre_before_start"):
                # replace planned orientation with current orientation
                current_pos_dict = api.diffractometer.get_motor_positions()
                dd0 = list(axis_setting_dicts.values())[0]
                for tag in dd0:
                    pos = current_pos_dict.get(tag)
                    if pos is not None:
                        dd0[tag] = pos

        if len(beam_energies) > 1:
            lines.append(
                "Experiment length (per repetition): %s * %6.1f°"
                % (len(beam_energies), strategy_length)
            )
        else:
            lines.append("Experiment length (per repetition): %6.1f°" % strategy_length)

        for rotation_id, sweeps in orientations.items():
            axis_settings = axis_setting_dicts[rotation_id]
            ss0 = "\nSweep :     " + ",  ".join(
                "%s= %6.1f°" % (x, axis_settings.get(x))
                for x in axis_names
                if x in axis_settings
            )
            ll1 = []
            for sweep in sweeps:
                start = sweep.start
                width = sweep.width
                ss1 = "%s= %6.1f°,  sweep width= %6.1f°" % (
                    sweep.goniostatSweepSetting.scanAxis,
                    start,
                    width,
                )
                ll1.append(ss1)
            lines.append(ss0 + ",  " + ll1[0])
            spacer = " " * (len(ss0) + 2)
            for ss1 in ll1[1:]:
                lines.append(spacer + ss1)

        info_text = "\n".join(lines)

        # Set up image width pulldown
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

        # set starting and unchanging values of parameters
        acq_parameters = api.beamline_setup.get_default_acquisition_parameters()

        resolution = api.resolution.get_value()

        # For calculating dose-budget transmission
        flux_density = api.flux.get_average_flux_density(transmission=100.0)
        if flux_density:
            std_dose_rate = (
                api.flux.dose_rate_per_photon_per_mmsq(initial_energy)
                * flux_density
                * data_model.get_relative_rad_sensitivity()
                * 1.0e-6  # convert to MGy/s
            )
        else:
            std_dose_rate = 0

        dose_budget = self.resolution2dose_budget(
            resolution,
            decay_limit=data_model.get_decay_limit(),
        )
        default_image_width = float(allowed_widths[default_width_index])
        default_exposure = data_model.get_default_exposure_time()
        exposure_limits = api.detector.get_exposure_time_limits()
        total_strategy_length = strategy_length * len(beam_energies)
        transmission = 100.0
        if (
            data_model.lattice_selected
            or wf_parameters.get("strategy_type") == "diffractcal"
        ):
            proposed_dose = dose_budget - data_model.get_characterisation_dose()
        else:
            proposed_dose = (
                dose_budget * data_model.get_characterisation_budget_fraction()
            )
            if std_dose_rate:
                # Use current settings if characterisation dose works out as less
                # than proposed 5% of dose budget
                currdose = (
                    total_strategy_length
                    * default_exposure
                    * std_dose_rate
                    * transmission
                    / (100 * default_image_width)
                )
                proposed_dose = min(currdose, proposed_dose)
        proposed_dose = round(max(proposed_dose, 0), use_dose_decimals)

        # define update functions

        def update_exptime(field_widget):
            """When image_width or exposure_time change,
             update rotation_rate, experiment_time and either use_dose or transmission
            In parameter popup"""
            parameters = field_widget.get_parameters_map()
            exposure_time = float(parameters.get("exposure") or 0.0)
            image_width = float(parameters.get("imageWidth") or 0.0)
            transmission = float(parameters.get("transmission") or 0.0)
            repetition_count = int(parameters.get("repetition_count") or 1)
            if image_width and exposure_time:
                rotation_rate = image_width / exposure_time
                dd0 = {
                    "rotation_rate": rotation_rate,
                }
                experiment_time = total_strategy_length / rotation_rate
                if std_dose_rate and transmission:
                    # NB - dose is calculated for *one* repetition
                    dd0["use_dose"] = (
                        std_dose_rate * experiment_time * transmission / 100.0
                    )
                dd0["experiment_time"] = experiment_time * repetition_count
                field_widget.set_values(**dd0)

        def update_transmission(field_widget):
            """When transmission changes, update use_dose
            In parameter popup"""
            parameters = field_widget.get_parameters_map()
            exposure_time = float(parameters.get("exposure") or 0.0)
            image_width = float(parameters.get("imageWidth") or 0.0)
            transmission = float(parameters.get("transmission") or 0.0)
            if image_width and exposure_time and std_dose_rate and transmission:
                # If we get here, Adjust dose
                # NB dose is calculated for *one* repetition
                experiment_time = exposure_time * total_strategy_length / image_width
                use_dose = std_dose_rate * experiment_time * transmission / 100
                field_widget.set_values(use_dose=use_dose)

        def update_resolution(field_widget):

            parameters = field_widget.get_parameters_map()
            dbg = self.resolution2dose_budget(
                float(parameters.get("resolution")),
                decay_limit=data_model.get_decay_limit(),
            )
            characterisation_dose = data_model.get_characterisation_dose()
            if characterisation_dose:
                use_dose = dbg - data_model.get_characterisation_dose()
            else:
                use_dose = dbg * data_model.get_characterisation_budget_fraction()
            field_widget.set_values(
                dose_budget=dbg, use_dose=use_dose, exposure=default_exposure
            )
            update_dose(field_widget)

        def update_dose(field_widget):
            """When use_dose changes, update transmission and/or exposure_time
            In parameter popup"""
            parameters = field_widget.get_parameters_map()
            image_width = float(parameters.get("imageWidth") or 0.0)
            exposure_time = float(parameters.get("exposure") or 0.0)
            use_dose = float(parameters.get("use_dose") or 0.0)
            transmission = float(parameters.get("transmission") or 0.0)
            min_exposure, max_exposure = exposure_limits

            # NB total_strategy_length use_dose aned experiment_time
            # all relate to a single repetition

            if image_width and exposure_time and std_dose_rate and use_dose:
                experiment_time = exposure_time * total_strategy_length / image_width
                if transmission:
                    prev_dose = (
                        std_dose_rate * experiment_time * transmission / 100
                    )
                    factor = use_dose / prev_dose
                    if factor >= 1:
                        # Increase dose by increasing transmission
                        transmission *= factor
                    else:
                        exposure_time *= factor
                        experiment_time *= factor

                else:
                    transmission = 100 * use_dose / (std_dose_rate * experiment_time)

                if transmission > 100.0:
                    # Transmission too high. Try max transmission and longer exposure
                    factor = transmission / 100.
                    transmission = 100.0
                    exposure_time *= factor
                    experiment_time *= factor
                    if max_exposure and exposure_time > max_exposure:
                        factor = max_exposure / exposure_time
                        use_dose *= factor
                        experiment_time *= factor

                if exposure_time < min_exposure:
                    factor = min_exposure / exposure_time
                    transmission /= factor
                    exposure_time = min_exposure
                    experiment_time *= factor

                field_widget.set_values(
                    exposure=exposure_time,
                    transmission=transmission,
                    use_dose=use_dose,
                    experiment_time=experiment_time,
                )

        reslimits = api.resolution.get_limits()
        if None in reslimits:
            reslimits = (0.5, 5.0)
        if std_dose_rate:
            use_dose_start = proposed_dose
            use_dose_frozen = False
        else:
            use_dose_start = 0
            use_dose_frozen = True
            logging.getLogger("user_level_log").warning(
                "Dose rate cannot be calculated - dose bookkeeping disabled"
            )

        field_list = [
            {
                "variableName": "_info",
                "uiLabel": "Data collection plan",
                "type": "textarea",
                "defaultValue": info_text,
            },
            # NB Transmission is in % in UI, but in 0-1 in workflow
            {
                "variableName": "transmission",
                "uiLabel": "Transmission (%)",
                "type": "floatstring",
                "defaultValue": transmission,
                "lowerBound": 0.0001,
                "upperBound": 100.0,
                "decimals": 4,
                "update_function": update_transmission,
            },
            {
                "variableName": "use_dose",
                "uiLabel": dose_label,
                "type": "floatstring",
                "defaultValue": use_dose_start,
                "lowerBound": 0.000001,
                "decimals": use_dose_decimals,
                "update_function": update_dose,
                "readOnly": use_dose_frozen,
            },
        ]
        if data_model.lattice_selected:
            field_list.append(
                {
                    "variableName": "repetition_count",
                    "uiLabel": "Number of repetitions",
                    "type": "spinbox",
                    "defaultValue": 1,
                    "lowerBound": 1,
                    "upperBound": 99,
                    "stepsize": 1,
                    "update_function": update_exptime,
                }
            )
        field_list.extend(
            [
                {
                    "variableName": "exposure",
                    "uiLabel": "Exposure Time (s)",
                    "type": "floatstring",
                    "defaultValue": default_exposure,
                    "lowerBound": exposure_limits[0],
                    "upperBound": exposure_limits[1],
                    "decimals": 6,
                    "update_function": update_exptime,
                },
                {
                    "variableName": "imageWidth",
                    "uiLabel": "Oscillation range",
                    "type": "combo",
                    "defaultValue": str(default_image_width),
                    "textChoices": [str(x) for x in allowed_widths],
                    "update_function": update_dose,
                },
            ]
        )
        # Add third column of non-edited values
        field_list[-1]["NEW_COLUMN"] = "True"
        field_list.append(
            {
                "variableName": "resolution",
                "uiLabel": "Detector resolution (A)",
                "type": "floatstring",
                "defaultValue": resolution,
                "lowerBound": reslimits[0],
                "upperBound": reslimits[1],
                "decimals": 3,
                "readOnly": False,
            }
        )
        if data_model.lattice_selected:
            field_list[-1]["readOnly"] = True
        else:
            field_list[-1]["update_function"] = update_resolution
        field_list.append(
            {
                "variableName": "dose_budget",
                "uiLabel": "Dose budget (MGy)",
                "type": "floatstring",
                "defaultValue": dose_budget,
                "lowerBound": 0.0,
                "decimals": 4,
                "readOnly": True,
            }
        )
        # NB - this is the default starting value, so repetition_count is 1 at this point
        experiment_time = total_strategy_length * default_exposure / default_image_width
        field_list.extend(
            [
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
            ]
        )

        if data_model.lattice_selected and data_model.get_interleave_order():
            # NB We do not want the wedgeWdth widget for Diffractcal
            field_list.append(
                {
                    "variableName": "wedgeWidth",
                    "uiLabel": "Wedge width (deg)",
                    "type": "text",
                    "defaultValue": (
                        "%s" % self.getProperty("default_wedge_width", 15)
                    ),
                    "lowerBound": 0.1,
                    "upperBound": 7200,
                    "decimals": 2,
                }
            )

        field_list[-1]["NEW_COLUMN"] = "True"

        energyLimits = api.energy.get_limits()
        ll0 = []
        for tag, val in beam_energies.items():
            ll0.append(
                {
                    "variableName": tag,
                    "uiLabel": "%s beam energy (keV)" % tag,
                    "type": "floatstring",
                    "defaultValue": val,
                    "lowerBound": energyLimits[0],
                    "upperBound": energyLimits[1],
                    "decimals": 4,
                }
            )
        ll0[0]["readOnly"] = True
        field_list.extend(ll0)

        field_list.append(
            {
                "variableName": "snapshot_count",
                "uiLabel": "Number of snapshots",
                "type": "combo",
                "defaultValue": str(data_model.get_snapshot_count()),
                "textChoices": ["0", "1", "2", "4"],
            }
        )

        # recentring mode:
        labels = list(RECENTRING_MODES.keys())
        modes = list(RECENTRING_MODES.values())
        default_recentring_mode = self.getProperty("default_recentring_mode", "sweep")
        if default_recentring_mode == "scan" or default_recentring_mode not in modes:
            raise ValueError(
                "invalid default recentring mode '%s' " % default_recentring_mode
            )
        use_modes = ["sweep"]
        if len(orientations) > 1:
            use_modes.append("start")
        if data_model.get_interleave_order():
            use_modes.append("scan")
        if self.recentring_file and (
            data_model.lattice_selected
            or wf_parameters.get("strategy_type") == "diffractcal"
        ):
            # Not Characteisation
            use_modes.append("none")
        for indx in range(len(modes) - 1, -1, -1):
            if modes[indx] not in use_modes:
                del modes[indx]
                del labels[indx]
        if default_recentring_mode in modes:
            indx = modes.index(default_recentring_mode)
            if indx:
                # Put default at top
                del modes[indx]
                modes.insert(indx, default_recentring_mode)
                default_label = labels.pop(indx)
                labels.insert(indx, default_label)
        else:
            default_recentring_mode = "sweep"
        default_label = labels[modes.index(default_recentring_mode)]
        if len(modes) > 1:
            field_list.append(
                {
                    "variableName": "recentring_mode",
                    "type": "dblcombo",
                    "defaultValue": default_label,
                    "textChoices": labels,
                }
            )

        self._return_parameters = gevent.event.AsyncResult()
        responses = dispatcher.send(
            "gphlParametersNeeded",
            self,
            field_list,
            self._return_parameters,
            update_dose,
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
            tag = "repetition_count"
            result[tag] = int(params.get(tag, 1))
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
                # If not set is likely not used, but we want a default value anyway
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

            tag = "recentring_mode"
            result[tag] = (
                RECENTRING_MODES.get(params.get(tag)) or default_recentring_mode
            )

            data_model.set_dose_budget(float(params.get("dose_budget", 0)))
            # Register the dose (about to be) consumed
            if std_dose_rate:
                if (
                    data_model.lattice_selected
                    or wf_parameters.get("strategy_type") == "diffractcal"
                ):
                    data_model.set_acquisition_dose(float(params.get("use_dose", 0)))
                else:
                    data_model.set_characterisation_dose(
                        float(params.get("use_dose", 0))
                    )
        #
        return result

    def setup_data_collection(self, payload, correlation_id):
        geometric_strategy = payload
        sweeps = geometric_strategy.get_ordered_sweeps()
        gphl_workflow_model = self._queue_entry.get_data_model()
        angular_tolerance = float(self.getProperty("angular_tolerance", 1.0))

        # enqueue data collection group
        strategy_type = gphl_workflow_model.get_workflow_parameters()["strategy_type"]
        if gphl_workflow_model.lattice_selected:
            # Data collection TODO: Use workflow info to distinguish
            new_dcg_name = "GPhL Data Collection"
        else:
            if strategy_type == "diffractcal":
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

        # NB for any type of acquisition, energy and resolution are set before this point

        bst = geometric_strategy.defaultBeamSetting
        if bst and self.getProperty("starting_beamline_energy") == "configured":
            # Preset energy
            # First set beam_energy and give it time to settle,
            # so detector distance will trigger correct resolution later
            initial_energy = api.energy._calculate_energy(bst.wavelength)
            # TODO NBNB put in wait-till ready to make sure value settles
            api.energy.move_energy(initial_energy)
        else:
            initial_energy = api.energy.getCurrentEnergy()

        # NB - now pre-setting of detector has been removed, this gets
        # the current resolution setting, whatever it is
        initial_resolution = api.resolution.get_value()
        # Put resolution value in workflow model object
        gphl_workflow_model.set_detector_resolution(initial_resolution)

        # Get modified parameters from UI and confirm acquisition
        # Run before centring, as it also does confirm/abort
        parameters = self.query_collection_strategy(geometric_strategy, initial_energy)
        if parameters is StopIteration:
            return StopIteration
        user_modifiable = geometric_strategy.isUserModifiable
        if user_modifiable:
            # Query user for new rotationSetting and make it,
            logging.getLogger("HWR").warning(
                "User modification of sweep settings not implemented. Ignored"
            )

        gphl_workflow_model.set_exposure_time(parameters.get("exposure" or 0.0))
        gphl_workflow_model.set_image_width(parameters.get("imageWidth" or 0.0))

        # Set transmission, detector_disance/resolution to final (unchanging) values
        # Also set energy to first energy value, necessary to get resolution consistent

        # Set beam_energies to match parameters
        # get wavelengths
        beam_energies = parameters.pop("beam_energies")
        wavelengths = list(
            GphlMessages.PhasingWavelength(
                wavelength=api.energy._calculate_wavelength(val), role=tag
            )
            for tag, val in beam_energies.items()
        )
        if self.strategyWavelength is not None:
            wavelengths[0] = self.strategyWavelength

        transmission = parameters["transmission"]
        logging.getLogger("GUI").info(
            "GphlWorkflow: setting transmission to %7.3f %%" % (100.0 * transmission)
        )
        api.transmission.set_value(100 * transmission)

        new_resolution = parameters.pop("resolution")
        if (
            new_resolution != initial_resolution
            and not gphl_workflow_model.lattice_selected
        ):
            logging.getLogger("GUI").info(
                "GphlWorkflow: setting detector distance for resolution %7.3f A"
                % new_resolution
            )
            # timeout in seconds: max move is ~2 meters, velocity 4 cm/sec
            api.resolution.move(new_resolution, timeout=60)

        snapshot_count = parameters.pop("snapshot_count", None)
        if snapshot_count is not None:
            gphl_workflow_model.set_snapshot_count(snapshot_count)

        recentring_mode = parameters.pop("recentring_mode")
        gphl_workflow_model.set_recentring_mode(recentring_mode)
        goniostatTranslations = []

        # Get all sweepSettings, in order
        sweepSettings = []
        sweepSettingIds = set()
        for sweep in sweeps:
            sweepSetting = sweep.goniostatSweepSetting
            sweepSettingId = sweepSetting.id_
            if sweepSettingId not in sweepSettingIds:
                sweepSettingIds.add(sweepSettingId)
                sweepSettings.append(sweepSetting)

        # For recentring mode start do settings in reverse order
        if recentring_mode == "start":
            sweepSettings.reverse()
        sweepSetting = sweepSettings[0]

        # Get current position values
        current_pos_dict = api.diffractometer.get_motor_positions()
        current_okp = tuple(current_pos_dict[role] for role in self.rotation_axis_roles)
        current_xyz = tuple(
            current_pos_dict[role] for role in self.translation_axis_roles
        )

        # Check if sample is currently centred, and centre first sweep if not
        if (
            self.getProperty("recentre_before_start")
            and not gphl_workflow_model.lattice_selected
        ):
            # Sample has never been centred reliably.
            # Centre it at sweepsetting and put it into goniostatTranslations
            settings = dict(sweepSetting.axisSettings)
            qe = self.enqueue_sample_centring(motor_settings=settings)
            translation, current_pos_dict = self.execute_sample_centring(
                qe, sweepSetting
            )
            current_okp = tuple(
                current_pos_dict[role] for role in self.rotation_axis_roles
            )
            current_xyz = tuple(
                current_pos_dict[role] for role in self.translation_axis_roles
            )
            goniostatTranslations.append(translation)
            gphl_workflow_model.set_current_rotation_id(sweepSetting.id_)

        elif gphl_workflow_model.lattice_selected or strategy_type == "diffractcal":
            # Acquisition or diffractcal; crystal was already centred
            settings = dict(sweepSetting.axisSettings)
            okp = tuple(settings.get(x, 0) for x in self.rotation_axis_roles)
            maxdev = max(abs(okp[1] - current_okp[1]), abs(okp[2] - current_okp[2]))

            if self.recentring_file:
                # calculate recentre first sweep recentring from okp
                translation_settings = self.calculate_recentring(
                    okp, ref_xyz=current_xyz, ref_okp=current_okp
                )
                logging.getLogger("HWR").debug(
                    "GPHL Recentring. okp, motors, %s, %s"
                    % (okp, sorted(translation_settings.items()))
                )
            else:
                # existing centring - take from current position
                translation_settings = dict(
                    (role, current_pos_dict.get(role))
                    for role in self.translation_axis_roles
                )

            tol = angular_tolerance if self.recentring_file else 1.0
            if maxdev <= tol:
                # first orientation matches current, set to current centring
                # Use sweepSetting as is, recentred or very close
                translation = GphlMessages.GoniostatTranslation(
                    rotation=sweepSetting, **translation_settings
                )
                goniostatTranslations.append(translation)
                gphl_workflow_model.set_current_rotation_id(sweepSetting.id_)

            else:

                if recentring_mode == "none":
                    if self.recentring_file:
                        translation = GphlMessages.GoniostatTranslation(
                            rotation=sweepSetting, **translation_settings
                        )
                        goniostatTranslations.append(translation)
                    else:
                        raise RuntimeError(
                            "Coding error, mode 'none' requires recentring parameters"
                        )
                else:
                    if self.recentring_file:
                        settings.update(translation_settings)
                    qe = self.enqueue_sample_centring(motor_settings=settings)
                    translation, dummy = self.execute_sample_centring(qe, sweepSetting)
                    goniostatTranslations.append(translation)
                    gphl_workflow_model.set_current_rotation_id(sweepSetting.id_)
                    if recentring_mode == "start":
                        # We want snapshots in this mode,
                        # and the first sweep is skipped in the loop below
                        okp = tuple(
                            int(settings.get(x, 0)) for x in self.rotation_axis_roles
                        )
                        self.collect_centring_snapshots("%s_%s_%s" % okp)

        else:
            # Characterisation, and sample was centred before we got here
            # Do characterisation at current position, not the hardcoded one
            rotation_settings = dict(
                (role, current_pos_dict[role]) for role in sweepSetting.axisSettings
            )
            newRotation = GphlMessages.GoniostatRotation(**rotation_settings)
            translation_settings = dict(
                (role, current_pos_dict.get(role))
                for role in self.translation_axis_roles
            )
            translation = GphlMessages.GoniostatTranslation(
                rotation=newRotation,
                requestedRotationId=sweepSetting.id_,
                **translation_settings
            )
            goniostatTranslations.append(translation)
            gphl_workflow_model.set_current_rotation_id(newRotation.id_)

        # calculate or determine centring for remaining sweeps
        if not goniostatTranslations:
            raise RuntimeError(
                "Coding error, first sweepSetting should have been set here"
            )
        for sweepSetting in sweepSettings[1:]:
            settings = dict(sweepSetting.axisSettings)
            if self.recentring_file:
                # Update settings
                okp = tuple(settings.get(x, 0) for x in self.rotation_axis_roles)
                settings.update(
                    self.calculate_recentring(
                        okp, ref_xyz=current_xyz, ref_okp=current_okp
                    )
                )
                logging.getLogger("HWR").debug(
                    "GPHL Recentring: " +
                    ", ".join("%s:%s" % item for item in sorted(settings.items()))
                )

            if recentring_mode == "start":
                qe = self.enqueue_sample_centring(motor_settings=settings)
                logging.getLogger("HWR").debug(
                    "GPHL recenter at : " +
                    ", ".join("%s:%s" % item for item in sorted(settings.items()))
                )
                translation, dummy = self.execute_sample_centring(qe, sweepSetting)
                goniostatTranslations.append(translation)
                gphl_workflow_model.set_current_rotation_id(sweepSetting.id_)
                okp = tuple(int(settings.get(x, 0)) for x in self.rotation_axis_roles)
                self.collect_centring_snapshots("%s_%s_%s" % okp)
            elif self.recentring_file and not gphl_workflow_model.lattice_selected :
                # Not the first sweep and not gone through stratcal
                # Calculate recentred positions and pass back
                translation = GphlMessages.GoniostatTranslation(
                    rotation=sweepSetting, **settings
                )
                logging.getLogger("HWR").debug(
                    "GPHL calculate recentring: " +
                    ", ".join("%s:%s" % item for item in sorted(settings.items()))
                )
                goniostatTranslations.append(translation)

        orgxy = api.detector.get_beam_centre_pix()
        resolution = api.resolution.get_value()
        distance = api.detector_distance.get_position()
        dds = geometric_strategy.defaultDetectorSetting
        if distance == dds.axisSettings.get("Distance"):
            id_ = dds._id
        else:
            id_ = None
        if gphl_workflow_model.lattice_selected or strategy_type == "diffractcal":
            detectorSetting = None
        else:
            detectorSetting = GphlMessages.BcsDetectorSetting(
                resolution, id_=id_, orgxy=orgxy, Distance=distance
            )

        # Return SampleCentred message
        sampleCentred = GphlMessages.SampleCentred(
            goniostatTranslations=goniostatTranslations,
            wavelengths=wavelengths,
            detectorSetting=detectorSetting,
            **parameters
        )
        return sampleCentred

    def calculate_recentring(self, okp, ref_okp, ref_xyz):
        """Calculate predicted translation values using recen
        okp is the omega,gamma,phi tuple of the target position,
        ref_okp and ref_xyz are the reference omega,gamma,phi and the
        corresponding x,y,z translation position
        """

        # Get program locations
        recen_executable = api.gphl_connection.get_executable("recen")
        # Get environmental variables
        envs = {
            "autoPROC_home": api.gphl_connection.software_paths.get("GPHL_INSTALLATION")
        }
        GPHL_XDS_PATH = api.gphl_connection.software_paths.get("GPHL_XDS_PATH")
        if GPHL_XDS_PATH:
            envs["GPHL_XDS_PATH"] = GPHL_XDS_PATH
        GPHL_CCP4_PATH = api.gphl_connection.software_paths.get("GPHL_CCP4_PATH")
        if GPHL_CCP4_PATH:
            envs["GPHL_CCP4_PATH"] = GPHL_CCP4_PATH
        # Run recen
        command_list = [
            recen_executable,
            "--input",
            self.recentring_file,
            "--init-xyz",
            "%s,%s,%s" % ref_xyz,
            "--init-okp",
            "%s,%s,%s" % ref_okp,
            "--okp",
            "%s,%s,%s" % okp,
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
        for tag, val in result.items():
            motor = api.diffractometer.getObjectByRole(tag)
            limits = motor.get_limits()
            if limits:
                limit = limits[0]
                if limit is not None and val < limit:
                    logging.getLogger("HWR").warning(
                        "WARNING, centring motor "
                        "%s position %s recentred to below minimum limit %s"
                        % (tag, val, limit)
                    )
                limit = limits[1]
                if limit is not None and val > limit:
                    logging.getLogger("HWR").warning(
                        "WARNING, centring motor "
                        "%s position %s recentred to above maximum limit %s"
                        % (tag, val, limit)
                    )
        #
        return result

    def collect_data(self, payload, correlation_id):
        collection_proposal = payload

        angular_tolerance = float(self.getProperty("angular_tolerance", 1.0))
        queue_manager = self._queue_entry.get_queue_controller()

        gphl_workflow_model = self._queue_entry.get_data_model()
        master_path_template = gphl_workflow_model.path_template
        relative_image_dir = collection_proposal.relativeImageDir

        sample = gphl_workflow_model.get_sample_node()
        # There will be exactly one for the kinds of collection we are doing
        crystal = sample.crystals[0]
        snapshot_count = gphl_workflow_model.get_snapshot_count()
        # wf_parameters = gphl_workflow_model.get_workflow_parameters()
        # if (
        #     gphl_workflow_model.lattice_selected
        #     or wf_parameters.get("strategy_type") == "diffractcal"
        # ):
        #     snapshot_count = gphl_workflow_model.get_snapshot_count()
        # else:
        #     # Do not make snapshots during chareacterisation
        #     snapshot_count = 0
        recentring_mode = gphl_workflow_model.get_recentring_mode()
        data_collections = []
        scans = collection_proposal.scans

        geometric_strategy = collection_proposal.strategy
        repeat_count = geometric_strategy.sweepRepeat
        sweep_offset = geometric_strategy.sweepOffset
        scan_count = len(scans)

        if repeat_count and sweep_offset and self.getProperty("use_multitrigger"):
            # commpress unrolled multi-trigger sweep
            # NBNB as of 202103 this is only allowed for a single sweep
            #
            # For now this is required
            if repeat_count != scan_count:
                raise ValueError(
                    " scan count %s does not match repeat count %s"
                    % (scan_count, repeat_count)
                )
            # treat only the first scan
            scans = scans[:1]

        sweeps = set()
        last_orientation = ()
        maxdev = -1
        snapshotted_rotation_ids = set()
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
            acq_parameters.exp_time = scan.exposure.time
            acq_parameters.num_passes = 1

            ##
            wavelength = sweep.beamSetting.wavelength
            acq_parameters.energy = api.energy._calculate_energy(wavelength)
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
            path_template = api.beamline_setup.get_default_path_template()
            # Naughty, but we want a clone, right?
            # NBNB this ONLY works because all the attributes are immutable values
            path_template.__dict__.update(master_path_template.__dict__)
            if relative_image_dir:
                path_template.directory = os.path.join(
                    api.session.get_base_image_directory(), relative_image_dir
                )
                path_template.process_directory = os.path.join(
                    api.session.get_base_process_directory(), relative_image_dir
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
            path_template.base_prefix = filename_params.get("prefix", "")
            path_template.start_num = acq_parameters.first_image
            path_template.num_files = acq_parameters.num_images

            # Handle orientations and (re) centring

            # NB this gets rotation axes always, and translation axes if present
            motor_settings = sweep.get_motor_settings()

            orientation = (
                motor_settings.get("kappa"), motor_settings.get( "kappa_phi")
            )
            if last_orientation:
                maxdev = max(
                    abs(orientation[ind] - last_orientation[ind]) for ind in range(2)
                )
            last_orientation = orientation

            goniostatRotation = sweep.goniostatSweepSetting
            rotation_id = goniostatRotation.id_
            if not sweeps or recentring_mode in ("start", "none"):
                # First sweep (previously centred), or necessary centrings already done
                # Collect using precalculated or stored centring position
                acq_parameters.centred_position = queue_model_objects.CentredPosition(
                    motor_settings
                )
            elif recentring_mode == "sweep" and (
                rotation_id == gphl_workflow_model.get_current_rotation_id()
                or (0 <= maxdev < angular_tolerance)
            ):
                # Use same postion as previous sweep, set only omega start
                acq_parameters.centred_position = queue_model_objects.CentredPosition(
                    {goniostatRotation.scanAxis: scan.start}
                )
            else:
                # New sweep, or recentriong_mode == scan
                # # We need to recentre
                # Put centring on queue and collect using the resulting position
                # NB this means that the actual translational axis positions
                # will NOT be known to the workflow
                self.enqueue_sample_centring(
                    motor_settings=motor_settings, in_queue=True
                )

            if (
                rotation_id in snapshotted_rotation_ids
                and rotation_id == gphl_workflow_model.get_current_rotation_id()
            ):
                acq_parameters.take_snapshots = 0
            else:
                # Only snapshots at the start or when orientation changes
                # NB the current_rotation_id can be set before acquisition commences
                # as it controls centring
                snapshotted_rotation_ids.add(rotation_id)
                acq_parameters.take_snapshots = snapshot_count

            gphl_workflow_model.set_current_rotation_id(rotation_id)
            sweeps.add(sweep)

            if repeat_count and sweep_offset and self.getProperty("use_multitrigger"):
                # Multitrigger sweep - add in parameters.
                # NB if we are here ther can be only one scan
                acq_parameters.num_triggers = scan_count
                acq_parameters.num_images_per_trigger = acq_parameters.num_images
                acq_parameters.num_images *= scan_count
                # NB this assumes sweepOffset is the offset between starting points
                acq_parameters.overlap = (
                    acq_parameters.num_images_per_trigger * acq_parameters.osc_range
                    - sweep_offset
                )
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

        dispatcher.send("gphlStartAcquisition", self, gphl_workflow_model)
        try:
            queue_manager.execute_entry(data_collection_entry)
        finally:
            dispatcher.send("gphlDoneAcquisition", self, gphl_workflow_model)
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
            procWithLatticeParams=gphl_workflow_model.get_use_cell_for_processing(),
            # Only if you want to override prior information rootdir, which we do not
            # imageRoot=path_template.directory
        )

    def select_lattice(self, payload, correlation_id):
        choose_lattice = payload

        data_model = self._queue_entry.get_data_model()
        wf_parameters = data_model.get_workflow_parameters()

        def update_solution(selection_table):

            field_widget = selection_table.parameters_widget

            if not field_widget.block_updates:
                # NB Needed in this function (only) as SelectionTable is not
                # a parent l;ike for other fields
                try:
                    field_widget.block_updates = True

                    parameters = field_widget.get_parameters_map()
                    solstr = selection_table.get_value()[0]
                    pgvar = parameters.get("point_group")
                    # space_group = parameters.get("space_group")
                    solution = solutions_dict.get(solstr)
                    lattice = solution.bravaisLattice
                    pglist = lattice2point_group_tags[lattice]
                    pgvalue = pgvar if pgvar and pgvar in pglist else pglist[-1]
                    sgoptions = [""] + crystal_symmetry.space_groups_from_params((lattice,))
                    field_widget.reset_pulldown(
                        "lattice", textChoices=alternative_lattices[lattice]
                    )
                    field_widget.reset_pulldown("point_group", textChoices=pglist)
                    field_widget.reset_pulldown("space_group", textChoices=sgoptions)
                    values = {
                        "lattice": lattice,
                        "point_group": pgvalue,
                        "space_group": ""
                    }
                    field_widget.set_values(**values)
                finally:
                    field_widget.block_updates = False

        def update_lattice(field_widget):
            parameters = field_widget.get_parameters_map()
            lattice = parameters["lattice"]
            pgvar = parameters["point_group"]
            pglist = lattice2point_group_tags[lattice]
            pgvalue = pgvar if pgvar and pgvar in pglist else pglist[-1]
            sgoptions = [""] + crystal_symmetry.space_groups_from_params(
                (lattice,),  point_groups=pgvalue.split("|")
            )
            sgvalue = ""
            field_widget.reset_pulldown(
                "point_group", textChoices=pglist, defaultValue=pgvalue
            )
            field_widget.reset_pulldown(
                "space_group", textChoices=sgoptions, defaultValue=sgvalue
            )

        def update_point_group(field_widget):
            parameters = field_widget.get_parameters_map()
            lattice = parameters["lattice"]
            pgvalue = parameters["point_group"]
            sgoptions = [""] + crystal_symmetry.space_groups_from_params(
                (lattice,),  point_groups=pgvalue.split("|")
            )
            sgvalue = ""
            field_widget.reset_pulldown(
                "space_group", textChoices=sgoptions, defaultValue=sgvalue
            )

        prior_space_group = choose_lattice.priorSpaceGroupString
        header, solutions_dict, select_row = self.parse_indexing_solution(choose_lattice)
        lattice = list(solutions_dict.values())[select_row].bravaisLattice
        point_groups = lattice2point_group_tags[lattice]
        point_group = point_groups[-1]
        if prior_space_group:
            info = crystal_symmetry.CRYSTAL_CLASS_MAP[
                crystal_symmetry.SPACEGROUP_MAP[prior_space_group].crystal_class
            ]
            if info.bravais_lattice == lattice:
                point_group = info.point_group
                if point_group == "32" and info.bravais_lattice == "hP":
                    point_group = info.crystal_class[:-1]
                # if point_group not in point_groups:
                #     point_group = point_groups[-1]
        solutions = list(solutions_dict)

        field_list = []
        field_list.append(
            {
                "variableName": "lattice",
                "uiLabel": "Lattice",
                "type": "combo",
                "defaultValue": lattice or "",
                "textChoices":  [""] + list(lattice2point_group_tags),
                "update_function": update_lattice,
            }
        )
        field_list.append(
            {
                "variableName": "point_group",
                "uiLabel": "Point group",
                "type": "combo",
                "defaultValue": point_group or "",
                "textChoices":  point_groups,
                "update_function": update_point_group,
            }
        )
        field_list.append(
            {
                "variableName": "space_group",
                "uiLabel": "Space group",
                "type": "combo",
                "defaultValue": prior_space_group or "",
                "textChoices":  [""] + crystal_symmetry.space_groups_from_params(
                    lattices=(lattice,)
                ),
            }
        )
        if api.gphl_workflow.getProperty("advanced_mode", False):
            use_cell_for_processing = self.getProperty("use_cell_for_processing", False)
            field_list.append(
                {
                    "variableName": "use_cell_for_processing",
                    "uiLabel": "Use cell and Space Group for processing?",
                    "type": "boolean",
                    "defaultValue": use_cell_for_processing,
                    "readOnly": False,
                }
            )
        field_list.append(
            {
                "variableName": "input_space_group",
                "uiLabel": "Predicted space group",
                "type": "text",
                "defaultValue": data_model.get_input_space_group() or "",
                "readOnly": True,
            }
        )

        field_list[-1]["NEW_COLUMN"] = "True"

        reslimits = api.resolution.get_limits()
        resolution = api.resolution.get_value()
        if None in reslimits:
            reslimits = (0.5, 5.0)
        selection_table = {
            "variableName": "_cplx",
            "uiLabel": "Select indexing solution:",
            "type": "selection_table",
            "header": header,
            "colours": None,
            "defaultValue": (solutions,),
            "update_function": update_solution,
        }
        field_list.append(selection_table)
        field_list.append(
            {
                "variableName": "resolution",
                "uiLabel": "Detector resolution (A)",
                "type": "floatstring",
                "defaultValue": resolution,
                "lowerBound": reslimits[0],
                "upperBound": reslimits[1],
                "decimals": 3,
                "readOnly": False,
            },
        )
        # if not api.energy.read_only:
        display_energy_decimals = int(self.getProperty("display_energy_decimals", 4))
        prev_energy = round(api.energy.get_value(), display_energy_decimals)
        energyLimits = api.energy.get_limits()
        field_list.append(
            {
                "variableName": "energy",
                "uiLabel": "Main aquisition energy (keV)",
                "type": "floatstring",
                "defaultValue": prev_energy,
                "lowerBound": energyLimits[0],
                "upperBound": energyLimits[1],
                "decimals": display_energy_decimals,
                "readOnly": False,
            }
        )

        variants = wf_parameters["variants"]
        if api.gphl_workflow.getProperty("advanced_mode", False):
            choices = variants
        else:
            choices = variants[:2]

        field_list.append(
            {
                "variableName": "variant",
                "uiLabel": "Strategy variant",
                "type": "combo",
                "defaultValue": choices[0],
                "textChoices": choices,
            }
        )

        # Color green (figuratively) if matches lattices,
        lattices = set(
            crystal_symmetry.CRYSTAL_CLASS_MAP[crystal_class].bravais_lattice
            for crystal_class in choose_lattice.priorCrystalClasses
        )
        if "mC" in lattices:
            # NBNB special case. mI non-standard but supported in XDS
            lattices.add("mI")
        colours = [None] * len(solutions)
        if lattices:
            for ii, soltxt in enumerate(solutions):
                bravaisLattice = solutions_dict[soltxt].bravaisLattice
                if any(x == bravaisLattice for x in lattices):
                    colours[ii] = "LIGHT_GREEN"
        selection_table["colours"] = colours
        selection_table["selectRow"] = select_row

        self._return_parameters = gevent.event.AsyncResult()
        responses = dispatcher.send(
            "gphlParametersNeeded", self, field_list, self._return_parameters, None
        )
        if not responses:
            self._return_parameters.set_exception(
                RuntimeError("Signal 'gphlParametersNeeded' is not connected")
            )

        params = self._return_parameters.get()
        if params is StopIteration:
            return StopIteration

        kwArgs = {}

        # if not api.energy.read_only:
        energy = float(params.pop("energy", prev_energy))
        if round(energy, display_energy_decimals) != prev_energy:
            api.energy.set_value(energy, timeout=60)
        wavelength = api.energy._calculate_wavelength(energy)
        role = self._queue_entry.get_data_model().get_beam_energy_tags()[0]
        self.strategyWavelength = GphlMessages.PhasingWavelength(wavelength, role=role)
        kwArgs["strategyWavelength"] = self.strategyWavelength

        new_resolution = float(params.pop("resolution", 0))
        if new_resolution:
            if new_resolution != resolution:
                logging.getLogger("GUI").info(
                    "GphlWorkflow: setting detector distance for resolution %7.3f A"
                    % new_resolution
                )
                # timeout in seconds: max move is ~2 meters, velocity 4 cm/sec
                api.resolution.move(new_resolution, timeout=60)
                resolution = new_resolution

        # orgxy = api.beam_info.get_beam_position()
        orgxy = api.detector.get_beam_centre_pix()
        distance = api.detector.get_distance()
        detectorSetting = GphlMessages.BcsDetectorSetting(
            resolution, id_=None, orgxy=orgxy, Distance=distance
        )
        kwArgs["strategyDetectorSetting"] = detectorSetting
        solution = solutions_dict[ConvertUtils.text_type(params["_cplx"][0])]

        options = {}
        maximum_chi = self.getProperty("maximum_chi")
        if maximum_chi:
            options["maximum_chi"] = float(maximum_chi)
        angular_tolerance = float(self.getProperty("angular_tolerance", 0))
        if angular_tolerance:
            options["angular_tolerance"] = angular_tolerance
            options["clip_kappa"] = float(angular_tolerance)
        options["strategy_type"] = wf_parameters["strategy_type"]
        options["variant"] = variant = params["variant"]
        data_model.set_variant(variant)
        kwArgs["strategyControl"] = json.dumps(options, indent=4, sort_keys=True)
        #
        data_model.lattice_selected = True
        lattice = params.get("lattice") or None
        lattices = (lattice,) if lattice else ()
        pgvar = params.get("point_group") or None
        point_groups = pgvar.split("|") if pgvar else None
        space_group = params.get("space_group") or None
        crystal_classes = crystal_symmetry.crystal_classes_from_params(
            lattices=lattices, point_groups=point_groups, space_group=space_group
        )
        data_model.set_crystal_classes(crystal_classes)
        data_model.set_space_group(space_group)
        use_cell_for_processing = params.pop("use_cell_for_processing", False)
        data_model.set_use_cell_for_processing(space_group and use_cell_for_processing)
        return GphlMessages.SelectedLattice(
            solution=solution,
            crystalClasses=crystal_classes,
            spaceGroup=space_group,
            **kwArgs
        )

    def parse_indexing_solution(self, choose_lattice):
        """

        Args:
            choose_lattice GphlMessages.ChooseLattice:

        Returns: tuple

        """

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

        solutions = choose_lattice.indexingSolutions
        indexing_format = choose_lattice.indexingFormat
        solutions_dict = OrderedDict()

        if indexing_format == "IDXREF":
            header = ["""  LATTICE-  BRAVAIS-   QUALITY  UNIT CELL CONSTANTS (ANGSTROEM & DEGREES)
 CHARACTER  LATTICE     OF FIT      a      b      c   alpha  beta gamma"""]

            line_format = (
                " %s  %2i        %s %12.1f    %6.1f %6.1f %6.1f %5.1f %5.1f %5.1f"
            )
            consistent_solutions = []
            for solution in solutions:
                if solution.isConsistent:
                    char1 = "*"
                    consistent_solutions.append(solution)
                else:
                    char1 = " "
                tpl = (
                    char1,
                    solution.latticeCharacter,
                    solution.bravaisLattice,
                    solution.qualityOfFit,
                )
                solutions_dict[
                    line_format % (tpl + solution.cell.lengths + solution.cell.angles)
                ] = solution

            crystal_classes = (
                choose_lattice.priorCrystalClasses
                or self._queue_entry.get_data_model().get_crystal_classes()
            )
            # Must match bravaisLattices column
            lattices = frozenset(
                crystal_symmetry.CRYSTAL_CLASS_MAP[crystal_class].bravais_lattice
                for crystal_class in crystal_classes
            )
            select_row = None
            if lattices:
                # Select best solution matching lattices
                for idx, solution in enumerate(consistent_solutions):
                    if solution.bravaisLattice in lattices:
                        select_row = idx
                        break

            if select_row is None:
                # No match found, select on solutions only
                lattice = consistent_solutions[-1].bravaisLattice
                for idx, solution in enumerate(consistent_solutions):
                    if solution.bravaisLattice == lattice:
                        select_row = idx
                        break

        else:
            raise RuntimeError("Indexing format %s not supported" % indexing_format)
        #
        return header, solutions_dict, select_row

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
                    "gphlParametersNeeded",
                    self,
                    field_list,
                    self._return_parameters,
                    None,
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
            # Here we DO want to set translation motors,
            # even if MiniKappaCorrection is active
            settings.update(goniostatTranslation.axisSettings)
        centring_queue_entry = self.enqueue_sample_centring(motor_settings=settings)
        goniostatTranslation, dummy = self.execute_sample_centring(
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

        task_label = "Centring (kappa=%0.1f,phi=%0.1f)" % (
            motor_settings.get("kappa"),
            motor_settings.get("kappa_phi"),
        )
        centring_model = queue_model_objects.SampleCentring(
            name=task_label, motor_positions=motor_settings
        )
        self._add_to_queue(self._data_collection_group, centring_model)
        centring_entry = queue_manager.get_entry_with_model(centring_model)
        centring_entry.in_queue = in_queue

        return centring_entry

    def collect_centring_snapshots(self, file_name_prefix="snapshot"):
        """

        :param file_name_prefix: str
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
                snapshot_filename = filename_template % (
                    file_name_prefix,
                    timestamp,
                    snapshot_index + 1,
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
            return (
                GphlMessages.GoniostatTranslation(
                    rotation=goniostatRotation,
                    requestedRotationId=requestedRotationId,
                    **dd0
                ),
                positionsDict,
            )
        else:
            self.abort("No Centring result found")

    def prepare_for_centring(self, payload, correlation_id):

        # TODO Add pop-up confirmation box ('Ready for centring?')

        return GphlMessages.ReadyForCentring()

    def obtain_prior_information(self, payload, correlation_id):

        workflow_model = self._queue_entry.get_data_model()
        sample_model = workflow_model.get_sample_node()

        # cell_params = workflow_model.get_cell_parameters()
        crystalObj = sample_model.crystals[0]
        cell_params = list(
            getattr(crystalObj, tag)
            for tag in (
                "cell_a",
                "cell_b",
                "cell_c",
                "cell_alpha",
                "cell_beta",
                "cell_gamma",
            )
        )
        if all(cell_params):
            unitCell = GphlMessages.UnitCell(*cell_params)
        else:
            unitCell = None

        # NB Expected resolution is deprecated.
        # It is set to the current resolution value, for now
        userProvidedInfo = GphlMessages.UserProvidedInfo(
            scatterers=(),
            crystalClasses=workflow_model.get_crystal_classes(),
            spaceGroup= workflow_model.get_space_group(),
            cell=unitCell,
            expectedResolution=api.collect.get_resolution(),
            isAnisotropic=None,
        )
        ll0 = ["PriorInformation"]
        for tag in (
            "expectedResolution",
            "isAnisotropic",
            "crystalClasses",
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
            rootDirectory=image_root,
            userProvidedInfo=userProvidedInfo,
        )
        #
        return priorInformation

    # Utility functions

    def resolution2dose_budget(self, resolution, decay_limit):
        """

        Args:
            resolution (float): resolution in A
            decay_limit (float): min. intensity at resolution edge at experiment end (%)

        Returns (float): Dose budget (MGy)

        """
        """Get resolution-dependent dose budget using configured values"""
        max_budget = self.getProperty("maximum_dose_budget", 20)
        result = 2 * resolution * resolution * math.log(100.0 / decay_limit)
        #
        return min(result, max_budget)

    def get_emulation_samples(self):
        """Get list of lims_sample information dictionaries for mock/emulation

        Returns: LIST[DICT]

        """
        crystal_file_name = "crystal.nml"
        result = []
        sample_dir = api.gphl_connection.software_paths.get("gphl_test_samples")
        serial = 0
        if sample_dir and os.path.isdir(sample_dir):
            for path, dirnames, filenames in sorted(os.walk(sample_dir)):
                if crystal_file_name in filenames:
                    data = {}
                    sample_name = os.path.basename(path)
                    indata = f90nml.read(os.path.join(path, crystal_file_name))[
                        "simcal_crystal_list"
                    ]
                    space_group = indata.get("sg_name")
                    cell_lengths = indata.get("cell_dim")
                    cell_angles = indata.get("cell_ang_deg")
                    resolution = indata.get("res_limit_def")

                    location = (serial // 10 + 1, serial % 10 + 1)
                    serial += 1
                    data["containerSampleChangerLocation"] = str(location[0])
                    data["sampleLocation"] = str(location[1])

                    data["sampleName"] = sample_name
                    if cell_lengths:
                        for ii, tag in enumerate(("cellA", "cellB", "cellC")):
                            data[tag] = cell_lengths[ii]
                    if cell_angles:
                        for ii, tag in enumerate(
                            ("cellAlpha", "cellBeta", "cellGamma")
                        ):
                            data[tag] = cell_angles[ii]
                    if space_group:
                        data["crystalSpaceGroup"] = space_group

                    data["experimentType"] = "Default"
                    data["proteinAcronym"] = self.TEST_SAMPLE_PREFIX
                    data["smiles"] = None
                    data["sampleId"] = 100000 + serial

                    # ISPyB docs:
                    # experimentKind: enum('Default','MAD','SAD','Fixed','OSC',
                    # 'Ligand binding','Refinement', 'MAD - Inverse Beam','SAD - Inverse Beam',
                    # 'MXPressE','MXPressF','MXPressO','MXPressP','MXPressP_SAD','MXPressI','MXPressE_SAD','MXScore','MXPressM',)
                    #
                    # Use "Mad, "SAD", "OSC"
                    dfp = data["diffractionPlan"] = {
                        # "diffractionPlanId": 457980,
                        "experimentKind": "Default",
                        "numberOfPositions": 0,
                        "observedResolution": 0.0,
                        "preferredBeamDiameter": 0.0,
                        "radiationSensitivity": 1.0,
                        "requiredCompleteness": 0.0,
                        "requiredMultiplicity": 0.0,
                        # "requiredResolution": 0.0,
                    }
                    dfp["requiredResolution"] = resolution
                    dfp["diffractionPlanId"] = 5000000 + serial

                    dd0 = EMULATION_DATA.get(sample_name, {})
                    for tag, val in dd0.items():
                        if tag in data:
                            data[tag] = val
                        elif tag in dfp:
                            dfp[tag] = val
                    #
                    result.append(data)
        #
        return result

    def get_emulation_crystal_data(self, sample_name=None):
        """If sample is a test data set for emulation, get crystal data

        Returns:
            Optional[dict]
        """
        if sample_name is None:
            sample_name = (
                self._queue_entry.get_data_model().get_sample_node().get_name()
            )

        crystal_data = None
        hklfile = None
        if sample_name and sample_name.startswith(self.TEST_SAMPLE_PREFIX):
            sample_name = sample_name[len(self.TEST_SAMPLE_PREFIX) + 1 :]

            sample_dir = api.gphl_connection.software_paths.get("gphl_test_samples")
            if not sample_dir:
                raise ValueError("Test sample requires gphl_test_samples dir specified")
            sample_dir = os.path.join(sample_dir, sample_name)
            if not os.path.isdir(sample_dir):
                raise ValueError("Sample data directory %s does not exist" % sample_dir)
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
