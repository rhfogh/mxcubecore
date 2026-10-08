# encoding: utf-8
"""Collection emulator, calling an external program to generate data images.
Written originally for Global Phasing simcal,
but could be made to work with other systems

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

import os
import re
import subprocess
import logging
import sys
import json
import jsonschema
import copy

from collections import OrderedDict
import f90nml

from mxcubecore import HardwareRepository as HWR
from mxcubecore.HardwareObjects.mockup.CollectMockup import CollectMockup
from mxcubecore.TaskUtils import task
from mxcubecore.utils import conversion

sys.path.insert(0, "/usr/local/experimental_methods")

from useful_routines import (
    adjust_filename_for_ispyb,
    DEFAULT_BROKER_PORT,
    get_string_from_timestamp,
    get_redis_connection,
)

try:
    from speech import speech
    from xray_experiment import xray_experiment
    from omega_scan import omega_scan
    from inverse_scan import inverse_scan
    from reference_images import reference_images
    from helical_scan import helical_scan
    from fluorescence_spectrum import fluorescence_spectrum
    from energy_scan import energy_scan
    from diffraction_tomography import diffraction_tomography

    # from xray_centring import xray_centring
    from raster_scan import raster_scan
    from nested_helical_acquisition import nested_helical_acquisition
    from tomography import tomography
    from film import film
    from slits import slits1, slits2, slits3, slits5, slits6

except ModuleNotFoundError:
    from experimental_methods import (
        speech,
        xray_experiment,
        omega_scan,
        inverse_scan,
        reference_images,
        helical_scan,
        raster_scan,
        diffraction_tomography,
        nested_helical_acquisition,
        tomography,
        film,
        fluorescence_spectrum,
        energy_scan,
        slits1, slits2, slits3, slits5, slits6
    )

__copyright__ = """ Copyright © 2017 - 2026 by Global Phasing Ltd. """
__license__ = "LGPLv3+"
__author__ = "Martin Savko based on CollectEmulator by Rasmus H Fogh"


class PX2Collect(CollectMockup, speech):

    def __init__(self, name):
        CollectMockup.__init__(self, name)
        speech.__init__(self, port=DEFAULT_BROKER_PORT, service="collect", verbose=False)

        self.log = logging.getLogger("HWR")

        self.xe = xray_experiment(directory="/tmp", name_pattern=f"xe_{get_string_from_timestamp()}")

        self.current_dc_parameters = None
        self.osc_id = None
        self.owner = None
        self.aborted_by_user = None
        self.slits1 = slits1()

        self._counter = 1
        self.redis = get_redis_connection()
        self.collection_id = -1

    def _get_simcal_input(self, data_collect_parameters, crystal_data):
        """Get ordered dict with simcal input from available data"""

        # Set up and add crystal data
        result = OrderedDict()
        setup_data = result["setup_list"] = crystal_data
        instrument_data = HWR.beamline.gphl_workflow.instrument_data

        #
        # # update with instrument data
        #
        sweep_count = len(data_collect_parameters["oscillation_sequence"])

        # Setting parameters in order (may not be necessary, but ...)
        # Missing: *mu*
        remap = {
            "beam": "nominal_beam_dir",
            "det_coord_def": "det_org_dist",
            "cone_s_height": "cone_height",
        }
        tags = (
            "lambda_sd",
            "beam",
            "beam_sd_deg",
            "pol_plane_n",
            "pol_frac",
            "d_sensor",
            "min_zeta",
            "det_name",
            "det_x_axis",
            "det_y_axis",
            "det_qx",
            "det_qy",
            "det_nx",
            "det_ny",
            # "det_org_x",
            # "det_org_y",
            "det_coord_def",
        )
        for tag in tags:
            val = instrument_data.get(remap.get(tag, tag))
            if val is not None:
                setup_data[tag] = val

        (
            setup_data["det_org_x"],
            setup_data["det_org_y"],
        ) = HWR.beamline.detector.get_beam_position()

        ll0 = list(HWR.beamline.gphl_workflow.rotation_axes.values())
        setup_data["omega_axis"] = ll0[0]
        setup_data["kappa_axis"] = ll0[1]
        setup_data["phi_axis"] = ll0[2]
        ll0 = list(HWR.beamline.gphl_workflow.translation_axes.values())
        setup_data["trans_x_axis"] = ll0[0]
        setup_data["trans_y_axis"] = ll0[1]
        setup_data["trans_z_axis"] = ll0[2]
        tags = (
            "cone_radius",
            "cone_s_height",
            "beam_stop_radius",
            "beam_stop_s_length",
            "beam_stop_s_distance",
        )
        for tag in tags:
            val = instrument_data.get(remap.get(tag, tag))
            if val is not None:
                setup_data[tag] = val

        # Add/overwrite parameters from emulator configuration
        conv = conversion.convert_string_value
        for key, val in self.config.simcal_parameters.items():
            setup_data[key] = conv(val)

        setup_data["n_vertices"] = 0
        setup_data["n_triangles"] = 0
        setup_data["n_orients"] = 0
        setup_data["n_sweeps"] = sweep_count

        # Add segments
        segments = HWR.beamline.gphl_workflow.detector_segments
        if segments:
            if isinstance(segments, dict):
                segment_count = 1
            else:
                segment_count = len(segments)
            setup_data["n_segments"] = segment_count
            result["segment_list"] = segments

        # Adjustments
        val = instrument_data.get("beam")
        if val:
            setup_data["beam"] = val

        # update with diffractcal data
        # TODO check that this works also for updating segment list
        fp0 = HWR.beamline.gphl_workflow.file_paths.get("diffractcal_file")
        if os.path.isfile(fp0):
            diffractcal_data = f90nml.read(fp0)["sdcp_instrument_list"]
            for tag in setup_data.keys():
                val = diffractcal_data.get(tag)
                if val is not None:
                    setup_data[tag] = val
            ll0 = diffractcal_data["gonio_axis_dirs"]
            setup_data["omega_axis"] = ll0[:3]
            setup_data["kappa_axis"] = ll0[3:6]
            setup_data["phi_axis"] = ll0[6:]

        # get resolution limit and detector distance
        detector_distance = data_collect_parameters.get("detector_distance", 0.0)
        if not detector_distance:
            resolution = data_collect_parameters.get("resolution")
            if resolution:
                self.set_resolution(resolution)
            detector_distance = HWR.beamline.detector.distance.get_value()
        # Add sweeps
        sweeps = []
        compress_data = False
        for osc in data_collect_parameters["oscillation_sequence"]:
            motors = data_collect_parameters["motors"]
            sweep = OrderedDict()

            energy = (
                data_collect_parameters.get("energy") or HWR.beamline.energy.get_value()
            )
            sweep["lambda"] = conversion.HC_OVER_E / energy
            sweep["res_limit"] = setup_data["res_limit_def"]
            sweep["exposure"] = osc["exposure_time"]
            ll0 = list(HWR.beamline.gphl_workflow.translation_axes)
            sweep["trans_xyz"] = list(motors.get(x) or 0.0 for x in ll0)
            sweep["det_coord"] = detector_distance
            # NBNB hardwired for omega scan TODO
            sweep["axis_no"] = 3
            sweep["omega_deg"] = osc["start"]
            # NB kappa and phi are overwritten from the motors dict, if set there
            sweep["kappa_deg"] = osc["kappaStart"]
            sweep["phi_deg"] = osc["phiStart"]
            sweep["step_deg"] = osc["range"]
            sweep["n_frames"] = osc["number_of_images"]
            sweep["image_no"] = osc["start_image_number"]
            # self.make_image_file_template(data_collect_parameters, suffix='cbf')

            # Extract format statement from template,
            # and convert to fortran format
            text_type = conversion.text_type
            template = text_type(data_collect_parameters["fileinfo"]["template"])
            ss0 = re.search("(%[0-9]+d)", template).group(0)
            template = template.replace(ss0, "?" * int(ss0[1:-1]))
            name_template = os.path.join(
                text_type(data_collect_parameters["fileinfo"]["directory"]),
                template,
                # data_collect_parameters['fileinfo']['template']
            )
            # We still use the normal name template for compressed data
            if name_template.endswith(".gz"):
                compress_data = True
                name_template = name_template[:-3]
            sweep["name_template"] = name_template

            # Overwrite kappa and phi from motors - if set
            val = motors.get("kappa")
            if val is not None:
                sweep["kappa_deg"] = val
            val = motors.get("kappa_phi")
            if val is not None:
                sweep["phi_deg"] = val

            # Skipped: spindle_deg=0.0, two_theta_deg=0.0, mu_air=-1, mu_sensor=-1

            sweeps.append(sweep)

        if len(sweeps) == 1:
            # NBNB in current code we can have only one sweep here,
            # but it will work for multiple
            result["sweep_list"] = sweep
        else:
            result["sweep_list"] = sweeps
        return result, compress_data

    @task
    def data_collection_hook_simcal(self):
        """Spawns data emulation using gphl simcal"""
        data_model = HWR.beamline.gphl_workflow._queue_entry.get_data_model()
        if data_model.skip_collection:
            return

        data_collect_parameters = self.current_dc_parameters

        if not HWR.beamline.gphl_workflow:
            raise ValueError("Emulator requires GPhL workflow installation")
        gphl_connection = HWR.beamline.gphl_connection
        if not gphl_connection:
            raise ValueError("Emulator requires GPhL connection installation")

        # Get program locations
        simcal_executive = gphl_connection.get_executable("simcal")
        simcal_licence_dir = (
            gphl_connection.get_bdg_licence_dir("simcal")
            or gphl_connection.config.software_paths["GPHL_INSTALLATION"]
        )
        # # Get environmental variables.
        envs = {"autoPROC_home": simcal_licence_dir}
        GPHL_XDS_PATH = gphl_connection.config.software_paths.get("GPHL_XDS_PATH")
        if GPHL_XDS_PATH:
            envs["GPHL_XDS_PATH"] = GPHL_XDS_PATH
        GPHL_CCP4_PATH = gphl_connection.config.software_paths.get("GPHL_CCP4_PATH")
        if GPHL_CCP4_PATH:
            envs["GPHL_CCP4_PATH"] = GPHL_CCP4_PATH
        text_type = conversion.text_type
        for tag, val in self.config.environment_variables.items():
            envs[text_type(tag)] = text_type(val)

        # get crystal data
        crystal_data, hklfile = HWR.beamline.gphl_workflow.get_emulation_crystal_data()

        input_data, compress_data = self._get_simcal_input(
            data_collect_parameters, crystal_data
        )

        # NB outfile is the echo output of the input file;
        # image files templates are set in the input file
        file_info = data_collect_parameters["fileinfo"]
        if not os.path.exists(file_info["directory"]):
            os.makedirs(file_info["directory"])
        infile = os.path.join(
            file_info["directory"], "simcal_in_%s.nml" % self._counter
        )

        f90nml.write(input_data, infile, force=True)
        outfile = os.path.join(
            file_info["directory"], "simcal_out_%s.nml" % self._counter
        )
        logfile = os.path.join(
            file_info["directory"], "simcal_log_%s.txt" % self._counter
        )
        self._counter += 1
        command_list = [
            simcal_executive,
            "--input",
            infile,
            "--output",
            outfile,
            "--hkl",
            hklfile,
        ]

        for tag, val in self.config.simcal_options.items():
            command_list.extend(conversion.command_option(tag, val, prefix="--"))
        self.log.info("Executing command: %s", " ".join(command_list))
        self.log.info("Executing environment: %s", sorted(envs.items()))

        if compress_data:
            command_list.append("--gzip-img")

        fp1 = open(logfile, "w")
        fp2 = subprocess.STDOUT
        # resource.setrlimit(resource.RLIMIT_STACK, (-1,-1))

        def set_ulimit():
            import resource

            resource.setrlimit(
                resource.RLIMIT_STACK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY)
            )

        try:
            self.emit("collectStarted", (None, 1))
            running_process = subprocess.Popen(
                command_list, stdout=fp1, stderr=fp2, env=envs, preexec_fn=set_ulimit
            )
            gphl_connection.collect_emulator_process = running_process

            # # NBNB leaving this in causes simulations to be killed and missing images
            # super(CollectEmulator, self).data_collection_hook()

            self.log.info("Waiting for simcal collection emulation.")
            # NBNB TODO put in time-out, somehow
            return_code = running_process.wait()
        except Exception:
            self.log.error("Error in GPhL collection emulation")
            raise
        finally:
            fp1.close()
        process = gphl_connection.collect_emulator_process
        gphl_connection.collect_emulator_process = None
        if process == "ABORTED":
            self.log.info("Simcal collection emulation aborted")
        elif return_code:
            raise RuntimeError(
                "simcal process terminated with return code %s" % return_code
            )
        else:
            self.log.info("Simcal collection emulation successful")
        self.ready_event.set()
        return

    def data_collection_hook(self):

        parameters = self.current_dc_parameters

        for parameter in parameters:
            self.log.info(
                "PX2Collect %s: %s" % (str(parameter), str(parameters[parameter]))
            )

        shutterless = parameters["shutterless"]

        osc_seq = parameters["oscillation_sequence"][0]

        motors = parameters["motors"]
        aligned_position = self.translate_position(
            motors
        )  # HWR.beamline.diffractometer.translate_from_mxcube_to_md(motors)
        # if 'centred_position' in osc_seq:
        # aligned_position = self.translate_position(self.centred_position)
        self.log.info("PX2Collect aligned_position: %s" % aligned_position)

        fileinfo = parameters["fileinfo"]
        sample_reference = parameters["sample_reference"]
        experiment_type = parameters["experiment_type"]
        if experiment_type == "OSC" and osc_seq["num_triggers"] > 1:
            experiment_type = "Characterization"
        energy = parameters["energy"]
        if energy < 1.0e3:
            energy *= 1.0e3
        photon_energy = energy
        transmission = parameters["transmission"]
        if isinstance(parameters["resolution"], dict):
            resolution = parameters["resolution"]["upper"]
        else:
            resolution = parameters["resolution"]
        exposure_time = osc_seq["exposure_time"]
        in_queue = parameters["in_queue"] != False

        overlap = -osc_seq["offset"]
        angle_per_frame = osc_seq["range"]
        scan_start_angle = osc_seq["start"]
        number_of_images = osc_seq["number_of_images"]
        image_nr_start = osc_seq["start_image_number"]

        directory = str(fileinfo["directory"].strip("\n"))

        prefix = str(fileinfo["prefix"])
        template = str(fileinfo["template"])
        run_number = fileinfo["run_number"]
        process_directory = fileinfo["process_directory"]

        if parameters["processing"] in ["True", True]:
            do_auto_analysis = True

        name_pattern = f"{prefix}_{run_number:d}"

        self.log.info("PX2Collect experiment_type: %s" % (experiment_type,))

        try:
            capi = HWR.beamline.sample_changer.cats_api
        except:
            capi = None

        if experiment_type == "OSC":
            self.emit("progressInit", ("Collection", 100, False))
            self.log.info("PX2Collect: executing omega_scan")
            scan_range = angle_per_frame * number_of_images
            scan_exposure_time = exposure_time * number_of_images
            self.log.info(
                "PX2Collect: omega_scan parameters:\n\tname_pattern: %s\
                                                        \n\tdirectory: %s\
                                                        \n\tposition: %s\
                                                        \n\tscan_range: %.2f\
                                                        \n\tscan_exposure_time: %.2f\
                                                        \n\tscan_start_angle: %.2f\
                                                        \n\tangle_per_frame: %.2f\
                                                        \n\timage_nr_start: %d\
                                                        \n\tphoton_energy: %.2f\
                                                        \n\ttransmission: %.2f\
                                                        \n\tresolution: %.2f\
                                                        \n\tprocessing: %s\
                                                        \n\tsample_reference %s"
                % (
                    name_pattern,
                    directory,
                    aligned_position,
                    scan_range,
                    scan_exposure_time,
                    scan_start_angle,
                    angle_per_frame,
                    image_nr_start,
                    photon_energy,
                    transmission,
                    resolution,
                    do_auto_analysis,
                    sample_reference,
                )
            )

            experiment = omega_scan(
                name_pattern,
                directory,
                position=aligned_position,
                scan_range=scan_range,
                scan_exposure_time=scan_exposure_time,
                scan_start_angle=scan_start_angle,
                angle_per_frame=angle_per_frame,
                image_nr_start=image_nr_start,
                photon_energy=energy,
                transmission=transmission,
                resolution=resolution,
                simulation=False,
                diagnostic=False,
                analysis=do_auto_analysis,
                parent=self,
                cats_api=capi,
                actuators_dictionary=self.xe.monitors_dictionary,
                monitors_dictionary=self.xe.actuators_dictionary,
            )

        elif experiment_type == "Characterization":
            self.emit("progressInit", ("Characterization", 100, False))
            self.log.debug("PX2Collect: executing reference_images")
            if osc_seq["num_triggers"] != 0:
                number_of_wedges = osc_seq["num_triggers"]
            else:
                number_of_wedges = max(osc_seq["number_of_images"], 4)
            try:
                wedge_size = osc_seq["num_images_per_trigger"]
                if wedge_size == 0:
                    wedge_size = 10

            except KeyError:
                wedge_size = 10
            try:
                range_per_frame_ref = osc_seq["range_per_frame"]
            except KeyError:
                if osc_seq["range"] >= 0.95:
                    range_per_frame_ref = float(osc_seq["range"]) / wedge_size
                else:
                    range_per_frame_ref = float(osc_seq["range"])

            angle_per_frame = range_per_frame_ref
            number_of_images = wedge_size
            range_per_scan = angle_per_frame * number_of_images

            scan_start_angles = []
            scan_exposure_time = exposure_time * number_of_images

            scan_range = range_per_frame_ref * wedge_size
            # scan_range = angle_per_frame * number_of_images
            if osc_seq["offset"] != 0:
                overlap = -osc_seq["offset"]
            else:
                step = 360 / wedge_size
                overlap = -step + scan_range

            for k in range(number_of_wedges):
                scan_start_angles.append(
                    scan_start_angle + k * -overlap + k * scan_range
                )

            self.log.info(
                "PX2Collect: reference_images parameters:\
                                                        \n\tname_pattern: %s\
                                                        \n\tdirectory: %s\
                                                        \n\tposition: %s\
                                                        \n\tscan_range: %.2f\
                                                        \n\tscan_exposure_time: %.2f\
                                                        \n\tscan_start_angles: %s\
                                                        \n\tangle_per_frame: %.2f\
                                                        \n\timage_nr_start: %d\
                                                        \n\tphoton_energy: %.2f\
                                                        \n\ttransmission: %.2f\
                                                        \n\tresolution: %.2f"
                % (
                    name_pattern,
                    directory,
                    aligned_position,
                    scan_range,
                    scan_exposure_time,
                    str(scan_start_angles),
                    angle_per_frame,
                    image_nr_start,
                    photon_energy,
                    transmission,
                    resolution,
                )
            )
            experiment = reference_images(
                name_pattern,
                directory,
                position=aligned_position,
                scan_range=scan_range,
                scan_exposure_time=scan_exposure_time,
                scan_start_angles=scan_start_angles,
                angle_per_frame=angle_per_frame,
                image_nr_start=image_nr_start,
                photon_energy=energy,
                transmission=transmission,
                resolution=resolution,
                simulation=False,
                diagnostic=False,
                analysis=do_auto_analysis,
                generate_sum=True,
                parent=self,
                cats_api=capi,
                actuators_dictionary=self.xe.monitors_dictionary,
                monitors_dictionary=self.xe.actuators_dictionary,
            )

        elif experiment_type == "Helical" and osc_seq["mesh_range"] == ():
            self.emit("progressInit", ("Helical scan", 100, False))
            self.log.info("PX2Collect: executing helical_scan")
            scan_range = angle_per_frame * number_of_images
            scan_exposure_time = exposure_time * number_of_images
            self.log.info("helical_pos %s" % self.helical_pos)
            self.log.info(
                "PX2Collect: helical_scan parameters:\
                                                        \n\tname_pattern: %s\
                                                        \n\tdirectory: %s\
                                                        \n\tscan_range: %.2f\
                                                        \n\tscan_exposure_time: %.2f\
                                                        \n\tscan_start_angle: %.2f\
                                                        \n\tangle_per_frame: %.2f\
                                                        \n\timage_nr_start: %d\
                                                        \n\tphoton_energy: %.2f\
                                                        \n\ttransmission: %.2f\
                                                        \n\tresolution: %.2f"
                % (
                    name_pattern,
                    directory,
                    scan_range,
                    scan_exposure_time,
                    scan_start_angle,
                    angle_per_frame,
                    image_nr_start,
                    photon_energy,
                    transmission,
                    resolution,
                )
            )
            experiment = helical_scan(
                name_pattern,
                directory,
                scan_range=scan_range,
                scan_exposure_time=scan_exposure_time,
                scan_start_angle=scan_start_angle,
                angle_per_frame=angle_per_frame,
                image_nr_start=image_nr_start,
                position_start=self.translate_position(self.helical_pos["1"]),
                position_end=self.translate_position(self.helical_pos["2"]),
                photon_energy=energy,
                transmission=transmission,
                resolution=resolution,
                simulation=False,
                diagnostic=False,
                analysis=do_auto_analysis,
                parent=self,
                cats_api=capi,
                actuators_dictionary=self.xe.monitors_dictionary,
                monitors_dictionary=self.xe.actuators_dictionary,
            )

        elif experiment_type == "Helical" and osc_seq["mesh_range"] != ():
            self.emit("progressInit", ("X-ray centring", 100, False))
            self.log.info("PX2Collect: executing xray_centring")
            horizontal_range, vertical_range = osc_seq["mesh_range"]
            number_of_lines = osc_seq["number_of_lines"]
            scan_range = angle_per_frame * number_of_lines

            self.log.info(
                "PX2Collect: xray_centring parameters:\
                                                        \n\tname_pattern: %s\
                                                        \n\tdirectory: %s\
                                                        \n\tscan_range: %.2f\
                                                        \n\tscan_exposure_time: %.3f\
                                                        \n\tscan_start_angle: %.2f\
                                                        \n\tangle_per_frame: %.2f\
                                                        \n\timage_nr_start: %d\
                                                        \n\tphoton_energy: %.2f\
                                                        \n\ttransmission: %.2f\
                                                        \n\tresolution: %.2f"
                % (
                    name_pattern,
                    directory,
                    scan_range,
                    scan_exposure_time,
                    scan_start_angle,
                    angle_per_frame,
                    image_nr_start,
                    photon_energy,
                    transmission,
                    resolution,
                )
            )
            experiment = xray_centring(
                name_pattern, directory, diagnostic=False, parent=self
            )

        elif experiment_type == "Mesh":
            self.emit("progressInit", ("Mesh scan", 100, False))
            self.log.info("PX2Collect: executing raster_scan")
            number_of_rows = int(osc_seq["number_of_lines"])
            number_of_columns = int(number_of_images / number_of_rows)
            horizontal_range, vertical_range = osc_seq["mesh_range"]
            # aligned_position = self.translate_position(osc_seq['centred_position'])
            angle_per_line = angle_per_frame * number_of_rows

            if shutterless == True:
                scan_range = angle_per_frame * number_of_rows
            else:
                scan_range = angle_per_frame

            if scan_range == 0:
                scan_range = 0.01

            try:
                nimages_per_point = int(self.redis.get("nimages_per_point"))
            except:
                nimages_per_point = 1
            try:
                npasses = int(self.redis.get("npasses"))
            except:
                npasses = 1
            try:
                dark_time_between_passes = float(
                    self.redis.get("dark_time_between_passes")
                )
            except:
                dark_time_between_passes = 0.0
            try:
                fast_axis = self.redis.get("fast_axis").decode()
            except:
                fast_axis = "vertical"
            if fast_axis is None:
                fast_axis = "vertical"

            self.log.info(
                "PX2Collect: raster_scan parameters:\
                                                        \n\tname_pattern: %s\
                                                        \n\tdirectory: %s\
                                                        \n\tvertical_range: %.2f\
                                                        \n\thorizontal_range: %.2f\
                                                        \n\tnumber_of_rows: %d\
                                                        \n\tnumber_of_columns: %d\
                                                        \n\tframe_time: %.2f\
                                                        \n\tscan_start_angle: %.2f\
                                                        \n\tscan_range: %.2f\
                                                        \n\timage_nr_start: %d\
                                                        \n\tphoton_energy: %.2f\
                                                        \n\ttransmission: %.2f\
                                                        \n\tresolution: %.2f\
                                                        \n\tshutterless: %s\
                                                        \n\tfast_axis: %s\
                                                        \n\tnimages_per_scan: %d\
                                                        \n\tnpasses: %d\
                                                        \n\tdark_time_between_passes: %.2f"
                % (
                    name_pattern,
                    directory,
                    vertical_range,
                    horizontal_range,
                    number_of_rows,
                    number_of_columns,
                    exposure_time,
                    scan_start_angle,
                    scan_range,
                    image_nr_start,
                    photon_energy,
                    transmission,
                    resolution,
                    shutterless,
                    fast_axis,
                    nimages_per_point,
                    npasses,
                    dark_time_between_passes,
                )
            )

            experiment = raster_scan(
                name_pattern,
                directory,
                vertical_range,
                horizontal_range,
                position=aligned_position,
                number_of_rows=number_of_rows,
                number_of_columns=number_of_columns,
                frame_time=exposure_time,
                scan_start_angle=scan_start_angle,
                scan_range=scan_range,
                image_nr_start=image_nr_start,
                photon_energy=energy,
                transmission=transmission,
                resolution=resolution,
                shutterless=shutterless,
                scan_axis=str(fast_axis),
                nimages_per_scan=nimages_per_point,
                npasses=npasses,
                dark_time_between_passes=dark_time_between_passes,
                use_centring_table=True,
                simulation=False,
                diagnostic=False,
                analysis=True,
                parent=self,
                cats_api=capi,
                actuators_dictionary=self.xe.monitors_dictionary,
                monitors_dictionary=self.xe.actuators_dictionary,
            )

        self.current_dc_parameters["undulatorGap1"] = experiment.get_undulator_gap_encoder_position()
        print(10*"\n")
        print("undulatorGap1", self.current_dc_parameters["undulatorGap1"])
        print(10*"\n")

        self.experiment = experiment

        if self.experiment._stop_flag == False:
            self.experiment.execute()

            if do_auto_analysis == True:
                try:
                    self.run_analysis(self.processing_filename)
                except:
                    pass
        self.ready_event.set()
        return

    def get_processing_filename(self, parameters):
        # save a json file for the autoprocessing
        execute_XDSME = eval(self.redis.get("XDSME"))
        execute_autoPROC = eval(self.redis.get("autoPROC"))
        autoproc_options = {"xdsme": execute_XDSME, "autoproc": execute_autoPROC}
        # print('type(parameters)', type(parameters))

        processing_parameters = dict(parameters)

        processing_parameters["collection_id"] = self.collection_id
        processing_parameters["autoproc_options"] = {
            "xdsme": execute_XDSME,
            "autoproc": execute_autoPROC,
        }

        directory = parameters["fileinfo"]["directory"].replace("RAW_DATA", "ARCHIVE")
        if not os.path.isdir(directory):
            try:
                os.makedirs(directory)
            except:
                print(f"Could not create {directory}, please check")

        prefix = parameters["fileinfo"]["prefix"]
        run_number = parameters["fileinfo"]["run_number"]
        filename = f"{prefix}_{run_number}_processing.json"
        processing_filename = os.path.join(directory, filename)

        jsonstr = json.dumps(processing_parameters)
        fd = open(processing_filename, "wb")
        fd.write(jsonstr.encode("utf-8"))
        fd.close()

        return processing_filename

    def get_collection_id(self):
        return self.collection_id

    def run_analysis(self, processing_filename):
        line = f"autoprocessing-px2 {processing_filename} &"
        self.log.info(f"executing {line}")
        os.system(line)

    def translate_position(self, position):
        return HWR.beamline.diffractometer.translate_from_mxcube_to_md(position)

    def set_image_quality_indicators_plot(self, collection_id, directory, name_pattern, cartography_filename, csv_filename):
        os.system(f"/usr/local/conda/envs/murko_3.11/bin/python /usr/local/experimental_methods/diffraction_experiment_analysis.py -d {directory} -n {name_pattern} &")

        if HWR.beamline.lims:
            HWR.beamline.lims.set_image_quality_indicators_plot(
                collection_id,
                cartography_filename,
                csv_filename,
            )