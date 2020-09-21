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
[Name] ALBACollect

[Description]
Specific implementation of the collection methods for ALBA synchrotron.

[Signals]
- progressInit
- collectConnected
- collectStarted
- collectReady
- progressStop
- collectOscillationFailed
"""

from __future__ import print_function

import os
import sys
import time
import gevent
import logging

from HardwareRepository.TaskUtils import task
from AbstractCollect import AbstractCollect
from taurus.core.tango.enums import DevState
from xaloc.resolution import get_dettaby, get_resolution

__credits__ = ["ALBA Synchrotron"]
__version__ = "2.3"
__category__ = "General"


class ALBACollect(AbstractCollect):
    """Main data collection class. Inherited from AbstractMulticollect class
       Collection is done by setting collection parameters and
       executing collect command
    """

    def __init__(self, name):
        AbstractCollect.__init__(self, name)
        self.logger = logging.getLogger("HWR.ALBACollect")
        self.supervisor_hwobj = None
        self.fastshut_hwobj = None
        self.slowshut_hwobj = None
        self.photonshut_hwobj = None
        self.frontend_hwobj = None
        self.diffractometer_hwobj = None
        self.omega_hwobj = None
        self.lims_client_hwobj = None
        self.machine_info_hwobj = None
        self.energy_hwobj = None
        self.resolution_hwobj = None
        self.transmission_hwobj = None
        self.detector_hwobj = None
        self.beam_info_hwobj = None
        self.graphics_manager_hwobj = None
        self.autoprocessing_hwobj = None
        self.flux_hwobj = None

        self.cmd_ni_conf = None
        self.cmd_ni_unconf = None

        self.chan_kappa_pos = None
        self.chan_phi_pos = None

        self.chan_undulator_gap = None

        self._error_msg = ""
        self.owner = None
        self.osc_id = None
        self._collecting = None

        self.omega_hwobj = None
        self.graphics_manager_hwobj = None

        self.helical_positions = None
#        self.saved_omega_velocity = None

        self.omega_init_pos = None

        self.bypass_shutters = False

    def init(self):
        self.logger.debug("Initializing {0}".format(self.__class__.__name__))
        self.ready_event = gevent.event.Event()

        self.supervisor_hwobj = self.getObjectByRole("supervisor")
        self.fastshut_hwobj = self.getObjectByRole("fast_shutter")
        self.slowshut_hwobj = self.getObjectByRole("slow_shutter")
        self.photonshut_hwobj = self.getObjectByRole("photon_shutter")
        self.frontend_hwobj = self.getObjectByRole("frontend")
        self.diffractometer_hwobj = self.getObjectByRole("diffractometer")
        self.omega_hwobj = self.getObjectByRole("omega")
        self.lims_client_hwobj = self.getObjectByRole("lims_client")
        self.machine_info_hwobj = self.getObjectByRole("machine_info")
        self.energy_hwobj = self.getObjectByRole("energy")
        self.resolution_hwobj = self.getObjectByRole("resolution")
        self.transmission_hwobj = self.getObjectByRole("transmission")
        self.detector_hwobj = self.getObjectByRole("detector")
        self.beam_info_hwobj = self.getObjectByRole("beam_info")
        self.graphics_manager_hwobj = self.getObjectByRole("graphics_manager")
        self.autoprocessing_hwobj = self.getObjectByRole("auto_processing")
        self.flux_hwobj = self.getObjectByRole("flux")

        self.cmd_ni_conf = self.getCommandObject("ni_configure")
        self.cmd_ni_unconf = self.getCommandObject("ni_unconfigure")

        self.chan_kappa_pos = self.getChannelObject("kappapos")
        self.chan_phi_pos = self.getChannelObject("phipos")

        #self.chan_undulator_gap = self.getChannelObject("chanUndulatorGap")

        undulators = []
        try:
            for undulator in self["undulators"]:
                undulators.append(undulator)
        except BaseException:
            pass

        self.exp_type_dict = {'Mesh': 'raster',
                              'Helical': 'Helical'}

        det_px, det_py = self.detector_hwobj.get_pixel_size()

        self.set_beamline_configuration(
            synchrotron_name="ALBA",
            directory_prefix=self.getProperty("directory_prefix"),
            default_exposure_time=self.detector_hwobj.get_default_exposure_time(),
            minimum_exposure_time=self.detector_hwobj.get_minimum_exposure_time(),
            detector_fileext=self.detector_hwobj.get_file_suffix(),
            detector_type=self.detector_hwobj.get_detector_type(),
            detector_manufacturer=self.detector_hwobj.get_manufacturer(),
            detector_model=self.detector_hwobj.get_model(),
            detector_px=det_px,
            detector_py=det_py,
            undulators=undulators,
            focusing_optic=self.getProperty('focusing_optic'),
            monochromator_type=self.getProperty('monochromator'),
            beam_divergence_vertical=self.beam_info_hwobj.get_beam_divergence_hor(),
            beam_divergence_horizontal=self.beam_info_hwobj.get_beam_divergence_ver(),
            polarisation=self.getProperty('polarisation'),
            input_files_server=self.getProperty("input_files_server"))

        self.emit("collectConnected", (True,))
        self.emit("collectReady", (True, ))

        self.bypass_shutters = bool(os.environ.get('MXCUBE_BYPASS_SHUTTERS'))
        if self.bypass_shutters:
            self.logger.warning("Starting MXCuBE BYPASSING the SHUTTERS")

    def data_collection_hook(self):
        """Main collection hook
        """

        self.logger.info("Running ALBA data collection hook")
        self.logger.info("Waiting for resolution ready...")
        self.resolution_hwobj.wait_end_of_move()
        self.logger.info("Waiting for detector distance ready...")
        self.detector_hwobj.wait_move_distance_done()
        self.logger.info("Waiting for energy ready...")
        self.energy_hwobj.wait_move_energy_done()

        # prepare input files for autoprocessing
        # pass wavelength needed in auto processing input files
        osc_pars = self.current_dc_parameters["oscillation_sequence"][0]
        osc_pars['wavelength'] = self.get_wavelength()

        self.autoprocessing_hwobj.create_input_files(self.current_dc_parameters)

        if self.aborted_by_user:
            self.emit_collection_failed("Aborted by user")
            self.aborted_by_user = False
            return

        ### EDNA_REF, OSC, MESH, HELICAL

        exp_type = self.current_dc_parameters['experiment_type']
        self.logger.debug("Collection method selected is %s)" % exp_type)

        if exp_type == "Characterization":
            self.logger.debug("Running a collect (CHARACTERIZATION)")
        elif exp_type == "Helical":
            self.logger.debug("Running a helical collection")
            self.logger.debug(
                "\thelical positions are: %s" % str(
                    self.helical_positions))
            hpos = self.helical_positions
            self.logger.debug(
                "\tphiy from %3.4f to %3.4f" %
                (hpos[0], hpos[4]))
            self.logger.debug(
                "\tphiz from %3.4f to %3.4f" %
                (hpos[1], hpos[5]))
            self.logger.debug(
                "\tsampx from %3.4f to %3.4f" %
                (hpos[2], hpos[6]))
            self.logger.debug(
                "\tsampy from %3.4f to %3.4f" %
                (hpos[3], hpos[7]))
        elif exp_type == "Mesh":
            self.logger.debug("Running a raster collection ()")
            self.logger.debug(
                "\tnumber of lines are: %s" %
                self.mesh_num_lines)
            self.logger.debug(
                "\ttotal nb of frames: %s" %
                self.mesh_total_nb_frames)
            self.logger.debug("\tmesh range : %s" % self.mesh_range)
            self.logger.debug("\tmesh center : %s" % self.mesh_center)
        else:
            self.logger.debug("Running a collect (STANDARD)")

        osc_seq = self.current_dc_parameters['oscillation_sequence'][0]

        # Unused variables
        # image_range = osc_seq['range']
        nb_images = osc_seq['number_of_images']
        # total_range = image_range * nb_images

        omega_pos = osc_seq['start']
        # Save omega initial position to be recovered after collection (cleanup).
        self.omega_init_pos = omega_pos

        ready, msg = self.prepare_acquisition()

        if not ready:
            self.collection_failed(msg)
            self.stop_collect()
            return

        self._collecting = True
        # for progressBar brick
        self.emit("progressInit", "Collection", osc_seq['number_of_images'])

#        omega_pos = osc_seq['start']
#        # Save position to recover omega position after collection (cleanup).
#        self.omega_init_pos = omega_pos

        self.emit("collectStarted", (self.owner, 1))

        first_image_no = osc_seq['start_image_number']

        if exp_type == 'OSC' or (exp_type == 'Characterization' and nb_images == 1):
            final_pos = self.prepare_collection(
                start_angle=omega_pos,
                nb_images=nb_images,
                first_image_no=first_image_no)
            self.detector_hwobj.start_collection()
            self.collect_images(final_pos, nb_images, first_image_no)
        elif exp_type == 'Characterization' and nb_images > 1:   # image one by one
            for imgno in range(nb_images):
                final_pos = self.prepare_collection(
                    start_angle=omega_pos, nb_images=1, first_image_no=first_image_no)
                self.detector_hwobj.start_collection()
                self.collect_images(final_pos, 1, first_image_no)
                first_image_no += 1
                omega_pos += 90

    def collect_images(self, final_pos, nb_images, first_image_no):
        #
        # Run
        #
        self.logger.info("Collecting images, by moving omega to %s" % final_pos)
        self.omega_hwobj.move(final_pos)
        self.wait_collection_done(nb_images, first_image_no)
        self.data_collection_end()
        self.collection_finished()

    def data_collection_end(self):
        self.omega_hwobj.set_velocity(60)
        self.unconfigure_ni()

    def data_collection_failed(self):
        self.logger.info("Data collection failed")
        AbstractCollect.data_collection_failed()
        self.collect_failed()
        # recovering sequence should go here

    def prepare_acquisition(self):

        fileinfo = self.current_dc_parameters['fileinfo']

        basedir = fileinfo['directory']

        # Save omega velocity
        # self.saved_omega_velocity = self.omega_hwobj.get_velocity()

        # Better use it nominal velocity (to be properly defined in Sardana motor)
        # Ensure omega has its nominal velocity to go to the initial position
        # We have to ensure omega is not moving when setting the velocity

        try:
            self.omega_hwobj.set_velocity(60)
        except Exception:
            self.logger.error("Error setting omega velocity, state is %s" % str(self.omega_hwobj.getState()))
            self.omega_hwobj.wait_end_of_move(timeout=20)
            self.omega_hwobj.set_velocity(60)
            self.logger.info("Omega velocity set to its nominal value")
        
        # create directories if needed
        self.check_directory(basedir)

        # check fast shutter closed. others opened

        if self.bypass_shutters:
            logging.getLogger('user_level_log').warning("Shutters BYPASSED")
        else:
            _ok, failed = self.check_shutters()
            if not _ok:
                msg = "Shutter(s) {} NOT READY".format(failed)
                logging.getLogger('user_level_log').error(msg)
                return _ok, msg
            else:
                logging.getLogger('user_level_log').info("Shutters READY")

        gevent.sleep(1)
        self.logger.info(
            "Waiting diffractometer ready (is %s)" % str(self.diffractometer_hwobj.current_state))
        self.diffractometer_hwobj.wait_device_ready(timeout=10)
        self.logger.info("Diffractometer is now ready.")

        # go to collect phase
        if not self.is_collect_phase():
            self.logger.info("Supervisor not in collect phase, asking to go...")
            success = self.go_to_collect()
            if not success:
                msg = "Supervisor cannot set COLLECT phase"
                self.logger.error(msg)
                return False, msg

        detok = self.detector_hwobj.prepare_acquisition(self.current_dc_parameters)

        if not detok:
            return False, 'Cannot prepare detector.'

        return detok, 'Collection prepared'

    def prepare_collection(self, start_angle, nb_images, first_image_no):
        osc_seq = self.current_dc_parameters['oscillation_sequence'][0]

        # start_angle = osc_seq['start']
        # nb_images = osc_seq['number_of_images']

        img_range = osc_seq['range']
        exp_time = osc_seq['exposure_time']

        total_dist = nb_images * img_range
        total_time = nb_images * exp_time
        omega_speed = float(total_dist / total_time)

        self.write_image_headers(start_angle)

        self.logger.info("nb_images: %s / img_range: %s / exp_time: %s /"
                                      " total_distance: %s / speed: %s" %
                                      (nb_images, img_range, exp_time, total_dist,
                                       omega_speed))
#        self.logger.info(
#            "  setting omega velocity to 60 to go to initial position")
#
#        try:
#            self.omega_hwobj.set_velocity(60)
#        except Exception:
#            self.logger.info("Omega state is %s" % str(self.omega_hwobj.getState()))
#            self.logger.info("Trying again in 5 sec to set omega velocity")
#            time.sleep(5)
#            self.omega_hwobj.set_velocity(60)

        omega_acceltime = self.omega_hwobj.get_acceleration()

        safe_delta = 9.0 * omega_speed * omega_acceltime

        init_pos = start_angle - safe_delta
        final_pos = start_angle + total_dist + safe_delta

        self.logger.info("Moving omega to initial position %s" % init_pos)
        try:
            self.omega_hwobj.wait_end_of_move(timeout=40)
            self.omega_hwobj.move(init_pos)
        except Exception:
            self.logger.info("Omega state is %s" % str(self.omega_hwobj.getState()))
            self.logger.info("Trying again in 5 sec to move omega to initial position %s" % init_pos)
            time.sleep(5)
            self.omega_hwobj.move(init_pos)


        self.detector_hwobj.prepare_collection(nb_images, first_image_no)

        # TODO: Increase timeout: 
        self.omega_hwobj.wait_end_of_move(timeout=40)

        self.logger.info(
            "Moving omega finished at %s" %
            self.omega_hwobj.getPosition())

        # program omega speed depending on exposure time

        self.logger.info("Setting omega velocity to %s" % omega_speed)
        self.omega_hwobj.set_velocity(omega_speed)
        if omega_speed != 0:
            self.configure_ni(start_angle, total_dist)

        return final_pos

    def write_image_headers(self, start_angle):
        fileinfo = self.current_dc_parameters['fileinfo']
        basedir = fileinfo['directory']

        exp_type = self.current_dc_parameters['experiment_type']
        osc_seq = self.current_dc_parameters['oscillation_sequence'][0]

        nb_images = osc_seq['number_of_images']
        # start_angle = osc_seq['start']

        img_range = osc_seq['range']

        if exp_type == "Characterization":
            angle_spacing = 90
        else:
            angle_spacing = img_range

        exp_time = osc_seq['exposure_time']

        # PROGRAM Image Headers
        #latency_time = 0.003
        latency_time = self.detector_hwobj.get_latency_time()
        limaexpt = exp_time - latency_time

        self.image_headers = {}

        angle_info = [start_angle, img_range, angle_spacing]

        self.image_headers['nb_images'] = nb_images
        self.image_headers['Exposure_time'] = "%.4f" % limaexpt
        self.image_headers['Exposure_period'] = "%.4f" % exp_time
        self.image_headers['Start_angle'] = "%f deg." % start_angle
        self.image_headers['Angle_increment'] = "%f deg." % img_range
        self.image_headers['Wavelength'] = self.energy_hwobj.get_wavelength()

        self.image_headers["Detector_distance"] = "%.5f m" % (
            self.detector_hwobj.get_distance() / 1000.0)
        self.image_headers["Detector_Voffset"] = '0 m'

        beamx, beamy = self.detector_hwobj.get_beam_centre()
        self.image_headers["Beam_xy"] = "(%.2f, %.2f) pixels" % (beamx, beamy)

        self.image_headers["Filter_transmission"] = "%.4f" % (
            self.transmission_hwobj.getAttFactor() / 100.0)
        self.image_headers["Flux"] = "%.4g" % self.flux_hwobj.get_flux()
        self.image_headers["Detector_2theta"] = "0.0000"
        self.image_headers["Polarization"] = "0.99"
        self.image_headers["Alpha"] = '0 deg.'

        self.image_headers["Kappa"] = "%.4f deg." % self.chan_kappa_pos.getValue()
        self.image_headers["Phi"] = "%.4f deg." % self.chan_phi_pos.getValue()

        self.image_headers["Chi"] = "0 deg."
        self.image_headers["Oscillation_axis"] = "omega (X, CW)"
        self.image_headers["N_oscillations"] = '1'
        self.image_headers["Detector_2theta"] = "0.0000 deg"

        self.image_headers["Image_path"] = ': %s' % basedir

        self.image_headers["Threshold_setting"] = '%0f eV' %\
                                                  self.detector_hwobj.get_threshold()
        self.image_headers["Gain_setting"] = '%s (vtr)' % str(
            self.detector_hwobj.get_gain())

        self.image_headers["Tau"] = '%s s' % str(199.1e-09)
        self.image_headers["Count_cutoff"] = '%s counts' % str(370913)
        self.image_headers["N_excluded_pixels"] = '= %s' % str(1178)
        self.image_headers["Excluded_pixels"] = ': %s' % str("badpix_mask.tif")
        self.image_headers["Trim_file"] = ': %s' % str(
            "p6m0108_E12661_T6330_vrf_m0p20.bin")

        self.detector_hwobj.set_image_headers(self.image_headers, angle_info)

    def wait_collection_done(self, nb_images, first_image_no):

        # Deprecated
        # osc_seq = self.current_dc_parameters['oscillation_sequence'][0]
        # first_image_no = osc_seq['start_image_number']
        # nb_images = osc_seq['number_of_images']
        last_image_no = first_image_no + nb_images - 1

        if nb_images > 1:
            self.wait_save_image(first_image_no)
        self.omega_hwobj.wait_end_of_move(timeout=720)
        self.wait_save_image(last_image_no)

    def wait_save_image(self, frame_number, timeout=25):

        fileinfo = self.current_dc_parameters['fileinfo']
        basedir = fileinfo['directory']
        template = fileinfo['template']

        filename = template % frame_number
        full_path = os.path.join(basedir, filename)

        start_wait = time.time()

        self.logger.debug("   waiting for image on disk: %s" % full_path)

        while not os.path.exists(full_path):
            # TODO: review next line for NFTS related issues.
            dirlist = os.listdir(basedir)  # forces directory flush ?
            if (time.time() - start_wait) > timeout:
                self.logger.debug("   giving up waiting for image")
                cam_state = self.detector_hwobj.chan_cam_state.getValue()
                acq_status = self.detector_hwobj.chan_acq_status.getValue()
                fault_error = self.detector_hwobj.chan_acq_status_fault_error.getValue()
                self.detector_hwobj.get_saving_statistics()
                msg = "cam_state = {}, acq_status = {}, fault_error = {}".format(
                    cam_state, acq_status, fault_error)
                logging.getLogger('user_level_log').error("Incompleted data collection")
                logging.getLogger('user_level_log').error(msg)
                # raise RuntimeError(msg)
                return False
            time.sleep(0.2)

        self.detector_hwobj.get_saving_statistics()

        # self.last_saved_image = fullpath

        # generate thumbnails
        archive_dir = fileinfo['archive_directory']
        self.check_directory(archive_dir)

        jpeg_filename = os.path.splitext(filename)[0] + ".jpeg"
        thumb_filename = os.path.splitext(filename)[0] + ".thumb.jpeg"

        thumb_fullpath = os.path.join(archive_dir, thumb_filename)
        jpeg_fullpath = os.path.join(archive_dir, jpeg_filename)

        self.logger.debug(
            "   creating thumbnails for  %s in: %s and %s" %
            (full_path, jpeg_fullpath, thumb_fullpath))
        cmd = "adxv_thumb 0.4 %s %s" % (full_path, jpeg_fullpath)
        os.system(cmd)
        cmd = "adxv_thumb 0.1 %s %s" % (full_path, thumb_fullpath)
        os.system(cmd)

        self.logger.debug("   writing thumbnails info in LIMS")
        self.store_image_in_lims(frame_number)

        return True

    def check_shutters(self):

        # Shutters ready: 1, 1, 1, 1
        
        # fast shutter closed: State = 1
        # slow shutter is close: State = 0
        # photon shutter is close: State = 0
        # front end is close: State = 0
        fast_shutter = self.fastshut_hwobj.getState()
        slow_shutter = self.slowshut_hwobj.getState()
        photon_shutter = self.photonshut_hwobj.getState()
        front_end = self.frontend_hwobj.getState()

        shutters = ['fast', 'slow', 'photon', 'front-end']
        states = [fast_shutter, slow_shutter, photon_shutter, front_end]

        failed = [s for s, state in zip(shutters, states) if not state]

        self.logger.debug("fast shutter state is: %s" % fast_shutter) 
        self.logger.debug("slow shutter state is: %s" % slow_shutter) 
        self.logger.debug("photon shutter state is: %s" % photon_shutter) 
        self.logger.debug("front_end state is: %s" % front_end) 

        return all([fast_shutter, slow_shutter, photon_shutter, front_end]), failed

    def get_image_headers(self):
        headers = []
        return headers

    def collection_end(self):
        #
        # data collection end (or abort)
        #
        self.logger.info(" finishing data collection ")
        self.emit("progressStop")

    def data_collection_cleanup(self):
        self.logger.debug("Cleanup: moving omega to initial position %s" % self.omega_init_pos)
        #try:
        self.detector_hwobj.stop_collection()
        self.omega_hwobj.stop()
        time.sleep(2)
        self.data_collection_end()
        time.sleep(2)
        self.omega_hwobj.move(self.omega_init_pos)
        #except:
        #    self.logger.error("Omega needs to be stopped before restoring initial position")
        #    self.omega_hwobj.stop()
        #    self.omega_hwobj.move(self.omega_init_pos)
            
        AbstractCollect.data_collection_cleanup(self)
        self.logger.debug("ALBA data_collection_cleanup finished")
        

    def check_directory(self, basedir):
        if not os.path.exists(basedir):
            try:
                os.makedirs(basedir)
            except OSError as e:
                import errno
                if e.errno != errno.EEXIST:
                    raise

    def collect_finished(self, green):
        logging.getLogger('user_level_log').info("Data collection finished")

    def collect_failed(self, par):
        self.logger.exception("Data collection failed")
        self.current_dc_parameters["status"] = 'failed'
        exc_type, exc_value, exc_tb = sys.exc_info()
        failed_msg = 'Data collection failed!\n%s' % exc_value
        self.emit("collectOscillationFailed", (self.owner, False, failed_msg,
                                               self.current_dc_parameters.get('collection_id'), 1))

        self.detector_hwobj.stop_collection()
        self.omega_hwobj.stop()
        self.data_collection_end()

    def go_to_collect(self, timeout=180):
        self.wait_supervisor_ready()
        self.logger.debug("Sending supervisor to collect phase")
        self.supervisor_hwobj.go_collect()

        gevent.sleep(0.5)

        t0 = time.time()
        while True:

# TODO: This call return None !!!!
            super_state = self.supervisor_hwobj.get_state()
            super_state2 = self.supervisor_hwobj.current_state

            self.logger.debug("Supervisor get_state() is %s" % super_state)
            self.logger.debug("Supervisor current current_state is %s" % super_state2)
            #TODO: review, sometimes get_current_phase returns None 
            try:
                cphase = self.supervisor_hwobj.get_current_phase().upper()
                self.logger.debug("Supervisor current phase is %s" % cphase)
            except:
                cphase = None
            
            if super_state == DevState.ON and cphase == "COLLECT":
                break
            if time.time() - t0 > timeout:
                msg = "Timeout sending supervisor to collect phase"
                self.logger.debug(msg)
                raise RuntimeError(msg)
            gevent.sleep(0.5)

        self.logger.debug("New supervisor phase is %s (Collect phase was requested)" % cphase)

        return self.is_collect_phase()

    def is_collect_phase(self):
        self.logger.debug("In is_collect_phase method")
        try:
            return self.supervisor_hwobj.get_current_phase().upper() == "COLLECT"
        except Exception as e:
            msg = "Cannot return current phase from supervisor. Please, restart MXCuBE."
            logging.getLogger('user_level_log').error(msg)
            raise Exception(msg)

    def go_to_sampleview(self, timeout=180):
        self.wait_supervisor_ready()
        self.logger.debug("Sending supervisor to sample view phase")
        self.supervisor_hwobj.go_sample_view()

        gevent.sleep(0.5)

        t0 = time.time()
        while True:
            #TODO: review, some calls return None for get_current_phase()
            try:
                super_state = self.supervisor_hwobj.get_state()
                cphase = self.supervisor_hwobj.get_current_phase().upper()
            except:
                super_state = cphase = None
            if super_state != DevState.MOVING and cphase == "SAMPLE":
                break
            if time.time() - t0 > timeout:
                self.logger.debug("Timeout sending supervisor to sample view phase")
                break
            gevent.sleep(0.5)

        self.logger.debug("New supervisor phase is %s" % cphase)

        return self.is_sampleview_phase()

    def is_sampleview_phase(self):
        return self.supervisor_hwobj.get_current_phase().upper() == "SAMPLE"

    def wait_supervisor_ready(self, timeout=30):
        self.logger.debug("Waiting to supervisor ready")

        gevent.sleep(0.5)

        t0 = time.time()
        while True:
            super_state = self.supervisor_hwobj.get_state()
            if super_state == DevState.ON:
                break
            if time.time() - t0 > timeout:
                self.logger.debug("Timeout waiting for supervisor ready")
                raise RuntimeError("Supervisor cannot be operated (state %s)" % super_state)
                break
            self.logger.debug("Supervisor state is %s" % super_state)
            gevent.sleep(0.5)


    def configure_ni(self, startang, total_dist):
        self.logger.debug(
            "Configuring NI660 with pars 0, %s, %s, 0, 1" %
            (startang, total_dist))
        self.cmd_ni_conf(0.0, startang, total_dist, 0, 1)

    def unconfigure_ni(self):
        self.cmd_ni_unconf()

    def open_safety_shutter(self):
        """ implements prepare_shutters in collect macro """

        # prepare ALL shutters

        if self.fastshut_hwobj.getState() != 0:
            self.fastshut_hwobj.close()

        if self.slowshut_hwobj.getState() != 1:
            self.slowshut_hwobj.open()

        if self.photonshut_hwobj.getState() != 1:
            self.photonshut_hwobj.open()

        if self.frontend_hwobj.getState() != 0:
            self.frontend_hwobj.open()

    def open_detector_cover(self):
        self.supervisor_hwobj.open_detector_cover()

    def open_fast_shutter(self):
        # self.fastshut_hwobj.open()
        # this function is empty for ALBA. we are not opening the fast shutter.
        # on the contrary open_safety_shutter (equivalent to prepare_shutters in
        # original collect macro will first close the fast shutter and open the
        # other three
        pass

    def close_fast_shutter(self):
        self.fastshut_hwobj.cmdOut()

    def close_safety_shutter(self):
        #  we will not close safety shutter during collections
        pass

    def close_detector_cover(self):
        #  we will not close detector cover during collections
        #  self.supervisor.close_detector_cover()
        pass

    def set_helical_pos(self, arg):
        """
        Descript. : 8 floats describe
        p1AlignmY, p1AlignmZ, p1CentrX, p1CentrY
        p2AlignmY, p2AlignmZ, p2CentrX, p2CentrY
        """
        self.helical_positions = [arg["1"]["phiy"], arg["1"]["phiz"],
                                  arg["1"]["sampx"], arg["1"]["sampy"],
                                  arg["2"]["phiy"], arg["2"]["phiz"],
                                  arg["2"]["sampx"], arg["2"]["sampy"]]

    def setMeshScanParameters(self, num_lines, num_images_per_line, mesh_range):
        """
        Descript. :
        """
        pass

    @task
    def _take_crystal_snapshot(self, filename):
        """
        Descript. :
        """
        if not self.is_sampleview_phase():
            self.go_to_sampleview()

        self.graphics_manager_hwobj.save_scene_snapshot(filename)
        self.logger.debug("Crystal snapshot saved (%s)" % filename)

    def set_energy(self, value):
        """
        Descript. : This is Synchronous to be able to calculate the resolution @ ALBA
        """
        #   program energy
        #   prepare detector for diffraction
        self.energy_hwobj.move_energy(value)
        logging.getLogger('user_level_log').warning("Setting beamline energy it can take a while, please be patient")
        self.energy_hwobj.wait_move_energy_done()

    def set_wavelength(self, value):
        """
        Descript. :
        """
        #   program energy
        #   prepare detector for diffraction
        self.energy_hwobj.move_wavelength(value)
        self.energy_hwobj.wait_move_wavelength_done()

    def get_energy(self):
        return self.energy_hwobj.get_energy()

    def set_transmission(self, value):
        """
        Descript. :
        """
        self.transmission_hwobj.set_value(value)

    def set_resolution(self, value,  energy=None):
        """
        Descript. : resolution is a motor in out system
        """
        # Current resolution non valid since depends on energy and detector distance!!
        #current_resolution = self.resolution_hwobj.getPosition()
        #self.logger.debug("Current resolution is %s, moving to %s" % (current_resolution, value))
        self.logger.debug("Moving resolution to %s" % value)

        if energy:
            # calculate the detector position to achieve the desired resolution
            _det_pos = get_dettaby(value, energy=energy)
            # calulate the corresponding resolution
            value = get_resolution(_det_pos, energy=energy)

        self.resolution_hwobj.move(value)

    def move_detector(self, value):
        self.detector_hwobj.move_distance(value)

    @task
    def move_motors(self, motor_position_dict):
        """
        Descript. :
        """
        self.diffractometer_hwobj.move_motors(motor_position_dict)

    def create_file_directories(self):
        """
        Method create directories for raw files and processing files.
        Directories for xds, mosflm, ednaproc and autoproc
        """
        self.create_directories(
            self.current_dc_parameters['fileinfo']['directory'],
            self.current_dc_parameters['fileinfo']['process_directory'])

        # create processing directories for each post process
        for proc in ['xds', 'mosflm', 'ednaproc', 'autoproc']:
            self._create_proc_files_directory(proc)

    def _create_proc_files_directory(self, proc_name):

        i = 1

        while True:
            _dirname = "%s_%s_%s_%d" % (
                proc_name,
                self.current_dc_parameters['fileinfo']['prefix'],
                self.current_dc_parameters['fileinfo']['run_number'],
                i)
            _directory = os.path.join(
                self.current_dc_parameters['fileinfo']['process_directory'],
                _dirname)
            if not os.path.exists(_directory):
                break
            i += 1

        try:
            self.create_directories(_directory)
            os.system("chmod -R 777 %s" % _directory)
        except Exception as e:
            msg = "Could not create directory %s\n%s" % (_directory, str(e))
            self.logger.exception(msg)
            return

        # save directory names in current_dc_parameters. They will later be used
        #  by autoprocessing.
        key = "%s_dir" % proc_name
        self.current_dc_parameters[key] = _directory
        self.logger.debug("dc_pars[%s] = %s" % (key, _directory))
        return _directory

    def get_wavelength(self):
        """
        Descript. :
            Called to save wavelength in lims
        """
        if self.energy_hwobj is not None:
            return self.energy_hwobj.get_wavelength()

    def get_detector_distance(self):
        """
        Descript. :
            Called to save detector_distance in lims
        """
        if self.detector_hwobj is not None:
            return self.detector_hwobj.get_distance()

    def get_resolution(self):
        """
        Descript. :
            Called to save resolution in lims
        """
        if self.resolution_hwobj is not None:
            return self.resolution_hwobj.getPosition()

    def get_transmission(self):
        """
        Descript. :
            Called to save transmission in lims
        """
        if self.transmission_hwobj is not None:
            return self.transmission_hwobj.getAttFactor()

    def get_undulators_gaps(self):
        """
        Descript. : return triplet with gaps. In our case we have one gap,
                    others are 0
        """
        # TODO
        try:
            if self.chan_undulator_gap:
                und_gaps = self.chan_undulator_gap.getValue()
                if type(und_gaps) in (list, tuple):
                    return und_gaps
                else:
                    return (und_gaps)
        except BaseException as e:
            self.logger.debug("Get undulator gaps error\n%s" % str(e))
            pass
        return {}

    def get_beam_size(self):
        """
        Descript. :
        """
        if self.beam_info_hwobj is not None:
            return self.beam_info_hwobj.get_beam_size()

    def get_slit_gaps(self):
        """
        Descript. :
        """
        if self.beam_info_hwobj is not None:
            return self.beam_info_hwobj.get_slits_gap()
        return None, None

    def get_beam_shape(self):
        """
        Descript. :
        """
        if self.beam_info_hwobj is not None:
            return self.beam_info_hwobj.get_beam_shape()

    def get_measured_intensity(self):
        """
        Descript. :
        """
        if self.flux_hwobj is not None:
            return self.flux_hwobj.get_flux()

    def get_machine_current(self):
        """
        Descript. :
        """
        if self.machine_info_hwobj:
            return self.machine_info_hwobj.get_current()
        else:
            return 0

    def get_machine_message(self):
        """
        Descript. :
        """
        if self.machine_info_hwobj:
            return self.machine_info_hwobj.get_message()
        else:
            return ''
    # TODO: implement fill mode
    def get_machine_fill_mode(self):
        """
        Descript. :
        """
        if self.machine_info_hwobj:
            return "FillMode not/impl"
        else:
            return ''

    def get_flux(self):
        """
        Descript. :
        """
        return self.get_measured_intensity()

    def trigger_auto_processing(self, event, frame):
        if event == "after":
            dc_pars = self.current_dc_parameters
            self.autoprocessing_hwobj.trigger_auto_processing(dc_pars)


def test_hwo(hwo):
    print("Energy: ", hwo.get_energy())
    print("Transmission: ", hwo.get_transmission())
    print("Resolution: ", hwo.get_resolution())
    print("Shutters (ready for collect): ", hwo.check_shutters())
    print("Supervisor(collect phase): ", hwo.is_collect_phase())

    print("Flux ", hwo.get_flux())
    print("Kappa ", hwo.kappapos_chan.getValue())
    print("Phi ", hwo.phipos_chan.getValue())
