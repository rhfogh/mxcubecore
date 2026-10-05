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
#   You should have received a copy of the GNU Lesser General Public License
#  along with MXCuBE. If not, see <http://www.gnu.org/licenses/>.


import logging
import os
import time
import gevent
import pickle
import copy
import h5py
import numpy as np
from scipy.optimize import minimize
import traceback

from mxcubecore.TaskUtils import task
from mxcubecore import HardwareRepository as HWR
from mxcubecore.utils import qt_import
from mxcubecore.BaseHardwareObjects import HardwareObjectState

from mxcubecore.HardwareObjects.abstract.AbstractDiffractometer import (
    AbstractDiffractometer,
    DiffractometerConstraint,
    DiffractometerHead,
    DiffractometerPhase,
)


dt = h5py.special_dtype(vlen=np.dtype("uint8"))

try:
    import lmfit
    from lmfit import fit_report
except ImportError:
    logging.warning(
        "Could not lmfit minimization library, "
        + "refractive model centring will not work."
    )

from useful_routines import (
    get_redis_connection,
    get_name_pattern_complement,
    get_result_position,
)

try:
    from goniometer import goniometer
    from detector import detector
    from oav_camera import oav_camera as camera
    from scan_and_align import scan_and_align
    from optical_alignment import optical_alignment
    #from anneal import anneal as anneal_procedure
    from diffraction_tomography import diffraction_tomography
    from history_saver import get_jpegs_from_arrays

except ModuleNotFoundError:
    from experimental_methods import (
        goniometer,
        detector,
        oav_camera as camera,
        optical_alignment,
        scan_and_align,
        #anneal as anneal_procedure,
        diffraction_tomography,
        get_jpegs_from_arrays,
    )


__credits__ = ["SOLEIL"]
__category__ = "General"

class PX2Diffractometer(AbstractDiffractometer):

    motor_name_mapping = [
        ("AlignmentX", "phix"),
        ("AlignmentY", "phiy"),
        ("AlignmentZ", "phiz"),
        ("CentringX", "sampx"),
        ("CentringY", "sampy"),
        ("Omega", "phi"),
        ("Kappa", "kappa"),
        ("Phi", "kappa_phi"),
        ("beam_x", "beam_x"),
        ("beam_y", "beam_y"),
    ]

    md_to_mxcube = dict(
        [(key, value) for key, value in motor_name_mapping]
    )
    mxcube_to_md = dict(
        [(value, key) for key, value in motor_name_mapping]
    )

    def __init__(self, name):
        super().__init__(name)

        self.goniometer = goniometer()
        self.camera = camera()
        self.detector = detector()

        try:
            self.archive_directory = HWR.beamline.session.get_archive_directory()
        except:
            self.archive_directory = "%s/manual_optical_alignment" % os.getenv("HOME")

        self.oa = optical_alignment(
            directory=self.archive_directory, name_pattern="auto_%s" % os.getuid()
        )
        self.redis = get_redis_connection()
        self.log = logging.getLogger("HWR")
        self.nclicks = 5
        self.step = None
        self.click_angles = np.array([0, +45, +180 - 30, 180 + 30, -45])
        self.user_click_x = -1
        self.user_click_y = -1
        self.beam_position = [680., 512.]
        self.centring_method = "Circle"
        self.zoom_centre = {"x": 0., "y": 0.}

    def init(self):
        super().init()
        self.head_type = DiffractometerHead(self.goniometer.get_head_type())
        self.current_phase = DiffractometerPhase(self.goniometer.get_current_phase())
        self.current_constraint = DiffractometerConstraint.RELEASE
        self.update_state(HardwareObjectState.READY)
        self.connect(self.nstate_equipment_hwobj_dict["zoom"], "valueChanged", self.zoom_changed)

    def get_camera_list(self):
        camera_list = [
            "auto",
            "oav",
            "cam14_quad",
            "cam14_1",
            "cam14_2",
            "cam14_3",
            "cam14_4",
            "cam1",
            "cam6",
            "cam8",
            "cam13",
            "murko",
        ]
        return camera_list

    def get_current_camera(self):
        current_camera = self.redis.get("mxcube_camera").decode()
        return current_camera

    def set_current_camera(self, camera):
        if camera != "auto":
            self.redis.set("mxcube_camera", camera)

    def zoom_changed(self, zoom=None):
        print(f"zoom_changed {zoom}")
        ppm = self.get_pixels_per_mm()
        print(f"ppm {ppm}")
        self.emit("pixelsPerMmChanged", (ppm,))

    def abort(self):
        self.goniometer.abort()
        self.update_state(HardwareObjectState.READY)

    def wait_status_ready(self, timeout=None):
        return True

    def save_centring_positions(self):
        self.goniometer.save_position()

    def _set_phase(self, value: DiffractometerPhase):
        print(f"_set_phase value value.value {value} {value.value}")
        self.goniometer.set_goniometer_phase(value.value)
        self.update_state(HardwareObjectState.READY)
        print("_set_phase done !")

    def get_pixels_per_mm(self):
        y_calib, x_calib = map(float, self.camera.get_calibration())
        return 1.0 / x_calib, 1.0 / y_calib

    def set_value_motors(self, motors_positions_dict):
        self.log.info(f"set_value_motors {motors_positions_dict}")
        position = self.translate_from_mxcube_to_md(motors_positions_dict)
        self.goniometer.set_position(position)

    def translate_from_mxcube_to_md(self, position):
        translated_position = {}

        for key in position:
            if isinstance(key, str):
                try:
                    translated_position[self.mxcube_to_md[key]] = position[key]
                except:
                    pass
            else:
                translated_position[key.actuator_name] = position[key]
        return translated_position

    def translate_from_md_to_mxcube(self, position):
        translated_position = {}

        for key in position:
            try:
                translated_position[self.md_to_mxcube[key]] = position[key]
            except KeyError:
                pass

        return translated_position

    def get_name_pattern_complement(self, start=None):
        return get_name_pattern_complement(start=start, gonio=self.goniometer)

    def save_click(
        self,
        x,
        y,
        omega=None,
        name_pattern=None,
        directory=None,
        directory_template="%s/manual_optical_alignment",
    ):

        timestamp = time.time()
        if name_pattern is None:
            name_pattern = f"double_click_{self.get_name_pattern_complement(start=timestamp)}"
        if directory is None:
            try:
                directory = "%s/opti" % HWR.beamline.session.get_archive_directory()
            except:
                directory = directory_template % os.getenv("HOME")

        if omega is None:
            omega = self.goniometer.get_omega_position()

        save_click_command = "click_saver.py -x %.2f -y %.2f -o %.2f -d %s -n %s -t %.3f &" % (
            x,
            y,
            omega,
            directory,
            name_pattern,
            timestamp,
        )

        self.log.info("save_click_command: %s" % save_click_command)
        os.system(save_click_command)


    def move_to_beam(self, x, y, omega=None, save=True):

        if save:
            self.save_click(x, y)

        if self.get_phase().value == "BeamLocation":
            self.log.info(
                "Diffractometer: Move to screen"
                + " position disabled in BeamLocation phase."
            )

        beam_pos_x, beam_pos_y = HWR.beamline.beam.get_beam_position_on_screen()
        y_pixel_size_mm, x_pixel_size_mm = self.camera.get_calibration()
        self.log.info(f"beam_pos {beam_pos_x} {beam_pos_y}")
        self.log.info(f"calibration {y_pixel_size_mm} {x_pixel_size_mm}")
        orthogonal_shift = x - beam_pos_x
        along_shift = y - beam_pos_y
        self.log.info(f"shift px {orthogonal_shift} {along_shift}")
        orthogonal_shift *= x_pixel_size_mm
        along_shift *= y_pixel_size_mm
        self.log.info(f"shift mm {orthogonal_shift} {along_shift}")

        aligned_position = self.goniometer.get_aligned_position_from_shift_and_reference_position(orthogonal_shift, along_shift, omega=omega)

        self.log.info(f"aligned_position\n{aligned_position}")
        #self.goniometer.set_position(aligned_position)
        self._move(aligned_position)

    def _move(self, aligned_position):
        gevent.spawn(self.goniometer.set_position, aligned_position)

    def move_omega(self, angle):
        self.goniometer.set_omega_position(angle)

    def move_omega_relative(self, relative_angle):
        self.goniometer.set_omega_relative_position(relative_angle)

    def set_user_click(self, x, y):
        self.user_clicked_event.set((x, y))

    def acquire_clicks(self, n_clicks, click_angles, name_pattern, directory, save=True):
        _start = time.time()
        vertical_clicks = []
        horizontal_clicks = []
        vertical_discplacements = []
        horizontal_displacements = []
        omegas = []
        calibrations = []

        for k, omega in enumerate(click_angles):
            self.move_omega(omega)
            self.user_clicked_event = gevent.event.AsyncResult()
            calibration = self.camera.get_calibration()
            self.pixels_per_mm_y, self.pixels_per_mm_x = 1.0 / calibration
            x, y = self.user_clicked_event.get()
            omega = self.goniometer.get_omega_position()
            if save:
                self.save_click(
                    x,
                    y,
                    omega=omega,
                    name_pattern=f"{name_pattern}_omega_{omega:.2f}_click_{k+1}",
                    directory=directory,
                )
            vertical_clicks.append(y)
            horizontal_clicks.append(x)
            omegas.append(omega)
            calibrations.append(calibration)

            x -= self.beam_position[0]
            x /= self.pixels_per_mm_x
            y -= self.beam_position[1]
            y /= self.pixels_per_mm_y
            vertical_discplacements.append(y)
            horizontal_displacements.append(x)

            self.log.info("click %d %f %f %f" % (k + 1, omega, x, y))

        self.log.info(f"all {n_clicks} clicks acquired in {time.time() - _start:.4f} seconds")
        return vertical_clicks, horizontal_clicks, vertical_discplacements, horizontal_displacements, omegas, calibrations

    def prepare_centring(self, use_frontlight=False, use_backlight=True, start=None):
        name_pattern = f"manu_{self.get_name_pattern_complement(start=start)}"

        try:
            directory = "%s/opti" % HWR.beamline.session.get_archive_directory()
        except:
            directory = "%s/manual_optical_alignment" % os.getenv("HOME")

        if use_backlight:
            self.goniometer.insert_backlight()
        if use_frontlight:
            self.goniometer.insert_frontlight()
        else:
            self.goniometer.extract_frontlight()

        return name_pattern, directory

    def get_nclicks_and_angles(self, n_click):

        if isinstance(self.nclicks, int) and self.nclicks >= 3:
            n_clicks = self.nclicks

        logging.getLogger("user_level_log").info(
            "expected number of clicks %d" % (n_clicks)
        )

        if self.click_angles is not None and len(self.click_angles) == n_clicks:
            click_angles = self.goniometer.get_omega_position() + self.click_angles
        else:
            click_angles = self.goniometer.get_omega_position() + np.linspace(0, 360, n_clicks)

        return n_clicks, click_angles

    def get_result_position(self):
        if self.centring_method == "Refractive":
            initial_parameters = lmfit.Parameters()
            initial_parameters.add_many(
                ("c", 0.0, True, -5e3, +5e3, None, None),
                ("r", 0.0, True, 0.0, 4e3, None, None),
                ("alpha", -np.pi / 3, True, -2 * np.pi, 2 * np.pi, None, None),
                ("front", 0.01, True, 0.0, 1.0, None, None),
                ("back", 0.005, True, 0.0, 1.0, None, None),
                ("n", 1.31, True, 1.29, 1.33, None, None),
                ("beta", 0.0, True, -2 * np.pi, +2 * np.pi, None, None),
            )

            fit_projection = lmfit.minimize(
                self.refractive_model_residual,
                initial_parameters,
                method="nelder",
                args=(angles, orthogonal_displacements),
            )
            self.log.info(fit_report(fit_projection))
            optimal_params = fit_projection.params
            v = optimal_params.valuesdict()
            c = v["c"]
            r = v["r"]
            alpha = v["alpha"]
            front = v["front"]
            back = v["back"]
            n = v["n"]
            beta = v["beta"]

            c *= 1.0e-3
            r *= 1.0e-3
            front *= 1.0e-3
            back *= 1.0e-3
        else:
            initial_parameters = [4.0, 25.0, 0.05]
            fit_projection = minimize(
                self.circle_model_residual,
                initial_parameters,
                method="nelder-mead",
                args=(angles, orthogonal_displacements),
            )

            c, r, alpha = fit_projection.x
            c *= 1e-3
            r *= 1.0e-3
            v = {"c": c, "r": r, "alpha": alpha}


        center_along_omega = np.mean(along_displacements)

        d_sampx = centringx_direction * r * np.sin(alpha)
        d_sampy = centringy_direction * r * np.cos(alpha)

        d_y = alignmenty_direction * center_along_omega
        d_z = alignmentz_direction * c

        move_vector_dictionary = {
            "AlignmentZ": d_z,
            "AlignmentY": d_y,
            "CentringX": d_sampx,
            "CentringY": d_sampy,
        }

        for motor in reference_position:
            result_position[motor] = reference_position[motor]
            if motor in move_vector_dictionary:
                result_position[motor] += move_vector_dictionary[motor]

        return result_position

    def manual_centring(
        self,
        n_clicks=3,
        # alignmenty_direction=-1.0, # MD2
        alignmenty_direction=+1.0,  # MD3 up
        # alignmentz_direction=-1.0, # MD2
        alignmentz_direction=-1.0,  # MD3 up
        # centringx_direction=-1.0, # MD2
        centringx_direction=+1.0,  # MD3 up
        centringy_direction=+1.0,  # MD3 up & MD2
        refractive_model=False,
        orientation="vertical",
        use_backlight=True,
        use_frontlight=False,
        save=True,
    ):
        """
        Descript. :
        """
        self.log.info("starting manual centring PX2Diffractometer")
        _start = time.time()

        name_pattern, directory = self.prepare_centring(use_frontlight=use_frontlight, use_backlight=use_backlight, start=_start)

        n_clicks, click_angles = self.get_nclicks_and_angles(n_clicks)

        self.oa.timestamp = _start
        self.oa.name_pattern = name_pattern
        self.oa.directory = directory
        self.oa.start_run_time = time.time()
        self.oa.innermost_start_time = time.time()

        vertical_clicks, horizontal_clicks, vertical_discplacements, horizontal_displacements, omegas, calibrations = self.acquire_clicks(n_clicks, click_angles, name_pattern, directory, save=save)
        print('these are the clicks ...')
        print(vertical_clicks, horizontal_clicks, vertical_discplacements, horizontal_displacements, omegas, calibrations)
        self.oa.innermost_end_time = time.time()
        self.oa.end_run_time = time.time()
        self.oa.vertical_clicks = vertical_clicks
        self.oa.horizontal_clicks = horizontal_clicks
        self.oa.omega_clicks = omegas

        if orientation == "horizontal":
            orthogonal_displacements = vertical_discplacements
            along_displacements = horizontal_displacements

        elif orientation == "vertical":
            orthogonal_displacements = horizontal_displacements
            along_displacements = vertical_discplacements

        reference_position = self.goniometer.get_aligned_position()
        angles = np.radians(omegas)

        result_position, ortho_model, along_model, move_vector_dictionary = get_result_position(
            orthogonal_displacements,
            omegas,
            reference_position,
            along_displacements=along_displacements,
            alignmenty_direction=alignmenty_direction,
            alignmentz_direction=alignmentz_direction,
            centringx_direction=centringx_direction,
            centringy_direction=centringy_direction,
            filename=f"{os.path.join(directory, name_pattern)}_alignment_overview.png",
        )

        result_position["Omega"] = click_angles[0]

        self.goniometer.set_position(result_position)
        self.oa.conclusion_end_time = time.time()
        self.oa.clean()
        _end = time.time()
        duration = _end - _start
        self.log.info(
            "input and analysis in manual_centring took %.3f seconds" % duration
        )

        [d_sampx, d_sampy, d_y, d_z] = [move_vector_dictionary[key] for key in ["CentringX", "CentringY", "AlignmentY", "AlignmentZ"]]
        results = {
            "vertical_clicks": vertical_clicks,
            "horizontal_clicks": horizontal_clicks,
            "vertical_discplacements": vertical_discplacements,
            "horizontal_displacements": horizontal_displacements,
            "omegas": omegas,
            "angles": angles,
            "calibrations": calibrations,
            "reference_position": reference_position,
            "result_position": result_position,
            "duration": duration,
            "orthogonal_optimal_parameters": ortho_model,
            "along_optimal_parameters": along_model,
            "move_vector_dictionary": move_vector_dictionary,
            "shift": [d_sampx, d_sampy, d_y, d_z],
        }

        self.log.info(
            "manual_centring finished in %.3f seconds" % (time.time() - _start)
        )

        if save:
            self.save_clicks(directory, name_pattern, results)

        translated_position = self.translate_from_md_to_mxcube(result_position)
        return translated_position

    def save_clicks(self, directory, name_pattern, results):
        try:
            if not os.path.isdir(directory):
                os.makedirs(directory)

            template = os.path.join(directory, name_pattern)

            clicks_filename = "%s_clicks.pickle" % template
            f = open(clicks_filename, "wb")
            pickle.dump(results, f)
            f.close()
        except:
            self.log.exception(traceback.format_exc())

    def set_nclicks(self, nclicks):
        self.log.info(
            "PX2Diffractometer: number of centring clicks changed: %s" % nclicks
        )
        try:
            self.nclicks = int(nclicks)
        except Exception:
            logging.getLogger("HWR").exception(traceback.format_exc())

    def set_step(self, step):
        self.log.info("PX2Diffractometer: centring step changed: %s" % step)
        try:
            self.step = float(step)
        except Exception:
            logging.getLogger("HWR").exception(traceback.format_exc())

    def set_centring_method(self, centring_method):
        self.log.info(
            "PX2Diffractometer: centring method changed: %s" % centring_method
        )
        try:
            self.centring_method = centring_method
        except Exception:
            logging.getLogger("HWR").exception(traceback.format_exc())

    def is_ready(self):
        """
        Detects if device is ready
        """
        return self.goniometer.get_status() == "Ready"

    def set_collecting(self, collecting=True):
        self.collecting = collecting

    def align_from_single_image(self, n_views=2):
        self.log.info("align_from_single_image, total number of views %d" % n_views)
        for k in range(n_views):
            self.log.info("align_from_single_image, view %d" % k)
            self.camera.align_from_single_image(generate_report=False)

    def get_positions(self):
        self.pixels_per_mm_x, self.pixels_per_mm_y = self.get_pixels_per_mm()
        positions = self.goniometer.get_aligned_position()
        self.current_motor_positions = self.translate_from_md_to_mxcube(positions)
        self.current_motor_positions["beam_x"] = (
            self.beam_position[0] - self.zoom_centre["x"]
        ) / self.pixels_per_mm_y
        self.current_motor_positions["beam_y"] = (
            self.beam_position[1] - self.zoom_centre["y"]
        ) / self.pixels_per_mm_x
        return self.current_motor_positions

    def use_sample_changer(self):
        return True

    def accept_centring(self):
        return True

    def user_confirms_centring(self):
        return False

    def has_kappa(self):
        return self.goniometer.has_kappa()

    def accept_centring(self):
        super().accept_centring()
        self.goniometer.save_position()


    def beam_position_check(self):
        # logging.getLogger("user_level_log").info("Going to check the beam position")
        logging.getLogger("user_level_log").info(
            "Beam position check desactivated, please talk to your beamline contact."
        )
        return
        # self.bpc(wait=False)

    #@task
    def bpc(self):
        # log = logging.getLogger("user_level_log")
        return
        # ba = beam_align.beam_align(
        # name_pattern="%s_%s" % (os.getuid(), time.asctime().replace(" ", "_")),
        # directory="%s/beam_align" % os.getenv("HOME"),
        # diagnostic=True)
        # log.info(
        # "Align beam to the optical centre of the camera"
        # )
        # log.info(
        # "Moving scintillator to sample position, please wait ..."
        # )
        # ba.execute()

        # if ba.no_beam is False:
        # log.info(
        # "Initial mirror positions (v, h) [mm]: %.4f %.4f"
        # % tuple(ba.initial_mirror_positions)
        # )
        # log.info(
        # "Initial error (v, h) [px]: %.1f, %.1f"
        # % tuple(ba.initial_pixel_shift)
        # )
        # log.info(
        # "Initial error (v, h) [um]: %.1f, %.1f"
        # % tuple(ba.initial_pixel_shift_mm * 1000.)
        # )
        # log.info(
        # "Final error (v, h) [px]: %.1f, %.1f"
        # % tuple(ba.final_pixel_shift)
        # )
        # log.info(
        # "Final error (v, h) [um]: %.1f, %.1f"
        # % tuple(ba.final_pixel_shift_mm * 1000.)
        # )
        # log.info(
        # "Delta in motor positions [um]: %.1f, %.1f"
        # % tuple((ba.final_mirror_position - ba.initial_mirror_positions) * 1000)
        # )
        # log.info(
        # "Final mirror positions [mm]: %.4f %.4f"
        # % tuple(ba.final_mirror_position)
        # )
        # else:
        # log.info(ba.no_beam_message)

    def anneal(self, duration=1, distance=20):
        aligned_position = self.goniometer.get_aligned_position()
        anneal_position = copy.copy(aligned_position)
        anneal_position["AlignmentY"] -= distance
        self._move(anneal_position)
        gevent.sleep(duration)
        self._move(aligned_position)

    #@task
    def show_excenter_finished_dialog(self, scan_length=0.1, step=90.0, parent=None):
        if parent is not None:
            qt_import.QMessageBox.warning(
                parent,
                "Success!",
                "Sample moved to optimal position\nplease, proceed with data collection.",
                qt_import.QMessageBox.Ok,
            )

    #@task
    def excenter(
        self,
        scan_length=0.40,
        step=45.0,
        dozor=True,
        colspot=False,
        along=False,
        interleave=True,
        default_directory="/nfs/data2/excenter",
        name_pattern="excenter",
        parent=None,
    ):

        timestamp = time.time()
        try:
            element = self.get_element()
        except:
            element = ""

        try:
            directory = "%s/tomo" % HWR.beamline.session.get_archive_directory()
        except:
            directory = default_directory

        specific = f"{self.get_name_pattern_complement(start=timestamp)}"
        directory = os.path.join(directory, specific)

        # angles = str(tuple(np.arange(start, 360.0, step)))
        # angles = np.array([0, 90, 180, 270]) # for high pressure measurements
        # angles = np.array((0, 45, 90, 135, 180)) # GPhL choice
        # angles = np.array([0, 90, 180, 225, 315])  # 360 deg version of GPhL choice
        angles = np.arange(0, 180, step)
        if interleave:
            angles[1::2] = angles[1::2] + 180
            angles.sort()

        current_omega_position = self.goniometer.get_omega_position()
        angles = angles + current_omega_position
        angles = str(list(angles))

        if colspot:
            method = "xds"
        else:
            method = "tioga"

        if scan_length == 0.25:
            scan_length = 0.4

        experiment = diffraction_tomography(
            directory=directory,
            name_pattern=name_pattern,
            vertical_range=scan_length,
            scan_start_angles=angles,
            method=method,
            parent=self,
            cats_api=HWR.beamline.sample_changer.cats_api,
            actuators_dictionary=HWR.beamline.collect.xe.monitors_dictionary,
            monitors_dictionary=HWR.beamline.collect.xe.actuators_dictionary,
        )

        experiment.execute()

        #execute_line = '/usr/local/experimental_methods/diffraction_tomography.py -d %s -n %s -y %.2f -a "%s" -A -C -D' % (
        #directory,
        #name_pattern,
        #scan_length,
        #angles,
        #)

        #os.system(execute_line)

        execute_line = (
            f"/usr/local/conda/envs/murko_3.11/bin/python /usr/local/experimental_methods/shape_from_diffraction_tomography.py -d {directory} -n {name_pattern} -M {method} -D &"
        )

        self.log.info("excenter angles %s" % angles)
        self.log.info("excenter line %s" % execute_line)

        os.system(execute_line)
        logging.getLogger("user_level_log").info("X-ray centring finished successfully")
        logging.getLogger("user_level_log").info("Sample moved to optimal position")
        logging.getLogger("user_level_log").warning("You may proceed with data collection")
        if parent is not None:
            qt_import.QMessageBox.warning(
                parent,
                "Success!",
                "X-ray centring finished successfully\nSample moved to optimal position.\nYou may proceed with data collection",
                qt_import.QMessageBox.Ok,
            )
        try:
            result_position = experiment.get_result_position()
        except:
            traceback.print_exc()
            result_position = self.goniometer.get_aligned_position()

        translated_position = self.translate_from_md_to_mxcube(result_position)
        HWR.beamline.sample_view.accept_centring()
        del experiment
        return translated_position
