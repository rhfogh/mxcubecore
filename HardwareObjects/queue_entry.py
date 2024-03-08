"""
Contains following classes:
* QueueEntryContainer
* BaseQueueEntry
* DummyQueueEntry
* TaskGroupQueueEntry
* SampleQueueEntry
* SampleCentringQueueEntry
* DataCollectionQueueEntry
* CharacterisationQueueEntry
* EnergyScanQueueEntry.

All queue entries inherits the baseclass BaseQueueEntry which inturn
inherits QueueEntryContainer. This makes it possible to arrange and
execute queue entries in a hierarchical maner.

The rest of the classes: DummyQueueEntry, TaskGroupQueueEntry,
SampleQueueEntry, SampleCentringQueueEntry, DataCollectionQueueEntry,
CharacterisationQueueEntry, EnergyScanQueueEntry are concrete
implementations of tasks.
"""

import gevent
import traceback
import logging
import time
import os
import autoprocessing
from copy import copy
from collections import namedtuple

from HardwareRepository.HardwareObjects import queue_model_objects
from HardwareRepository.HardwareObjects.queue_model_enumerables import EXPERIMENT_TYPE, COLLECTION_ORIGIN_STR, CENTRING_METHOD, States
from HardwareRepository.dispatcher import dispatcher
from HardwareRepository import HardwareRepository

from HardwareRepository.HardwareObjects.Gphl import GphlQueueEntry

from HardwareRepository.HardwareObjects.base_queue_entry import (
    BaseQueueEntry,
    QueueEntryContainer,
    QueueEntryStatusType,
    status_list,
    QUEUE_ENTRY_STATUS,
    QueueSkippEntryException,
    QueueExecutionException,
    QueueAbortedException,
)

#status_list = ["SUCCESS", "WARNING", "FAILED", "SKIPPED"]
#QueueEntryStatusType = namedtuple("QueueEntryStatusType", status_list)
#QUEUE_ENTRY_STATUS = QueueEntryStatusType(0, 1, 2, 3)

"""
class QueueExecutionException(Exception):
    def __init__(self, message, origin):
        Exception.__init__(self, message, origin)
        self.message = message
        self.origin = origin
        self.stack_trace = traceback.format_exc()


class QueueAbortedException(QueueExecutionException):
    def __init__(self, message, origin):
        Exception.__init__(self, message, origin)
        self.origin = origin
        self.message = message
        self.stack_trace = traceback.format_exc()


class QueueSkippEntryException(QueueExecutionException):
    def __init__(self, message, origin):
        Exception.__init__(self, message, origin)
        self.origin = origin
        self.message = message
        self.stack_trace = traceback.format_exc()
"""

'''
class QueueEntryContainer(object):
    """
    A QueueEntryContainer has a list of queue entries, classes
    inheriting BaseQueueEntry, and a Queue object. The Queue object
    controls/handles the execution of the queue entries.
    """


    def __init__(self):
        object.__init__(self)
        self._queue_entry_list = []
        self._queue_controller = None
        self._parent_container = None

    def get_queue_entry_list(self):
        return self._queue_entry_list

    def enqueue(self, queue_entry, queue_controller=None):
        # A queue entry container has a QueueController object
        # which controls the execution of the tasks in the
        # container. The container is set to be its own controller
        # if none is given.
        if queue_controller:
            queue_entry.set_queue_controller(queue_controller)
        else:
            queue_entry.set_queue_controller(self)

        queue_entry.set_container(self)
        self._queue_entry_list.append(queue_entry)

    def dequeue(self, queue_entry):
        """
        Dequeues the QueueEntry <queue_entry> and returns the
        dequeued entry.

        Throws ValueError if the queue_entry is not in the queue.

        :param queue_entry: The queue entry to dequeue/remove.
        :type queue_entry: QueueEntry

        :returns: The dequeued entry.
        :rtype: QueueEntry
        """
        result = None
        index = None
        queue_entry.set_queue_controller(None)
        queue_entry.set_container(None)

        try:
            index = self._queue_entry_list.index(queue_entry)
        except ValueError:
            raise

        if index is not None:
            result = self._queue_entry_list.pop(index)

        log = logging.getLogger("queue_exec")
        log.info("dequeue called with: " + str(queue_entry))
        # log.info('Queue is :' + str(self.get_queue_controller()))

        return result

    def swap(self, queue_entry_a, queue_entry_b):
        """
        Swaps places between the two queue entries <queue_entry_a> and
        <queue_entry_b>.

        Throws a ValueError if one of the entries does not exist in the
        queue.

        :param queue_entry: Queue entry to swap
        :type queue_entry: QueueEntry

        :param queue_entry: Queue entry to swap
        :type queue_entry: QueueEntry
        """
        index_a = None
        index_b = None

        try:
            index_a = self._queue_entry_list.index(queue_entry_a)
        except ValueError:
            raise

        try:
            index_b = self._queue_entry_list.index(queue_entry_b)
        except ValueError:
            raise

        if (index_a is not None) and (index_b is not None):
            temp = self._queue_entry_list[index_a]
            self._queue_entry_list[index_a] = self._queue_entry_list[index_b]
            self._queue_entry_list[index_b] = temp

        log = logging.getLogger("queue_exec")
        log.info("swap called with: " + str(queue_entry_a) + ", " + str(queue_entry_b))
        log.info("Queue is :" + str(self.get_queue_controller()))

    def set_queue_controller(self, queue_controller):
        """
        Sets the queue controller, the object that controls execution
        of this QueueEntryContainer.

        :param queue_controller: The queue controller object.
        :type queue_controller: QueueController
        """
        self._queue_controller = queue_controller

    def get_queue_controller(self):
        """
        :returns: The queue controller
        :type queue_controller: QueueController
        """
        return self._queue_controller

    def set_container(self, queue_entry_container):
        """
        Sets the parent queue entry to <queue_entry_container>

        :param queue_entry_container:
        :type queue_entry_container: QueueEntryContainer
        """
        self._parent_container = queue_entry_container

    def get_container(self):
        """
        :returns: The parent QueueEntryContainer.
        :rtype: QueueEntryContainer
        """
        return self._parent_container


class BaseQueueEntry(QueueEntryContainer):
    """
    Base class for queue entry objects. Defines the overall
    interface and behaviour for a queue entry.
    """

    def __init__(self, view=None, data_model=None, view_set_queue_entry=True):
        QueueEntryContainer.__init__(self)
        self._data_model = None
        self._view = None
        self.set_data_model(data_model)
        self.set_view(view, view_set_queue_entry)
        self._checked_for_exec = False
        self.beamline_setup = None
        self.status = QUEUE_ENTRY_STATUS.SUCCESS
        self.type_str = ""

    # def __getstate__(self):
    #     return QueueEntryContainer.__getstate__(self)

    # def __setstate__(self, d):
    #     return QueueEntryContainer.__setstate__(self, d)

    def is_failed(self):
        return self.status == QUEUE_ENTRY_STATUS.FAILED

    def enqueue(self, queue_entry):
        """
        Method inherited from QueueEntryContainer, a derived class
        should newer need to override this method.
        """
        QueueEntryContainer.enqueue(self, queue_entry, self.get_queue_controller())

    def set_data_model(self, data_model):
        """
        Sets the model node of this queue entry to <data_model>

        :param data_model: The data model node.
        :type data_model: TaskNode
        """
        self._data_model = data_model

    def get_data_model(self):
        """
        :returns: The data model of this queue entry.
        :rtype: TaskNode
        """
        return self._data_model

    def set_view(self, view, view_set_queue_entry=True):
        """
        Sets the view of this queue entry to <view>. Makes the
        correspodning bi-directional connection if view_set_queue_entry
        is set to True. Which is normaly case, it can be usefull with
        'uni-directional' connection in some rare cases.

        :param view: The view to associate with this entry
        :type view: ViewItem

        :param view_set_queue_entry: Bi- or uni-directional
                                     connection to view.
        :type view_set_queue_entry: bool
        """
        if view:
            self._view = view

            if view_set_queue_entry:
                view.set_queue_entry(self)

    def get_view(self):
        """
        :returns the view:
        :rtype: ViewItem
        """
        return self._view

    def is_enabled(self):
        """
        :returns: True if this item is enabled.
        :rtype: bool
        """
        return self._checked_for_exec

    def set_enabled(self, state):
        """
        Enables or disables this entry, controls wether this item
        should be executed (enabled) or not (disabled)

        :param state: Enabled if state is True otherwise disabled.
        :type state: bool
        """
        self._checked_for_exec = state

    def execute(self):
        """
        Execute method, should be overriden my subclasses, defines
        the main body of the procedure to be performed when the entry
        is executed.

        The default executer calls excute on all child entries after
        this method but before post_execute.
        """
        logging.getLogger("queue_exec").info("Calling execute on: " + str(self))

    def pre_execute(self):
        """
        Procedure to be done before execute.
        """
        logging.getLogger("queue_exec").info("Calling pre_execute on: " + str(self))
        self.beamline_setup = self.get_queue_controller().getObjectByRole(
            "beamline_setup"
        )
        self.get_data_model().set_running(True)

    def post_execute(self):
        """
        Procedure to be done after execute, and execute of all
        children of this entry.
        """
        logging.getLogger("queue_exec").info("Calling post_execute on: " + str(self))
        view = self.get_view()

        view.setHighlighted(True)
        view.setOn(False)
        self.get_data_model().set_executed(True)
        self.get_data_model().set_running(False)
        self.get_data_model().set_enabled(False)
        self.set_enabled(False)
        self._set_background_color()

    def _set_background_color(self):
        view = self.get_view()

        if self.get_data_model().is_executed():
            """
            if self.status == QUEUE_ENTRY_STATUS.SUCCESS:
                view.setBackgroundColor(widget_colors.LIGHT_GREEN)
            elif self.status == QUEUE_ENTRY_STATUS.WARNING:
                view.setBackgroundColor(widget_colors.LIGHT_YELLOW)
            elif self.status == QUEUE_ENTRY_STATUS.FAILED:
                view.setBackgroundColor(widget_colors.LIGHT_RED)
            """
            view.set_background_color(self.status + 1)
        else:
            view.set_background_color(0)
            # view.setBackgroundColor(widget_colors.WHITE)

    def stop(self):
        """
        Stops the execution of this entry, should free
        external resources, cancel all pending processes and so on.
        """
        self.get_view().setText(1, "Stopped")
        logging.getLogger("queue_exec").info("Calling stop on: " + str(self))

    def handle_exception(self, ex):
        view = self.get_view()

        if view and isinstance(ex, QueueExecutionException):
            if ex.origin is self:
                # view.setBackgroundColor(widget_colors.LIGHT_RED)
                view.set_background_color(3)

    def __str__(self):
        s = "<%s object at %s> [" % (self.__class__.__name__, hex(id(self)))

        for entry in self._queue_entry_list:
            s += str(entry)

        return s + "]"

    def get_type_str(self):
        return self.type_str
'''

class DummyQueueEntry(BaseQueueEntry):
    def __init__(self, view=None, data_model=None):
        BaseQueueEntry.__init__(self, view, data_model)

    def execute(self):
        BaseQueueEntry.execute(self)
        self.get_view().setText(1, "Sleeping 5 s")
        time.sleep(5)

    def pre_execute(self):
        BaseQueueEntry.pre_execute(self)

    def post_execute(self):
        BaseQueueEntry.post_execute(self)


class TaskGroupQueueEntry(BaseQueueEntry):
    def __init__(self, view=None, data_model=None):
        BaseQueueEntry.__init__(self, view, data_model)
        self.lims_client_hwobj = None
        self.session_hwobj = None
        self.interleave_task = None
        self.interleave_items = None
        self.interleave_sw_list = None
        self.interleave_stoped = None

    def execute(self):
        BaseQueueEntry.execute(self)
        task_model = self.get_data_model()
        gid = task_model.lims_group_id

        do_new_dc_group = True
        # Do not create a new data collection group if one already exists
        # or if the current task group contains a GenericWorkflowQueueEntry
        if gid:
            do_new_dc_group = False
        elif len(self._queue_entry_list) > 0:
            if isinstance(self._queue_entry_list[0], GenericWorkflowQueueEntry):
                do_new_dc_group = False

        init_ref_images = False
        if do_new_dc_group:
            # Creating a collection group with the current session id
            # and a dummy exepriment type OSC. The experiment type
            # will be updated when the collections are stored.
            if task_model.interleave_num_images:
                init_ref_images = task_model.interleave_num_images
                group_data = {
                    "sessionId": self.session_hwobj.session_id,
                    "experimentType": "Collect - Multiwedge",
                }
            elif task_model.inverse_beam_num_images:
                init_ref_images = task_model.inverse_beam_num_images
                group_data = {
                    "sessionId": self.session_hwobj.session_id,
                    "experimentType": "Collect - Multiwedge",
                }
            else:
                group_data = {
                    "sessionId": self.session_hwobj.session_id,
                    "experimentType": "OSC",
                }

            sample_model = task_model.get_sample_node()
            # task_model.get_parent()
            if sample_model.lims_container_location > -1:
                group_data[
                    "actualContainerSlotInSC"
                ] = sample_model.lims_container_location
            if sample_model.lims_sample_location > -1:
                group_data[
                    "actualSampleSlotInContainer"
                ] = sample_model.lims_sample_location

            try:
                gid = self.lims_client_hwobj._store_data_collection_group(group_data)
                self.get_data_model().lims_group_id = gid
            except Exception as ex:
                msg = (
                    "Could not create the data collection group"
                    + " in LIMS. Reason: "
                    + str(ex)
                )
                raise QueueExecutionException(msg, self)

        self.interleave_items = []
        if init_ref_images:
            # At first all children are gathered together and
            # checked if interleave is set. For this implementation
            # interleave is just possible for discreet data collections
            ref_num_images = 0
            children_data_model_list = self._data_model.get_children()

            for child_data_model in children_data_model_list:
                if isinstance(child_data_model, queue_model_objects.DataCollection):
                    if task_model.inverse_beam_num_images is not None:
                        child_data_model.acquisitions[
                            0
                        ].acquisition_parameters.num_images /= 2
                    num_images = child_data_model.acquisitions[
                        0
                    ].acquisition_parameters.num_images

                    if num_images > init_ref_images:
                        if num_images > ref_num_images:
                            ref_num_images = num_images
                        interleave_item = {}
                        child_data_model.set_experiment_type(
                            EXPERIMENT_TYPE.COLLECT_MULTIWEDGE
                        )
                        interleave_item["data_model"] = child_data_model
                        for queue_entry in self._queue_entry_list:
                            if queue_entry.get_data_model() == child_data_model:
                                interleave_item["queue_entry"] = queue_entry
                                interleave_item["tree_item"] = queue_entry.get_view()
                        self.interleave_items.append(interleave_item)

                        if task_model.inverse_beam_num_images is not None:
                            inverse_beam_item = copy(interleave_item)
                            inverse_beam_item["data_model"] = interleave_item[
                                "data_model"
                            ].copy()
                            inverse_beam_item["data_model"].acquisitions[
                                0
                            ].acquisition_parameters.osc_start += 180
                            inverse_beam_item["data_model"].acquisitions[
                                0
                            ].acquisition_parameters.first_image = (
                                interleave_item["data_model"]
                                .acquisitions[0]
                                .acquisition_parameters.first_image
                                + interleave_item["data_model"]
                                .acquisitions[0]
                                .acquisition_parameters.num_images
                            )
                            self.interleave_items.append(inverse_beam_item)
        if len(self.interleave_items) > 1:
            interleave_num_images = task_model.interleave_num_images
            self.interleave_task = gevent.spawn(
                self.execute_interleaved, ref_num_images, init_ref_images
            )
            self.interleave_task.join()

    def execute_interleaved(self, ref_num_images, interleave_num_images):
        task_model = self.get_data_model()

        if task_model.interleave_num_images:
            method_type = "interleave"
        elif task_model.inverse_beam_num_images:
            method_type = "inverse beam"

        logging.getLogger("queue_exec").info(
            "Preparing %s data collection" % method_type
        )

        for interleave_item in self.interleave_items:
            interleave_item["queue_entry"].set_enabled(False)
            interleave_item["tree_item"].set_checkable(False)
            interleave_item["data_model"].lims_group_id = (
                interleave_item["data_model"].get_parent().lims_group_id
            )
            cpos = (
                interleave_item["data_model"]
                .acquisitions[0]
                .acquisition_parameters.centred_position
            )
            # sample = interleave_item["data_model"].get_parent().get_parent()
            sample = interleave_item["data_model"].get_sample_node()
            empty_cpos = queue_model_objects.CentredPosition()
            param_list = queue_model_objects.to_collect_dict(
                interleave_item["data_model"],
                self.session_hwobj,
                sample,
                cpos if cpos != empty_cpos else None,
            )
            self.collect_hwobj.prepare_interleave(
                interleave_item["data_model"], param_list
            )

        self.interleave_sw_list = queue_model_objects.create_interleave_sw(
            self.interleave_items, ref_num_images, interleave_num_images
        )

        self._queue_controller.emit("queue_interleaved_started")
        for item_index, item in enumerate(self.interleave_sw_list):
            if not self.interleave_stoped:
                self.get_view().setText(
                    1,
                    "Subwedge %d:%d)"
                    % ((item_index + 1), len(self.interleave_sw_list)),
                )
                acq_par = (
                    self.interleave_items[item["collect_index"]]["data_model"]
                    .acquisitions[0]
                    .acquisition_parameters
                )
                acq_first_image = acq_par.first_image

                acq_par.first_image = item["sw_first_image"]
                acq_par.num_images = item["sw_actual_size"]
                acq_par.osc_start = item["sw_osc_start"]
                acq_par.in_interleave = (
                    acq_first_image,
                    acq_first_image + item["collect_num_images"] - 1,
                )
                self.interleave_items[item["collect_index"]][
                    "queue_entry"
                ].in_queue = item_index < (len(self.interleave_sw_list) - 1)

                msg = "Executing %s collection (subwedge %d:%d, " % (
                    method_type,
                    (item_index + 1),
                    len(self.interleave_sw_list),
                )
                msg += "from %d to %d, " % (
                    acq_par.first_image,
                    acq_par.first_image + acq_par.num_images - 1,
                )
                msg += "osc start: %.2f, osc total range: %.2f)" % (
                    item["sw_osc_start"],
                    item["sw_osc_range"],
                )
                logging.getLogger("user_level_log").info(msg)

                try:
                    self.interleave_items[item["collect_index"]][
                        "queue_entry"
                    ].pre_execute()
                    self.interleave_items[item["collect_index"]][
                        "queue_entry"
                    ].execute()
                except BaseException:
                    pass
                self.interleave_items[item["collect_index"]][
                    "queue_entry"
                ].post_execute()
                self.interleave_items[item["collect_index"]]["tree_item"].setText(
                    1,
                    "Subwedge %d:%d done"
                    % (item["collect_index"] + 1, item["sw_index"] + 1),
                )

                sig_data = {
                    "current_idx": item_index,
                    "item": item,
                    "nitems": len(self.interleave_sw_list),
                    "sw_size": interleave_num_images,
                }

                self._queue_controller.emit("queue_interleaved_sw_done", (sig_data,))

        if not self.interleave_stoped:
            logging.getLogger("queue_exec").info(
                "%s collection finished" % method_type.title()
            )
            self._queue_controller.emit("queue_interleaved_finished")

        self.interleave_task = None

    def pre_execute(self):
        BaseQueueEntry.pre_execute(self)
        self.lims_client_hwobj = self.beamline_setup.lims_client_hwobj
        self.session_hwobj = self.beamline_setup.session_hwobj
        self.collect_hwobj = self.beamline_setup.collect_hwobj

    def post_execute(self):
        BaseQueueEntry.post_execute(self)
        self.get_view().setText(1, "")

    def stop(self):
        BaseQueueEntry.stop(self)
        if self.interleave_task:
            self.interleave_stoped = True
            self.interleave_task.kill()
        self.get_view().setText(1, "Interleave stoped")


class SampleQueueEntry(BaseQueueEntry):
    """
    Defines the behaviour of sample queue entries. Mounting, launching centring
    and so on.
    """

    def __init__(self, view=None, data_model=None):
        BaseQueueEntry.__init__(self, view, data_model)
        self.sample_changer_hwobj = None
        self.diffractometer_hwobj = None
        self.plate_manipulator_hwobj = None
        self.sample_centring_result = None

    def __getstate__(self):
        d = dict(self.__dict__)
        d["sample_centring_result"] = None
        return d

    def __setstate__(self, d):
        self.__dict__.update(d)

    def execute(self):
        BaseQueueEntry.execute(self)
        log = logging.getLogger("queue_exec")
        sc_used = not self._data_model.free_pin_mode

        # Only execute samples with collections and when sample changer is used
        if len(self.get_data_model().get_children()) != 0 and sc_used:
            if self.diffractometer_hwobj.in_plate_mode():
                return
            else:
                mount_device = self.sample_changer_hwobj

            if mount_device is not None:
                log.info("Loading sample " + str(self._data_model.location))
                sample_mounted = mount_device.is_mounted_sample(
                    tuple(self._data_model.location)
                )
                if not sample_mounted:
                    self.sample_centring_result = gevent.event.AsyncResult()
                    try:
                        mount_sample(
                            self.beamline_setup,
                            self._view,
                            self._data_model,
                            self.centring_done,
                            self.sample_centring_result,
                        )
                        #self.beamline_setup.diffractometer_hwobj.close_kappa_task()
                        #self.beamline_setup.shape_history_hwobj.start_auto_centring(wait=True)
                        #time.sleep(2)
                    except Exception as e:
                        self._view.setText(1, "Error loading")
                        msg = (
                            "Error loading sample, "# please check"
                            #+ " sample changer: "
                            + str(e)
                        )
                        log.error(msg)
                        self.status = QUEUE_ENTRY_STATUS.FAILED
                        if isinstance(e, QueueSkippEntryException):
                            raise
                        else:
                            raise QueueExecutionException(str(e), self)
                else:
                    log.info("Sample already mounted")
            else:
                msg = (
                    "SampleQueuItemPolicy does not have any "
                    + "sample changer hardware object, cannot "
                    + "mount sample"
                )
                log.info(msg)
            self.get_view().setText(1, "")

    def centring_done(self, success, centring_info):
        if not success:
            msg = (
                "Loop centring failed or was cancelled, " + "please continue manually."
            )
            logging.getLogger("user_level_log").warning(msg)
        self.sample_centring_result.set(centring_info)

    def pre_execute(self):
        BaseQueueEntry.pre_execute(self)
        try:
            self.sample_changer_hwobj = self.beamline_setup.sample_changer_hwobj
        except AttributeError:
            self.sample_changer_hwobj = None
        self.diffractometer_hwobj = self.beamline_setup.diffractometer_hwobj
        try:
            self.plate_manipulator_hwobj = self.beamline_setup.plate_manipulator_hwobj
        except AttributeError:
            self.plate_manipulator_hwobj = None
        self.shape_history = self.beamline_setup.shape_history_hwobj

    def post_execute(self):
        BaseQueueEntry.post_execute(self)
        params = []

        # Start grouped processing, get information from each collection
        # and call autoproc with grouped processing option
        for child in self.get_data_model().get_children():
            for grand_child in child.get_children():
                if isinstance(grand_child, queue_model_objects.DataCollection):
                    xds_dir = grand_child.acquisitions[0].path_template.xds_dir
                    residues = grand_child.processing_parameters.num_residues
                    anomalous = grand_child.processing_parameters.anomalous
                    space_group = grand_child.processing_parameters.space_group
                    cell = grand_child.processing_parameters.get_cell_str()
                    inverse_beam = grand_child.acquisitions[
                        0
                    ].acquisition_parameters.inverse_beam

                    params.append(
                        {
                            "collect_id": grand_child.id,
                            "xds_dir": xds_dir,
                            "residues": residues,
                            "anomalous": anomalous,
                            "spacegroup": space_group,
                            "cell": cell,
                            "inverse_beam": inverse_beam,
                        }
                    )

        try:
            programs = self.beamline_setup.collect_hwobj["auto_processing"]
            autoprocessing.start(programs, "end_multicollect", params)
        except KeyError:
            pass

        self._set_background_color()
        self._view.setText(1, "")

    def _set_background_color(self):
        BaseQueueEntry._set_background_color(self)

    def get_type_str(self):
        return "Sample"


class BasketQueueEntry(BaseQueueEntry):
    def __init__(self, view=None, data_model=None):
        BaseQueueEntry.__init__(self, view, data_model)


class SampleCentringQueueEntry(BaseQueueEntry):
    """
    Entry for centring a sample
    """

    def __init__(self, view=None, data_model=None):
        BaseQueueEntry.__init__(self, view, data_model)
        self.sample_changer_hwobj = None
        self.diffractometer_hwobj = None
        self.shape_history = None

    def __setstate__(self, d):
        self.__dict__.update(d)

    def __getstate__(self):
        d = dict(self.__dict__)
        d["move_kappa_phi_task"] = None
        return d

    def execute(self):
        BaseQueueEntry.execute(self)

        self.get_view().setText(1, "Waiting for input")
        log = logging.getLogger("user_level_log")

        data_model = self.get_data_model()

        kappa = data_model.get_kappa()
        kappa_phi = data_model.get_kappa_phi()

        # kappa and kappa_phi settings are applied first, and assume that the
        # beamline does have axes with exactly these names
        #
        # Other motor_positions are applied afterwards, but in random order.
        # motor_positions override kappa and kappa_phi if both are set
        #
        # Since setting one motor can change the position of another
        # (on ESRF ID30B setting kappa and kappa_phi changes the translation motors)
        # the order is important.

        if kappa is None:
            d_kappa = 0
        else:
            d_kappa = (
                self.diffractometer_hwobj.current_motor_positions["kappa"] - kappa
            )
            d_kappa = min([abs(d_kappa), abs(d_kappa - 360), abs(d_kappa + 360)])
        if kappa_phi is None:
            d_kappa_phi = 0
        else:
            d_kappa_phi = (
                self.diffractometer_hwobj.current_motor_positions["kappa_phi"]
                - kappa_phi
            )
            d_kappa_phi = min(
                [abs(d_kappa_phi), abs(d_kappa_phi - 360), abs(d_kappa_phi + 360)]
            )

        if d_kappa or d_kappa_phi:
            # Only move if either kappa or phi are changing
            ARBITRARY_TOLERANCE = 0.5
            if d_kappa > ARBITRARY_TOLERANCE or d_kappa_phi > ARBITRARY_TOLERANCE:
                # The point is that it is very rare to centre at an orientation
                # different from the current one, and when you do you want
                # the lights turned on etc.
                self.diffractometer_hwobj.set_phase("Centring", timeout = 20)
            if (
                not hasattr(self.diffractometer_hwobj, "in_kappa_mode")
                or self.diffractometer_hwobj.in_kappa_mode()
            ):
                # NB the timeout parameter is unnecesary; 60s timeout is the default
                self.diffractometer_hwobj.move_kappa_and_phi(
                    kappa, kappa_phi, timeout = 60
                )

        motor_positions = dict(
            tt
            for tt in data_model.get_other_motor_positions().items()
            if tt[1] is not None
        )
        if motor_positions:
            self.diffractometer_hwobj.move_motors(motor_positions)

        log.warning(
            "Please center a new or select an existing point and press continue."
        )
        self.get_queue_controller().pause(True)

        shapes = list(self.shape_history.get_selected_shapes())
        if shapes:
            pos = shapes[0]
            if hasattr(pos, "get_centred_position"):
                cpos = pos.get_centred_position()
            else:
                cpos = pos.get_centred_positions()[0]
        else:
            msg = "No centred position selected, using current position."
            log.info(msg)

            # Create a centred positions of the current position
            pos_dict = self.diffractometer_hwobj.getPositions()
            cpos = queue_model_objects.CentredPosition(pos_dict)

        self._data_model.set_centring_result(cpos)

        self.get_view().setText(1, "Input accepted")

    def pre_execute(self):
        BaseQueueEntry.pre_execute(self)
        self.sample_changer_hwobj = self.beamline_setup.sample_changer_hwobj
        self.diffractometer_hwobj = self.beamline_setup.diffractometer_hwobj
        self.shape_history = self.beamline_setup.shape_history_hwobj

    def post_execute(self):
        # If centring is executed once then we dont have to execute it again
        self.get_view().set_checkable(False)
        BaseQueueEntry.post_execute(self)

    def get_type_str(self):
        return "Sample centering"


class DataCollectionQueueEntry(BaseQueueEntry):
    """
    Defines the behaviour of a data collection.
    """

    def __init__(self, view=None, data_model=None, view_set_queue_entry=True):
        BaseQueueEntry.__init__(self, view, data_model, view_set_queue_entry)

        self.collect_hwobj = None
        self.diffractometer_hwobj = None
        self.collect_task = None
        self.centring_task = None
        self.shape_history = None
        self.session = None
        self.lims_client_hwobj = None
        self.enable_take_snapshots = True
        self.enable_store_in_lims = True
        self.in_queue = False

        self.parallel_processing_hwobj = None

    def __setstate__(self, d):
        self.__dict__.update(d)

    def __getstate__(self):
        d = dict(self.__dict__)
        d["collect_task"] = None
        d["centring_task"] = None
        d["shape_history"] = self.shape_history.name() if self.shape_history else None
        d["session"] = self.session.name() if self.session else None
        d["lims_client_hwobj"] = (
            self.lims_client_hwobj.name() if self.lims_client_hwobj else None
        )
        return d

    def execute(self):
        BaseQueueEntry.execute(self)
        data_collection = self.get_data_model()

        if data_collection:
            acq_params = data_collection.acquisitions[0].acquisition_parameters
            cpos = acq_params.centred_position

            empty_cpos = all(mpos is None for mpos in cpos.as_dict().values())

            if empty_cpos and data_collection.center_before_collect:
                _p, _s = center_before_collect(
                    self.get_view(),
                    self.diffractometer_hwobj,
                    self.get_queue_controller(),
                    self.shape_history,
                )

                acq_params.centred_position = _p

            self.collect_dc(data_collection, self.get_view())

        if self.shape_history:
            self.shape_history.de_select_all()

    def pre_execute(self):
        BaseQueueEntry.pre_execute(self)

        self.lims_client_hwobj = self.beamline_setup.lims_client_hwobj
        self.collect_hwobj = self.beamline_setup.collect_hwobj
        self.diffractometer_hwobj = self.beamline_setup.diffractometer_hwobj
        self.shape_history = self.beamline_setup.shape_history_hwobj
        self.session = self.beamline_setup.session_hwobj

        try:
            self.parallel_processing_hwobj = self.beamline_setup.parallel_processing_hwobj
        except AttributeError:
            self.parallel_processing_hwobj = None

        qc = self.get_queue_controller()

        qc.connect(self.collect_hwobj, "collectStarted", self.collect_started)
        qc.connect(self.collect_hwobj, "collectNumberOfFrames", self.preparing_collect)
        qc.connect(
            self.collect_hwobj, "collectOscillationStarted", self.collect_osc_started
        )
        qc.connect(self.collect_hwobj, "collectOscillationFailed", self.collect_failed)
        qc.connect(
            self.collect_hwobj, "collectOscillationFinished", self.collect_finished
        )
        qc.connect(self.collect_hwobj, "collectImageTaken", self.image_taken)
        qc.connect(
            self.collect_hwobj, "collectNumberOfFrames", self.collect_number_of_frames
        )

        if self.parallel_processing_hwobj is not None:
            qc.connect(
                self.parallel_processing_hwobj,
                "processingFinished",
                self.processing_finished,
            )
            qc.connect(
                self.parallel_processing_hwobj,
                "processingFailed",
                self.processing_failed,
            )

        data_model = self.get_data_model()

        if data_model.get_parent():
            gid = data_model.get_parent().lims_group_id
            data_model.lims_group_id = gid

    def post_execute(self):
        BaseQueueEntry.post_execute(self)
        qc = self.get_queue_controller()

        qc.disconnect(self.collect_hwobj, "collectStarted", self.collect_started)
        qc.disconnect(
            self.collect_hwobj, "collectNumberOfFrames", self.preparing_collect
        )
        qc.disconnect(
            self.collect_hwobj, "collectOscillationStarted", self.collect_osc_started
        )
        qc.disconnect(
            self.collect_hwobj, "collectOscillationFailed", self.collect_failed
        )
        qc.disconnect(
            self.collect_hwobj, "collectOscillationFinished", self.collect_finished
        )
        qc.disconnect(self.collect_hwobj, "collectImageTaken", self.image_taken)
        qc.disconnect(
            self.collect_hwobj, "collectNumberOfFrames", self.collect_number_of_frames
        )

        if self.parallel_processing_hwobj is not None:
            qc.disconnect(
                self.parallel_processing_hwobj,
                "processingFinished",
                self.processing_finished,
            )
            qc.disconnect(
                self.parallel_processing_hwobj,
                "processingFailed",
                self.processing_failed,
            )

        self.get_view().set_checkable(False)

    def collect_dc(self, dc, list_item):
        log = logging.getLogger("user_level_log")

        if self.collect_hwobj:
            acq_1 = dc.acquisitions[0]
            acq_1.acquisition_parameters.in_queue = self.in_queue
            cpos = acq_1.acquisition_parameters.centred_position
            sample = self.get_data_model().get_sample_node()
            self.collect_hwobj.run_processing_after = dc.run_processing_after
            self.collect_hwobj.aborted_by_user = None
            self.processing_task = None

            try:
                if dc.experiment_type is EXPERIMENT_TYPE.HELICAL:
                    acq_1, acq_2 = (dc.acquisitions[0], dc.acquisitions[1])
                    self.collect_hwobj.set_helical(True)
                    self.collect_hwobj.set_mesh(False)
                    start_cpos = acq_1.acquisition_parameters.centred_position
                    end_cpos = acq_2.acquisition_parameters.centred_position
                    helical_oscil_pos = {
                        "1": start_cpos.as_dict(),
                        "2": end_cpos.as_dict(),
                    }
                    self.collect_hwobj.set_helical_pos(helical_oscil_pos)
                    # msg = "Helical data collection, moving to start position"
                    # log.info(msg)
                    # list_item.setText(1, "Moving sample")
                elif dc.experiment_type is EXPERIMENT_TYPE.MESH:
                    mesh_nb_lines = acq_1.acquisition_parameters.num_lines
                    mesh_total_nb_frames = acq_1.acquisition_parameters.num_images
                    mesh_range = acq_1.acquisition_parameters.mesh_range
                    mesh_center = acq_1.acquisition_parameters.centred_position
                    self.collect_hwobj.set_mesh_scan_parameters(
                        mesh_nb_lines, mesh_total_nb_frames, mesh_center, mesh_range
                    )
                    self.collect_hwobj.set_helical(False)
                    self.collect_hwobj.set_mesh(True)
                    dc.grid.used_count += 1
                else:
                    self.collect_hwobj.set_helical(False)
                    self.collect_hwobj.set_mesh(False)

                if (
                    dc.run_processing_parallel
                    and acq_1.acquisition_parameters.num_images > 4
                    and self.parallel_processing_hwobj is not None
                ):
                    self.processing_task = gevent.spawn(
                        self.parallel_processing_hwobj.run_processing, dc
                    )

                empty_cpos = queue_model_objects.CentredPosition()

                if cpos != empty_cpos:
                    self.shape_history.select_shape_with_cpos(cpos)
                else:
                    pos_dict = self.diffractometer_hwobj.getPositions()
                    cpos = queue_model_objects.CentredPosition(pos_dict)
                    snapshot = self.shape_history.get_snapshot()
                    acq_1.acquisition_parameters.centred_position = cpos
                    acq_1.acquisition_parameters.centred_position.snapshot_image = (
                        snapshot
                    )

                self.shape_history.inc_used_for_collection(cpos)

                param_list = queue_model_objects.to_collect_dict(
                    dc, self.session, sample, cpos if cpos != empty_cpos else None
                )

                self.collect_task = self.collect_hwobj.collect(
                    COLLECTION_ORIGIN_STR.MXCUBE, param_list
                )
                self.collect_task.get()

                if "collection_id" in param_list[0]:
                    dc.id = param_list[0]["collection_id"]

                dc.acquisitions[0].path_template.xds_dir = param_list[0]["xds_dir"]

            except gevent.GreenletExit:
                # log.warning("Collection stopped by user.")
                list_item.setText(1, "Stopped")
                raise QueueAbortedException("queue stopped by user", self)
            except Exception as ex:
                raise QueueExecutionException(str(ex), self)
        else:
            log.error(
                "Could not call the data collection routine,"
                + " check the beamline configuration"
            )
            list_item.setText(1, "Failed")
            msg = (
                "Could not call the data collection"
                + " routine, check the beamline configuration"
            )
            raise QueueExecutionException(msg, self)

    def collect_started(self, owner, num_oscillations):
        logging.getLogger("user_level_log").info("Collection: Started")
        self.get_view().setText(1, "Collecting...")

    def collect_number_of_frames(self, number_of_images=0, exposure_time=0):
        pass

    def image_taken(self, image_number):
        if image_number > 0:
            num_images = (
                self.get_data_model().acquisitions[0].acquisition_parameters.num_images
            )
            num_images += (
                self.get_data_model().acquisitions[0].acquisition_parameters.first_image
                - 1
            )
            self.get_view().setText(1, str(image_number) + "/" + str(num_images))

    def preparing_collect(self, number_images=0, exposure_time=0):
        self.get_view().setText(1, "Collecting")

    def collect_failed(self, owner, state, message, *args):
        # this is to work around the remote access problem
        dispatcher.send("collect_finished")
        self.get_view().setText(1, "Failed")
        self.status = QUEUE_ENTRY_STATUS.FAILED
        logging.getLogger("queue_exec").error(message.replace("\n", " "))
        raise QueueExecutionException(message.replace("\n", " "), self)

    def collect_osc_started(
        self, owner, blsampleid, barcode, location, collect_dict, osc_id
    ):
        self.get_view().setText(1, "Preparing")

    def collect_finished(self, owner, state, message, *args):
        # this is to work around the remote access problem
        dispatcher.send("collect_finished")
        self.get_view().setText(1, "Collection done")
        logging.getLogger("user_level_log").info("Collection: Done")

        if self.processing_task is not None:
            self.get_view().setText(1, "Processing...")
            logging.getLogger("user_level_log").warning("Processing: Please wait...")
            self.parallel_processing_hwobj.done_event.wait(timeout=60)
            self.parallel_processing_hwobj.done_event.clear()

    def stop(self):
        BaseQueueEntry.stop(self)

        self.collect_hwobj.stop_collect()

        if self.processing_task is not None:
            self.parallel_processing_hwobj.stop_processing()
            #self.processing_task.kill()
            logging.getLogger("user_level_log").info("Processing: Stopped")
        if self.centring_task is not None:
            self.centring_task.kill(block=False)

        self.get_view().setText(1, "Stopped")
        logging.getLogger("queue_exec").info("Calling stop on: " + str(self))
        logging.getLogger("user_level_log").info("Collection: Stopped")
        # this is to work around the remote access problem
        dispatcher.send("collect_finished")
        raise QueueAbortedException("Queue stopped", self)

    def processing_finished(self):
        dispatcher.send("collect_finished")
        self.processing_task = None
        self.get_view().setText(1, "Done")
        logging.getLogger("user_level_log").info("Processing: Done")

    def processing_failed(self):
        self.processing_task = None
        self.get_view().setText(1, "Processing failed")
        logging.getLogger("user_level_log").error("Processing: Failed")

    def get_type_str(self):
        data_model = self.get_data_model()
        if data_model.is_helical():
            return "Helical"
        elif data_model.is_mesh():
            return "Mesh"
        else:
            return "OSC"

    def add_processing_msg(self, time, method, status, msg):
        data_model = self.get_data_model()
        data_model.add_processing_msg(time, method, status, msg)
        self.get_view().update_tool_tip()
        self.get_view().update_processing_status()


class CharacterisationGroupQueueEntry(BaseQueueEntry):
    """
    Used to group (couple) a CollectionQueueEntry and a
    CharacterisationQueueEntry, creating a virtual entry for characterisation.
    """

    def __init__(self, view=None, data_model=None, view_set_queue_entry=True):
        BaseQueueEntry.__init__(self, view, data_model, view_set_queue_entry)
        self.dc_qe = None
        self.char_qe = None
        self.in_queue = False

    def execute(self):
        BaseQueueEntry.execute(self)

    def pre_execute(self):
        BaseQueueEntry.pre_execute(self)
        char = self.get_data_model()
        reference_image_collection = char.reference_image_collection

        # Trick to make sure that the reference collection has a sample.
        reference_image_collection._parent = char.get_parent()

        gid = self.get_data_model().get_parent().lims_group_id
        reference_image_collection.lims_group_id = gid

        # Enqueue the reference collection and the characterisation routine.
        dc_qe = DataCollectionQueueEntry(
            self.get_view(), reference_image_collection, view_set_queue_entry=False
        )
        dc_qe.set_enabled(True)
        dc_qe.in_queue = self.in_queue
        self.enqueue(dc_qe)
        self.dc_qe = dc_qe
        if char.run_characterisation:
            try:
                char_qe = CharacterisationQueueEntry(
                    self.get_view(), char, view_set_queue_entry=False
                )
            except Exception as ex:
                logging.getLogger("HWR").exception(
                    "Could not create CharacterisationQueueEntry"
                )
                self.char_qe = None
            else:
                char_qe.set_enabled(True)
                self.enqueue(char_qe)
                self.char_qe = char_qe

    def post_execute(self):
        if self.char_qe:
            self.status = self.char_qe.status
        else:
            self.status = self.dc_qe.status
        BaseQueueEntry.post_execute(self)


class CharacterisationQueueEntry(BaseQueueEntry):
    """
    Defines the behaviour of a characterisation
    """

    def __init__(self, view=None, data_model=None, view_set_queue_entry=True):

        BaseQueueEntry.__init__(self, view, data_model, view_set_queue_entry)
        self.data_analysis_hwobj = None
        self.diffractometer_hwobj = None
        self.queue_model_hwobj = None
        self.session_hwobj = None
        self.edna_result = None
        self.auto_add_diff_plan = True

    def __getstate__(self):
        d = BaseQueueEntry.__getstate__(self)

        d["data_analysis_hwobj"] = (
            self.data_analysis_hwobj.name() if self.data_analysis_hwobj else None
        )
        d["diffractometer_hwobj"] = (
            self.diffractometer_hwobj.name() if self.diffractometer_hwobj else None
        )
        d["queue_model_hwobj"] = (
            self.queue_model_hwobj.name() if self.queue_model_hwobj else None
        )
        d["session_hwobj"] = self.session_hwobj.name() if self.session_hwobj else None

        return d

    def __setstate__(self, d):
        BaseQueueEntry.__setstate__(self, d)

        self.data_analysis_hwobj = (
            HardwareRepository.getHardwareRepository().getHardwareObject(
                d["data_analysis_hwobj"]
            )
            if d["data_analysis_hwobj"]
            else None
        )
        self.diffractometer_hwobj = (
            HardwareRepository.getHardwareRepository().getHardwareObject(
                d["diffractometer_hwobj"]
            )
            if d["diffractometer_hwobj"]
            else None
        )
        self.queue_model_hwobj = (
            HardwareRepository.getHardwareRepository().getHardwareObject(
                d["queue_model_hwobj"]
            )
            if d["queue_model_hwobj"]
            else None
        )
        self.session_hwobj = (
            HardwareRepository.getHardwareRepository().getHardwareObject(
                d["session_hwobj"]
            )
            if d["session_hwobj"]
            else None
        )

    def execute(self):
        BaseQueueEntry.execute(self)

        if self.data_analysis_hwobj is not None:
            if self.get_data_model().wait_result:
                logging.getLogger("user_level_log").warning(
                    "Characterisation: Please wait ..."
                )
                self.start_char()
            else:
                logging.getLogger("user_level_log").info(
                    "Characterisation: Started in the background"
                )
                gevent.spawn(self.start_char)

    def start_char(self):
        log = logging.getLogger("user_level_log")
        self.get_view().setText(1, "Characterising")
        #log.info("Characterising, please wait ...")
        char = self.get_data_model()
        reference_image_collection = char.reference_image_collection
        characterisation_parameters = char.characterisation_parameters

        if self.data_analysis_hwobj is not None:
            edna_input = self.data_analysis_hwobj.from_params(
                reference_image_collection, characterisation_parameters
            )

            self.edna_result = self.data_analysis_hwobj.characterise(edna_input)

        if self.edna_result:
            #log.info("Characterisation completed.")

            char.html_report = self.data_analysis_hwobj.get_html_report(
                self.edna_result
            )

            try:
                strategy_result = (
                    self.edna_result.getCharacterisationResult().getStrategyResult()
                )
            except BaseException:
                strategy_result = None

            if strategy_result:
                collection_plan = strategy_result.getCollectionPlan()
            else:
                collection_plan = None

            if collection_plan:
                if char.auto_add_diff_plan:
                    # default action
                    self.handle_diffraction_plan(self.edna_result, None)
                else:
                    collections = queue_model_objects.dc_from_edna_output(
                        self.edna_result,
                        char.reference_image_collection,
                        None,  # new_dcg_model
                        None,  # sample_data_model
                        self.beamline_setup,
                    )
                    char.diffraction_plan.append(collections)
                    self.queue_model_hwobj.emit(
                        "diff_plan_available", (char, collections)
                    )

                self.get_view().setText(1, "Done")
            else:
                self.get_view().setText(1, "No result")
                self.status = QUEUE_ENTRY_STATUS.WARNING
                log.warning(
                    "Characterisation completed "
                    + "successfully but without collection plan."
                )
        else:
            self.get_view().setText(1, "Charact. Failed")

            if self.data_analysis_hwobj.is_running():
                log.error("EDNA-Characterisation, software is not responding.")
                log.error(
                    "Characterisation completed with error: "
                    + " data analysis server is not responding."
                )
            else:
                log.error("EDNA-Characterisation completed with a failure.")
                log.error("Characterisation completed with errors.")

        char.set_executed(True)
        self.get_view().setHighlighted(True)

    def handle_diffraction_plan(self, edna_result, edna_collections):
        char = self.get_data_model()
        reference_image_collection = char.reference_image_collection

        dcg_model = char.get_parent()
        sample_data_model = dcg_model.get_parent()

        new_dcg_name = "Diffraction plan"
        new_dcg_num = dcg_model.get_parent().get_next_number_for_name(new_dcg_name)

        new_dcg_model = queue_model_objects.TaskGroup()
        new_dcg_model.set_enabled(False)
        new_dcg_model.set_name(new_dcg_name)
        new_dcg_model.set_number(new_dcg_num)
        new_dcg_model.set_origin(char._node_id)

        self.queue_model_hwobj.add_child(sample_data_model, new_dcg_model)
        if edna_collections is None:
            edna_collections = queue_model_objects.dc_from_edna_output(
                edna_result,
                reference_image_collection,
                new_dcg_model,
                sample_data_model,
                self.beamline_setup,
            )
        for edna_dc in edna_collections:
            path_template = edna_dc.acquisitions[0].path_template
            run_number = self.queue_model_hwobj.get_next_run_number(path_template)
            path_template.run_number = run_number
            path_template.compression = char.diff_plan_compression

            edna_dc.set_enabled(char.run_diffraction_plan)
            edna_dc.set_name(path_template.get_prefix())
            edna_dc.set_number(path_template.run_number)
            self.queue_model_hwobj.add_child(new_dcg_model, edna_dc)

        return edna_collections

    def pre_execute(self):
        BaseQueueEntry.pre_execute(self)
        self.get_view().setOn(True)
        self.get_view().setHighlighted(False)

        self.data_analysis_hwobj = self.beamline_setup.data_analysis_hwobj
        self.diffractometer_hwobj = self.beamline_setup.diffractometer_hwobj
        # should be an other way how to get queue_model_hwobj:
        self.queue_model_hwobj = self._queue_controller.queue_model_hwobj
        self.session_hwobj = self.beamline_setup.session_hwobj

    def post_execute(self):
        BaseQueueEntry.post_execute(self)

    def get_type_str(self):
        return "Characterisation"

    def stop(self):
        BaseQueueEntry.stop(self)
        self.data_analysis_hwobj.stop()


class EnergyScanQueueEntry(BaseQueueEntry):
    def __init__(self, view=None, data_model=None):
        BaseQueueEntry.__init__(self, view, data_model)
        self.energy_scan_hwobj = None
        self.session_hwobj = None
        self.energy_scan_task = None
        self._failed = False

    def __getstate__(self):
        d = dict(self.__dict__)
        d["energy_scan_task"] = None
        return d

    def __setstate__(self, d):
        self.__dict__.update(d)

    def execute(self):
        BaseQueueEntry.execute(self)

        if self.energy_scan_hwobj:
            energy_scan = self.get_data_model()
            self.get_view().setText(1, "Starting energy scan")

            sample_model = self.get_data_model().get_sample_node()

            sample_lims_id = sample_model.lims_id

            # No sample id, pass None to startEnergyScan
            if sample_lims_id == -1:
                sample_lims_id = None

            self.energy_scan_task = gevent.spawn(
                self.energy_scan_hwobj.startEnergyScan,
                energy_scan.element_symbol,
                energy_scan.edge,
                energy_scan.path_template.directory,
                energy_scan.path_template.get_prefix(),
                self.session_hwobj.session_id,
                sample_lims_id,
            )

        self.energy_scan_hwobj.ready_event.wait()
        self.energy_scan_hwobj.ready_event.clear()

    def pre_execute(self):
        BaseQueueEntry.pre_execute(self)
        self._failed = False
        self.energy_scan_hwobj = self.beamline_setup.energyscan_hwobj
        self.session_hwobj = self.beamline_setup.session_hwobj

        qc = self.get_queue_controller()

        qc.connect(
            self.energy_scan_hwobj, "scanStatusChanged", self.energy_scan_status_changed
        )

        qc.connect(
            self.energy_scan_hwobj, "energyScanStarted", self.energy_scan_started
        )

        qc.connect(
            self.energy_scan_hwobj, "energyScanFinished", self.energy_scan_finished
        )

        qc.connect(self.energy_scan_hwobj, "energyScanFailed", self.energy_scan_failed)

    def post_execute(self):
        BaseQueueEntry.post_execute(self)
        qc = self.get_queue_controller()

        qc.disconnect(
            self.energy_scan_hwobj, "scanStatusChanged", self.energy_scan_status_changed
        )

        qc.disconnect(
            self.energy_scan_hwobj, "energyScanStarted", self.energy_scan_started
        )

        qc.disconnect(
            self.energy_scan_hwobj, "energyScanFinished", self.energy_scan_finished
        )

        qc.disconnect(
            self.energy_scan_hwobj, "energyScanFailed", self.energy_scan_failed
        )

        if self._failed:
            raise QueueAbortedException("Queue stopped", self)
        self.get_view().set_checkable(False)

    def energy_scan_status_changed(self, msg):
        logging.getLogger("user_level_log").info(msg)

    def energy_scan_started(self, *args):
        logging.getLogger("user_level_log").info("Energy scan started.")
        self.get_view().setText(1, "In progress")

    def energy_scan_finished(self, scan_info):
        self.get_view().setText(1, "Done")

        energy_scan = self.get_data_model()

        (
            pk,
            fppPeak,
            fpPeak,
            ip,
            fppInfl,
            fpInfl,
            rm,
            chooch_graph_x,
            chooch_graph_y1,
            chooch_graph_y2,
            title,
        ) = self.energy_scan_hwobj.doChooch(
            energy_scan.element_symbol,
            energy_scan.edge,
            energy_scan.path_template.directory,
            energy_scan.path_template.get_archive_directory(),
            "%s_%d"
            % (
                energy_scan.path_template.get_prefix(),
                energy_scan.path_template.run_number,
            ),
        )
        # scan_file_archive_path,
        # scan_file_path)

        # Trying to get the sample from the EnergyScan model instead through
        # the view. Keeping the old way fore backward compatability
        if energy_scan.sample:
            sample = energy_scan.sample
        else:
            sample = self.get_view().parent().parent().get_model()

        sample.crystals[0].energy_scan_result.peak = pk
        sample.crystals[0].energy_scan_result.inflection = ip
        sample.crystals[0].energy_scan_result.first_remote = rm
        sample.crystals[0].energy_scan_result.second_remote = None

        energy_scan.result.pk = pk
        energy_scan.result.fppPeak = fppPeak
        energy_scan.result.fpPeak = fpPeak
        energy_scan.result.ip = ip
        energy_scan.result.fppInfl = fppInfl
        energy_scan.result.fpInfl = fpInfl
        energy_scan.result.rm = rm
        energy_scan.result.chooch_graph_x = chooch_graph_x
        energy_scan.result.chooch_graph_y1 = chooch_graph_y1
        energy_scan.result.chooch_graph_y2 = chooch_graph_y2
        energy_scan.result.title = title
        try:
            energy_scan.result.data = self.energy_scan_hwobj.get_scan_data()
        except BaseException:
            pass

        if (
            sample.crystals[0].energy_scan_result.peak
            and sample.crystals[0].energy_scan_result.inflection
        ):
            logging.getLogger("user_level_log").info(
                "Energy scan: Result peak: %.4f, inflection: %.4f"
                % (
                    sample.crystals[0].energy_scan_result.peak,
                    sample.crystals[0].energy_scan_result.inflection,
                )
            )

        self.get_view().setText(1, "Done")
        self._queue_controller.emit("energy_scan_finished", (pk, ip, rm, sample))

    def energy_scan_failed(self):
        self._failed = True
        self.get_view().setText(1, "Failed")
        self.status = QUEUE_ENTRY_STATUS.FAILED
        logging.getLogger("user_level_log").error("Energy scan: failed")
        raise QueueExecutionException("Energy scan failed", self)

    def stop(self):
        BaseQueueEntry.stop(self)

        try:
            # self.get_view().setText(1, 'Stopping ...')
            self.energy_scan_hwobj.cancelEnergyScan()

            if self.centring_task:
                self.centring_task.kill(block=False)
        except gevent.GreenletExit:
            raise

        self.get_view().setText(1, "Stopped")
        logging.getLogger("queue_exec").info("Calling stop on: " + str(self))
        # this is to work around the remote access problem
        dispatcher.send("collect_finished")
        raise QueueAbortedException("Queue stopped", self)

    def get_type_str(self):
        return "Energy scan"


class XRFSpectrumQueueEntry(BaseQueueEntry):
    def __init__(self, view=None, data_model=None):
        BaseQueueEntry.__init__(self, view, data_model)
        self.xrf_spectrum_hwobj = None
        self.session_hwobj = None
        self._failed = False

    def __getstate__(self):
        d = dict(self.__dict__)
        d["xrf_spectrum_task"] = None
        return d

    def __setstate__(self, d):
        self.__dict__.update(d)

    def execute(self):
        BaseQueueEntry.execute(self)

        if self.xrf_spectrum_hwobj is not None:
            xrf_spectrum = self.get_data_model()
            self.get_view().setText(1, "Starting xrf spectrum")

            sample_model = self.get_data_model().get_sample_node()
            node_id = xrf_spectrum._node_id

            sample_lims_id = sample_model.lims_id
            # No sample id, pass None to startEnergySpectrum
            if sample_lims_id == -1:
                sample_lims_id = None

            self.xrf_spectrum_hwobj.startXrfSpectrum(
                xrf_spectrum.count_time,
                xrf_spectrum.path_template.directory,
                xrf_spectrum.path_template.get_archive_directory(),
                "%s_%d"
                % (
                    xrf_spectrum.path_template.get_prefix(),
                    xrf_spectrum.path_template.run_number,
                ),
                self.session_hwobj.session_id,
                node_id,
            )
            self.xrf_spectrum_hwobj.ready_event.wait()
            self.xrf_spectrum_hwobj.ready_event.clear()
        else:
            logging.getLogger("user_level_log").info(
                "XRFSpectrum not defined in beamline setup"
            )
            self.xrf_spectrum_failed()

    def pre_execute(self):
        BaseQueueEntry.pre_execute(self)
        self._failed = False
        self.xrf_spectrum_hwobj = self.beamline_setup.xrf_spectrum_hwobj
        self.session_hwobj = self.beamline_setup.session_hwobj
        qc = self.get_queue_controller()
        qc.connect(
            self.xrf_spectrum_hwobj,
            "xrfSpectrumStatusChanged",
            self.xrf_spectrum_status_changed,
        )

        qc.connect(
            self.xrf_spectrum_hwobj, "xrfSpectrumStarted", self.xrf_spectrum_started
        )
        qc.connect(
            self.xrf_spectrum_hwobj, "xrfSpectrumFinished", self.xrf_spectrum_finished
        )
        qc.connect(
            self.xrf_spectrum_hwobj, "xrfSpectrumFailed", self.xrf_spectrum_failed
        )

    def post_execute(self):
        BaseQueueEntry.post_execute(self)
        qc = self.get_queue_controller()
        qc.disconnect(
            self.xrf_spectrum_hwobj,
            "xrfSpectrumStatusChanged",
            self.xrf_spectrum_status_changed,
        )

        qc.disconnect(
            self.xrf_spectrum_hwobj, "xrfSpectrumStarted", self.xrf_spectrum_started
        )

        qc.disconnect(
            self.xrf_spectrum_hwobj, "xrfSpectrumFinished", self.xrf_spectrum_finished
        )

        qc.disconnect(
            self.xrf_spectrum_hwobj, "xrfSpectrumFailed", self.xrf_spectrum_failed
        )
        if self._failed:
            raise QueueAbortedException("Queue stopped", self)
        self.get_view().set_checkable(False)

    def xrf_spectrum_status_changed(self, msg):
        logging.getLogger("user_level_log").info(msg)

    def xrf_spectrum_started(self):
        logging.getLogger("user_level_log").info("XRF spectrum started.")
        self.get_view().setText(1, "In progress")

    def xrf_spectrum_finished(self, mcaData, mcaCalib, mcaConfig):
        xrf_spectrum = self.get_data_model()
        spectrum_file_path = os.path.join(
            xrf_spectrum.path_template.directory,
            xrf_spectrum.path_template.get_prefix(),
        )
        spectrum_file_archive_path = os.path.join(
            xrf_spectrum.path_template.get_archive_directory(),
            xrf_spectrum.path_template.get_prefix(),
        )

        xrf_spectrum.result.mca_data = mcaData
        xrf_spectrum.result.mca_calib = mcaCalib
        xrf_spectrum.result.mca_config = mcaConfig

        logging.getLogger("user_level_log").info("XRF spectrum finished.")
        self.get_view().setText(1, "Done")

    def xrf_spectrum_failed(self):
        self._failed = True
        self.get_view().setText(1, "Failed")
        self.status = QUEUE_ENTRY_STATUS.FAILED
        logging.getLogger("user_level_log").error("XRF spectrum failed.")
        raise QueueExecutionException("XRF spectrum failed", self)

    def get_type_str(self):
        return "XRF spectrum"

class GenericWorkflowQueueEntry(BaseQueueEntry):
    def __init__(self, view=None, data_model=None):
        BaseQueueEntry.__init__(self, view, data_model)
        self.rpc_server_hwobj = None
        self.workflow_hwobj = None
        self.workflow_running = False
        self.workflow_started = False

    def execute(self):
        BaseQueueEntry.execute(self)

        # Start execution of a new workflow
        if str(self.workflow_hwobj.state.value) != "ON":
            # We are trying to start a new workflow and the Tango server is not idle,
            # therefore first abort any running workflow:
            self.workflow_hwobj.abort()
            if self.workflow_hwobj.command_failure():
                msg = (
                    "Workflow abort command failed! Please check workflow Tango server."
                )
                logging.getLogger("user_level_log").error(msg)
            else:
                # Then sleep three seconds for allowing the server to abort a running
                # workflow:
                time.sleep(3)
                # If the Tango server has been restarted the state.value is None.
                # If not wait till the state.value is "ON":
                if self.workflow_hwobj.state.value is not None:
                    while str(self.workflow_hwobj.state.value) != "ON":
                        time.sleep(0.5)

        msg = "Starting workflow (%s), please wait." % (self.get_data_model()._type)
        logging.getLogger("user_level_log").info(msg)
        workflow_params = self.get_data_model().params_list
        # Add the current node id to workflow parameters
        # group_node_id = self._parent_container._data_model._node_id
        # workflow_params.append("group_node_id")
        # workflow_params.append("%d" % group_node_id)
        self.workflow_hwobj.start(workflow_params)
        if self.workflow_hwobj.command_failure():
            msg = "Workflow start command failed! Please check workflow Tango server."
            logging.getLogger("user_level_log").error(msg)
            self.workflow_running = False
        else:
            self.workflow_running = True
            while self.workflow_running:
                time.sleep(1)

    def workflow_state_handler(self, state):
        if isinstance(state, tuple):
            state = str(state[0])
        else:
            state = str(state)

        if state == "ON":
            self.workflow_running = False
        elif state == "RUNNING":
            self.workflow_started = True
        elif state == "OPEN":
            msg = "Workflow waiting for input, verify parameters and press continue."
            logging.getLogger("user_level_log").warning(msg)
            self.get_queue_controller().show_workflow_tab()

    def pre_execute(self):
        BaseQueueEntry.pre_execute(self)
        qc = self.get_queue_controller()
        self.workflow_hwobj = self.beamline_setup.workflow_hwobj

        qc.connect(self.workflow_hwobj, "stateChanged", self.workflow_state_handler)

    def post_execute(self):
        BaseQueueEntry.post_execute(self)
        qc = self.get_queue_controller()
        qc.disconnect(self.workflow_hwobj, "stateChanged", self.workflow_state_handler)
        # reset state
        self.workflow_started = False
        self.workflow_running = False

        self.get_data_model().set_executed(True)
        self.get_data_model().set_enabled(False)

    def stop(self):
        BaseQueueEntry.stop(self)
        self.workflow_hwobj.abort()
        self.get_view().setText(1, "Stopped")
        raise QueueAbortedException("Queue stopped", self)


class XrayCenteringQueueEntry(BaseQueueEntry):
    """
    Defines the behaviour of an Advanced scan
    """

    def __init__(self, view=None, data_model=None, view_set_queue_entry=True):

        BaseQueueEntry.__init__(self, view, data_model, view_set_queue_entry)
        self.mesh_qe = None
        self.helical_qe = None
        self.in_queue = False

    def execute(self):
        BaseQueueEntry.execute(self)

    def pre_execute(self):
        BaseQueueEntry.pre_execute(self)
        xray_centering = self.get_data_model()
        reference_image_collection = xray_centering.reference_image_collection
        reference_image_collection.grid = self.beamline_setup.shape_history_hwobj.create_auto_grid()
        reference_image_collection.acquisitions[0].acquisition_parameters.centred_position = \
            reference_image_collection.grid.get_centred_position()

        # Trick to make sure that the reference collection has a sample.
        reference_image_collection._parent = xray_centering.get_parent()
        xray_centering.line_collection._parent = xray_centering.get_parent()

        gid = self.get_data_model().get_parent().lims_group_id
        reference_image_collection.lims_group_id = gid

        # Enqueue the reference mesh scan collection
        mesh_qe = DataCollectionQueueEntry(
            self.get_view(), reference_image_collection, view_set_queue_entry=False
        )
        mesh_qe.set_enabled(True)
        mesh_qe.in_queue = self.in_queue
        self.mesh_qe = mesh_qe

        # Creat e a helical data collection based on the first collection
        helical_qe = DataCollectionQueueEntry(
            self.get_view(), reference_image_collection, view_set_queue_entry=False
        )

        #helical_model = helical_qe.get_data_model()
        #@helical_model.set_experiment_type(EXPERIMENT_TYPE.HELICAL)
        #@helical_model.grid = None

        acq_two = queue_model_objects.Acquisition()
        helical_model.acquisitions.append(acq_two)
        helical_model.acquisitions[0].acquisition_parameters.num_images = 100
        helical_model.acquisitions[0].acquisition_parameters.num_lines = 1
        helical_acq_path_template = helical_model.acquisitions[0].path_template
        helical_acq_path_template.base_prefix = (
            "line_" + helical_acq_path_template.base_prefix
        )
        helical_qe._data_model = helical_model

        helical_qe.set_enabled(True)
        helical_qe.in_queue = self.in_queue
        self.helical_qe = helical_qe

        advanced_connector_qe = AdvancedConnectorQueueEntry(
            self.get_view(), reference_image_collection, view_set_queue_entry=False
        )
        advanced_connector_qe.first_qe = mesh_qe
        advanced_connector_qe.second_qe = helical_qe
        advanced_connector_qe.set_enabled(True)

        self.enqueue(mesh_qe)
        self.enqueue(advanced_connector_qe)
        self.enqueue(helical_qe)

    def post_execute(self):
        if self.helical_qe:
            self.status = self.helical_qe.status
        else:
            self.status = self.mesh_qe
        BaseQueueEntry.post_execute(self)


class AdvancedConnectorQueueEntry(BaseQueueEntry):
    """Controls different steps
    """

    def __init__(self, view=None, data_model=None, view_set_queue_entry=True):

        BaseQueueEntry.__init__(self, view, data_model, view_set_queue_entry)
        self.first_qe = None
        self.second_qe = None
        self.diffractometer_hwobj = None
        self.shape_history_hwobj = None

    def pre_execute(self):
        BaseQueueEntry.pre_execute(self)
        self.diffractometer_hwobj = self.beamline_setup.diffractometer_hwobj
        self.shape_history_hwobj = self.beamline_setup.shape_history_hwobj

    def execute(self):
        BaseQueueEntry.execute(self)
        firt_qe_data_model = self.first_qe.get_data_model()

        if firt_qe_data_model.run_processing_parallel == "XrayCentering":
            best_positions = firt_qe_data_model.parallel_processing_result.get(
                "best_positions", []
            )

            if len(best_positions) > 0:
                best_cpos = best_positions[0]["cpos"]
                helical_model = self.second_qe.get_data_model()

                # logging.getLogger("user_level_log").info(\
                #    "Moving to the best position")
                # self.diffractometer_hwobj.move_motors(best_cpos)
                # gevent.sleep(2)

                logging.getLogger("user_level_log").info("Rotating 90 degrees")
                self.diffractometer_hwobj.move_omega_relative(90)
                logging.getLogger("user_level_log").info("Creating a helical line")

                gevent.sleep(2)
                auto_line, cpos_one, cpos_two = (
                    self.shape_history_hwobj.create_auto_line()
                )
                helical_model.acquisitions[
                    0
                ].acquisition_parameters.osc_start = cpos_one.phi
                helical_model.acquisitions[
                    0
                ].acquisition_parameters.centred_position = cpos_one
                helical_model.acquisitions[
                    1
                ].acquisition_parameters.centred_position = cpos_two

                self.second_qe.set_enabled(True)
            else:
                logging.getLogger("user_level_log").warning(
                    "No diffraction found. Cancelling Xray centering"
                )
                self.second_qe.set_enabled(False)


class OpticalCentringQueueEntry(BaseQueueEntry):
    """
    Entry for automatic sample centring with lucid
    """

    def __init__(self, view=None, data_model=None):
        BaseQueueEntry.__init__(self, view, data_model)
        self.diffractometer_hwobj = None

    def execute(self):
        BaseQueueEntry.execute(self)
        self.diffractometer_hwobj.automatic_centring_try_count = (
            self.get_data_model().try_count
        )

        self.diffractometer_hwobj.start_centring_method(
            self.diffractometer_hwobj.CENTRING_METHOD_AUTO, wait=True
        )

    def pre_execute(self):
        BaseQueueEntry.pre_execute(self)
        self.diffractometer_hwobj = self.beamline_setup.diffractometer_hwobj

    def post_execute(self):
        self.get_view().set_checkable(False)
        BaseQueueEntry.post_execute(self)

    def get_type_str(self):
        return "Optical automatic centering"


class XrayImagingQueueEntry(BaseQueueEntry):
    """
    """

    def __init__(self, view=None, data_model=None, view_set_queue_entry=True):
        BaseQueueEntry.__init__(self, view, data_model, view_set_queue_entry)

    def execute(self):
        BaseQueueEntry.execute(self)
        self.beamline_setup.xray_imaging_hwobj.execute(self.get_data_model())

    def pre_execute(self):
        BaseQueueEntry.pre_execute(self)

        qc = self.get_queue_controller()
        qc.connect(self.beamline_setup.xray_imaging_hwobj, "collectImageTaken", self.image_taken)
        qc.connect(self.beamline_setup.xray_imaging_hwobj, "collectOscillationFailed", self.collect_failed)

        data_model = self.get_data_model()

        if data_model.get_parent():
            gid = data_model.get_parent().lims_group_id
            data_model.lims_group_id = gid

        self.beamline_setup.xray_imaging_hwobj.pre_execute(self.get_data_model())

    def post_execute(self):
        BaseQueueEntry.post_execute(self)
        self.beamline_setup.xray_imaging_hwobj.post_execute(self.get_data_model())

        qc = self.get_queue_controller()
        qc.disconnect(self.beamline_setup.xray_imaging_hwobj, "collectImageTaken", self.image_taken)
        qc.disconnect(self.beamline_setup.xray_imaging_hwobj, "collectOscillationFailed", self.collect_failed)

    def stop(self):
        BaseQueueEntry.stop(self)
        self.beamline_setup.xray_imaging_hwobj.stop_collect()

    def collect_failed(self, owner, state, message, *args):
        # this is to work around the remote access problem
        dispatcher.send("collect_finished")
        self.get_view().setText(1, "Failed")
        self.status = QUEUE_ENTRY_STATUS.FAILED
        logging.getLogger("queue_exec").error(message.replace("\n", " "))
        raise QueueExecutionException(message.replace("\n", " "), self)

    def image_taken(self, image_number):
        if image_number > 0:
            num_images = (
                self.get_data_model().acquisitions[0].acquisition_parameters.num_images
            )
            self.get_view().setText(1, str(image_number) + "/" + str(num_images))


def mount_sample(
    beamline_setup_hwobj, view, data_model, centring_done_cb, async_result
):
    view.setText(1, "Loading sample")
    beamline_setup_hwobj.shape_history_hwobj.clear_all()
    log = logging.getLogger("queue_exec")
    loc = data_model.location
    holder_length = data_model.holder_length

    snapshot_before_filename = "/tmp/test_before.png"
    snapshot_after_filename = "/tmp/test_after.png"

    robot_action_dict = {
        "actionType": "LOAD",
        "containerLocation": loc[1],
        "dewarLocation": loc[0],
        "sampleBarcode": data_model.code,
        "sampleId": data_model.lims_id,
        "sessionId": beamline_setup_hwobj.session_hwobj.session_id,
        "startTime": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    # "xtalSnapshotBefore": data_model.get_snapshot_filename(prefix="before"),
    # "xtalSnapshotAfter": data_model.get_snapshot_filename(prefix="after")}

    # This is a possible solution how to deal with two devices that
    # can move sample on beam (sample changer, plate holder, in future
    # also harvester)
    # TODO make sample_Changer_one, sample_changer_two
    if beamline_setup_hwobj.diffractometer_hwobj.in_plate_mode():
        sample_mount_device = beamline_setup_hwobj.plate_manipulator_hwobj
    else:
        sample_mount_device = beamline_setup_hwobj.sample_changer_hwobj

    if hasattr(sample_mount_device, "__TYPE__"):
        if sample_mount_device.__TYPE__ in ["Marvin", "CATS"]:
            element = "%d:%02d" % tuple(loc)
            sample_mount_device.load(sample=element, wait=True)
        elif sample_mount_device.__TYPE__ == "PlateManipulator":
            sample_mount_device.load_sample(sample_location=loc)
        else:
            if (
                sample_mount_device.load_sample(
                    holder_length, sample_location=loc, wait=True
                )
                is False
            ):
                # WARNING: explicit test of False return value.
                # This is to preserve backward compatibility (load_sample was supposed to return None);
                # if sample could not be loaded, but no exception is raised, let's skip
                # the sample
                raise QueueSkippEntryException(
                    "Sample changer could not load sample", ""
                )

    robot_action_dict["endTime"] = time.strftime("%Y-%m-%d %H:%M:%S")
    if sample_mount_device.hasLoadedSample():
        robot_action_dict["status"] = "SUCCESS"
    else:
        robot_action_dict["message"] = "Sample was not loaded"
        robot_action_dict["status"] = "ERROR"

    beamline_setup_hwobj.lims_client_hwobj.store_robot_action(robot_action_dict)

    if not sample_mount_device.hasLoadedSample():
        # Disables all related collections
        view.setOn(False)
        view.setText(1, "Sample not loaded")
        raise QueueSkippEntryException("Sample not loaded", "")
    else:
        view.setText(1, "Sample loaded")
        dm = beamline_setup_hwobj.diffractometer_hwobj
        if dm is not None:
            if hasattr(sample_mount_device, "__TYPE__"):
                if sample_mount_device.__TYPE__ in (
                    "Marvin",
                    "PlateManipulator",
                    "Mockup",
                ):
                    return
            try:
                dm.connect("centringAccepted", centring_done_cb)
                centring_method = view.listView().parent().parent().centring_method
                if centring_method == CENTRING_METHOD.MANUAL:
                    log.warning(
                        "Manual centring used, waiting for" + " user to center sample"
                    )
                    dm.start_centring_method(dm.MANUAL3CLICK_MODE)
                elif centring_method == CENTRING_METHOD.LOOP:
                    dm.start_centring_method(dm.C3D_MODE)
                    log.warning(
                        "Centring in progress. Please save"
                        + " the suggested centring or re-center"
                    )
                elif centring_method == CENTRING_METHOD.FULLY_AUTOMATIC:
                    log.info("Centring sample, please wait.")
                    dm.start_centring_method(dm.C3D_MODE)
                else:
                    dm.start_centring_method(dm.MANUAL3CLICK_MODE)

                view.setText(1, "Centring !")
                centring_result = async_result.get()
                if centring_result["valid"]:
                    view.setText(1, "Centring done !")
                    log.info("Centring saved")
                else:
                    view.setText(1, "Centring failed !")
                    if centring_method == CENTRING_METHOD.FULLY_AUTOMATIC:
                        raise QueueSkippEntryException(
                            "Could not center sample, skipping", ""
                        )
                    else:
                        raise RuntimeError("Could not center sample")
            except Exception as ex:
                log.exception("Could not center sample: " + str(ex))
            finally:
                dm.disconnect("centringAccepted", centring_done_cb)


def center_before_collect(view, dm, queue, shapes):
    view.setText(1, "Waiting for input")
    log = logging.getLogger("user_level_log")

    log.info("Please select, or center on a new position and press continue.")

    queue.pause(True)
    pos, shape = None, None

    if len(shapes.get_selected_shapes()):
        shape = shapes.get_selected_shapes()[0]
        pos = shape.mpos()
    else:
        msg = "No centred position selected, using current position."
        log.info(msg)

        # Create a centred postions of the current postion
        pos = dm.getPositions()
        shape = shapes.add_shape_from_mpos([pos], (0, 0), "P")

    view(1, "Centring completed")
    log.info("Centring completed")

    return queue_model_objects.CentredPosition(pos), shape


MODEL_QUEUE_ENTRY_MAPPINGS = {
    queue_model_objects.DataCollection: DataCollectionQueueEntry,
    queue_model_objects.Characterisation: CharacterisationGroupQueueEntry,
    queue_model_objects.EnergyScan: EnergyScanQueueEntry,
    queue_model_objects.XRFSpectrum: XRFSpectrumQueueEntry,
    queue_model_objects.SampleCentring: SampleCentringQueueEntry,
    queue_model_objects.OpticalCentring: OpticalCentringQueueEntry,
    queue_model_objects.Sample: SampleQueueEntry,
    queue_model_objects.Basket: BasketQueueEntry,
    queue_model_objects.TaskGroup: TaskGroupQueueEntry,
    queue_model_objects.Workflow: GenericWorkflowQueueEntry,
    queue_model_objects.XrayCentering: XrayCenteringQueueEntry,
    queue_model_objects.GphlWorkflow: GphlQueueEntry.GphlWorkflowQueueEntry,
    queue_model_objects.XrayImaging: XrayImagingQueueEntry,
}
