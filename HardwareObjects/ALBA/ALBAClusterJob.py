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
[Name] ALBAClusterJob

[Description]
HwObj (module) used to define different pipeline objects running on the ALBA cluster.

With the arguments passed, we need to first dump a yaml file with the configuration and
then, reload this file to create the new EDNAJob object.

In the utils module, a method is supplied to create a yaml file

In addition, we need to create
a manager object to initialize the account and provide the interface to the cluster CLA.

[Signals]
- None
"""

from __future__ import print_function

import os
import time
import logging

from ALBAClusterClient import EDNAJob, Manager, Account
from ALBAClusterClient.utils import create_edna_yml

# from XSDataMXCuBEv1_3 import XSDataResultMXCuBE

__credits__ = ["ALBA Synchrotron"]
__version__ = "2.3"
__category__ = "General"

root = os.environ['POST_PROCESSING_SCRIPTS_ROOT']

# TODO: Maybe better inherit from EDNAJob and define a HW class for manager?
class ALBAClusterJob(object):

    def __init__(self, *args):
        self.account = Account()
        self.manager = Manager([self.account])
        self.job = None
        self._yml_file = None

    def run(self):
        self.job = EDNAJob(self._yml_file)
        self.manager.submit(self.job)

    def wait_done(self, wait=True):

        state = self.manager.get_job_state(self.job)
        logging.getLogger("HWR").debug("Job state is %s" % state)

        while state in ["RUNNING", "PENDING"]:
            logging.getLogger("HWR").debug("Job / is %s" % state)
            time.sleep(0.5)
            state = self.manager.get_job_state(self.job)

        logging.getLogger("HWR").debug(" job finished with state: \"%s\"" % state)
        return state


class ALBAAutoProcJob(ALBAClusterJob):

    plugin_name = 'EDPluginControlAutoPROCv1_0'
    slurm_script = os.path.join(root, 'edna-mx/autoproc/mxcube/edna-mx.autoproc.sl')
    configdef = os.path.join(root, 'edna-mx/autoproc/benchmark/config.def')

    def __init__(self, *args):
        super(ALBAAutoProcJob, self).__init__(*args)
        collect_id, input_file, output_dir = args

        self._yml_file = create_edna_yml(str(collect_id),
                                         self.plugin_name,
                                         input_file,
                                         self.slurm_script,
                                         workarea='SCRATCH',
                                         benchmark=False,
                                         dest=output_dir,
                                         use_scripts_root=True,
                                         xds=None,
                                         configdef=self.configdef)


class ALBAEdnaProcJob(ALBAClusterJob):
    plugin_name = 'EDPluginControlEDNAprocv1_0'
    slurm_script = os.path.join(root, 'edna-mx/ednaproc/mxcube/edna-mx.ednaproc.sl')

    def __init__(self, *args):
        super(ALBAEdnaProcJob, self).__init__(*args)
        collect_id, input_file, output_dir = args

        self._yml_file = create_edna_yml(str(collect_id),
                                         self.plugin_name,
                                         input_file,
                                         self.slurm_script,
                                         workarea='SCRATCH',
                                         benchmark=False,
                                         dest=output_dir,
                                         use_scripts_root=True,
                                         xds=None,
                                         configdef=None)


class ALBAStrategyJob(ALBAClusterJob):
    plugin_name = 'EDPluginControlInterfaceToMXCuBEv1_3'
    slurm_script = os.path.join(root, 'edna-mx/strategy/mxcube/edna-mx.strategy.sl')

    def __init__(self, *args):
        super(ALBAStrategyJob, self).__init__(*args)
        collect_id, input_file, output_dir = args
        logging.getLogger('HWR').debug(collect_id) 
        self._yml_file = create_edna_yml(str(collect_id),
                                         self.plugin_name,
                                         input_file,
                                         self.slurm_script,
                                         workarea='SCRATCH',
                                         benchmark=False,
                                         dest=output_dir,
                                         use_scripts_root=True,
                                         xds=None,
                                         configdef=None)
        logging.getLogger('HWR').debug('End ALBAStrategyJob init')
        time.sleep(3)

