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
[Name] ALBACluster

[Description]
HwObj providing access to the ALBA cluster.

[Signals]
- None
"""

from __future__ import print_function

import os
import time
import logging

from HardwareRepository.BaseHardwareObjects import HardwareObject
from ALBAClusterClient import EDNAJob, Manager, Account
from ALBAClusterClient.utils import create_edna_yml


__credits__ = ["ALBA Synchrotron"]
__version__ = "2.3"
__category__ = "General"
__author__ = "Jordi Andreu"


class ALBACluster(HardwareObject):
    def __init__(self, name):
        HardwareObject.__init__(self, name)
        self.logger = logging.getLogger("HWR.ALBACluster")
        self.account = None
        self.manager = None
        self.use_env_scripts_root = True
        self._scripts_root = None
        self.pipelines = {}
        self._jobs = None

    def init(self):
        self.logger.debug("Initializing {0}".format(self.__class__.__name__))
        user = self.getProperty("user")
        cla = self.getProperty("cla")

        self._scripts_root = self.getProperty("pipelines_scripts_root")

        if self._scripts_root:
            self.use_env_scripts_root = False

        for name in ['strategy', 'ednaproc', 'autoproc']:
            if self.getProperty("{}_pipeline".format(name)):
                _pipeline = eval(self.getProperty("{}_pipeline".format(name)))
                if _pipeline:
                    self.pipelines[name] = _pipeline
                    self.logger.debug("Adding {0} pipeline {1}".format(name, _pipeline))

        self.account = Account(user=user, cla=cla, scripts_root=None)
        self.manager = Manager([self.account])
        self.logger.debug("cluster user: {0}".format(self.account.user))
        self.logger.debug("cluster CLA: {0}".format(self.account.cla))

    def run(self, job):
        self.manager.submit(job)

    def wait_done(self, job):
        state = self.manager.get_job_state(job)
        logging.getLogger("HWR").debug("Job state is %s" % state)

        while state in ["RUNNING", "PENDING"]:
            logging.getLogger("HWR").debug("Job / is %s" % state)
            time.sleep(0.5)
            state = self.manager.get_job_state(job)

        logging.getLogger("HWR").debug(" job finished with state: \"%s\"" % state)
        return state

    def create_strategy_job(self, collect_id, input_file, output_dir):

        plugin_name = self.pipelines['strategy']['plugin']
        slurm_script = os.path.join(self._sripts_root,
                                    self.pipelines['strategy']['script'])

        _yml_file = create_edna_yml(str(collect_id),
                                    plugin_name,
                                    input_file,
                                    slurm_script,
                                    workarea='SCRATCH',
                                    benchmark=False,
                                    dest=output_dir,
                                    use_scripts_root=self.use_env_scripts_root,
                                    xds=None,
                                    configdef=None)
        return EDNAJob(_yml_file)

    def create_autoproc_job(self, collect_id, input_file, output_dir):

        plugin_name = self.pipelines['autoproc']['plugin']
        slurm_script = os.path.join(self._sripts_root,
                                    self.pipelines['autoproc']['script'])
        configdef = os.path.join(self._sripts_root,
                                 self.pipelines['autoproc']['configdef'])

        _yml_file = create_edna_yml(str(collect_id),
                                    plugin_name,
                                    input_file,
                                    slurm_script,
                                    workarea='SCRATCH',
                                    benchmark=False,
                                    dest=output_dir,
                                    use_scripts_root=self.use_env_scripts_root,
                                    xds=None,
                                    configdef=configdef)
        return EDNAJob(_yml_file)

    def create_ednaproc_job(self, collect_id, input_file, output_dir):

        plugin_name = self.pipelines['ednaproc']['plugin']
        slurm_script = os.path.join(self._sripts_root,
                                    self.pipelines['ednaproc']['script'])

        _yml_file = create_edna_yml(str(collect_id),
                                    plugin_name,
                                    input_file,
                                    slurm_script,
                                    workarea='SCRATCH',
                                    benchmark=False,
                                    dest=output_dir,
                                    use_scripts_root=self.use_env_scripts_root,
                                    xds=None,
                                    configdef=None)
        return EDNAJob(_yml_file)
