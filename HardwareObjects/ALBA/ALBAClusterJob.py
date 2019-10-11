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


class ALBAClusterJob(object):

    def __init__(self, *args):
        self.account = Account('opbl13', 'opbl13','claxaloc01')
        self.manager = Manager([self.account])
        self.job = None
        self._yml_file = None

    def run(self):
        self.job = EDNAJob(self._yml_file)
        self.manager.submit(self.job)

    def wait_done(self, wait=True):

        if not self.job:
            return

        time.sleep(0.5)

        state = self.manager.get_job_state(self.job)

        if not wait:
            return state

        while state in ["RUNNING", "PENDING"]:
            logging.getLogger("HWR").debug("Job / is %s" % state)
            time.sleep(0.5)
            state = self.manager.get_job_state(self.job)

        logging.getLogger("HWR").debug(" job finished with state: \"%s\"" % state)
        return state

    # def get_result(self, state):
    #     pass


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
    xdsinp = os.path.join(root, 'edna-mx/ednaproc/benchmark/XDS.INP')

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
                                         xds=self.xdsinp,
                                         configdef=None)


class ALBAStrategyJob(ALBAClusterJob):
    plugin_name = 'EDPluginControlInterfaceToMXCuBEv1_3'
    slurm_script = os.path.join(root, 'edna-mx/strategy/mxcube/edna-mx.strategy.sl')

    def __init__(self, *args):
        super(ALBAStrategyJob, self).__init__(*args)
        collect_id, input_file, output_dir = args

        self._yml_file = create_edna_yml(str(collect_id),
                                         self.plugin_name,
                                         input_file,
                                         self.slurm_script,
                                         workarea='USER',
                                         benchmark=False,
                                         dest=output_dir,
                                         use_scripts_root=True,
                                         xds=None,
                                         configdef=None)

    # def run(self, *args):
    #
    #     logging.getLogger("HWR").debug("Starting StrategyJob - ")
    #
    #     input_file, results_file, edna_directory = args
    #
    #     job_name = os.path.basename(os.path.dirname(edna_directory))
    #
    #     self.job = EDNAJob(
    #         "edna-strategy",
    #         job_name,
    #         self.sls_script,
    #         input_file,
    #         edna_directory,
    #         'SCRATCH')
    #     self.job.submit()
    #
    #     logging.getLogger("HWR").debug("         StrategyJob - %s" % str(self.job))
    #
    # def get_result(self, state):
    #     if state == "COMPLETED":
    #         outfile = os.path.join(
    #             self.edna_directory,
    #             "ControlInterfaceToMXCuBEv1_3_dataOutput.xml")
    #
    #         logging.getLogger("HWR").debug("Job / state is COMPLETED")
    #         logging.getLogger("HWR").debug("  looking for file: %s" % outfile)
    #         if os.path.exists(outfile):
    #             job_output = open(outfile).read()
    #             open(self.results_file, "w").write(job_output)
    #             result = XSDataResultMXCuBE.parseFile(self.results_file)
    #         else:
    #             logging.getLogger("HWR").debug(
    #                 "EDNA Job finished without success / cannot find output file ")
    #             result = ""
    #     else:
    #         logging.getLogger("HWR").debug(
    #             "EDNA Job finished without success / state was %s" % self.job.state)
    #         result = ""
    #
    #     return result


# def test():
#
#     collect_id = 432
#     input_file = '/beamlines/bl13/projects/cycle2018-I/2018002222-ispybtest/20190218/' \
#                  'PROCESS_DATA/test_processing/ednaproc_mx2018002222_4_1/' \
#                  'EDNAprocInput_432.xml'
#     output_dir = '/beamlines/bl13/projects/cycle2018-I/2018002222-ispybtest/20190218/' \
#                  'PROCESS_DATA/test_processing/ednaproc_mx2018002222_4_1'
#     ALBAEdnaProcJob().run(collect_id, input_file, output_dir)


# if __name__ == "__main__":
#     #test()
#     ALBAAutoProcJob(1,
#                     '/home/jandreu/Development/BL13-XALOC/bl13_processing/edna-mx/autoproc/benchmark/XSDataInputAutoPROC.xml',
#                     '/home/jandreu/Development/BL13-XALOC/bl13_processing/')

