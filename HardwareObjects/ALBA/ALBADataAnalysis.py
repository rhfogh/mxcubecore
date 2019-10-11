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
[Name] ALBADataAnalysis

[Description]
Prepare files and launch strategy pipeline (online) to the ALBA cluster

[Signals]
- None
"""

from __future__ import print_function

import os
import time
import logging

from DataAnalysis import DataAnalysis
from XSDataMXCuBEv1_3 import XSDataResultMXCuBE
from XSDataCommon import XSDataFile, XSDataString
#from ALBAClusterClient import XalocJob
from ALBAClusterJob import ALBAStrategyJob

__credits__ = ["ALBA Synchrotron"]
__version__ = "2.3"
__category__ = "General"

root = os.environ['POST_PROCESSING_SCRIPTS_ROOT']
sls_script = os.path.join(root, 'edna-mx/strategy/mxcube/edna-mx.strategy.sl')


class ALBADataAnalysis(DataAnalysis):

    def __init__(self, name):
        DataAnalysis.__init__(self, name)
        self.logger = logging.getLogger("HWR.ALBADataAnalysis")
        self.job = None
        self.edna_directory = None
        self.input_file = None
        self.results_file = None

    def init(self):
        self.logger.debug("Initializing {0}".format(self.__class__.__name__))
        DataAnalysis.init(self)

    def prepare_edna_input(self, edna_input, edna_directory):

        # used for strategy calculation (characterization) using data analysis cluster
        # ALBA specific
        edna_input.process_directory = edna_directory

        output_dir = XSDataFile()
        path = XSDataString()
        path.setValue(edna_directory)
        output_dir.setPath(path)
        edna_input.setOutputFileDirectory(output_dir)

    def run_edna(self, dc_id, input_file, results_file, edna_directory):
        return self.run(dc_id, input_file, results_file, edna_directory)

    def run(self, *args):
        dc_id, input_file, results_file, edna_directory = args

        jobname = os.path.basename(os.path.dirname(edna_directory))

        self.logger.debug("Submitting Job")
        self.logger.debug(" job_name: %s" % jobname)
        self.logger.debug(" sls_script: %s, " % sls_script)
        self.logger.debug(" input file: %s" % input_file)
        self.logger.debug(" results file: %s" % results_file)
        self.logger.debug(" edna directory: %s" % edna_directory)

        # self.job = XalocJob(
        #     "edna-strategy",
        #     jobname,
        #     sls_script,
        #     input_file,
        #     edna_directory,
        #     'USER')
        # self.job.submit()

        job = ALBAStrategyJob(dc_id, input_file, edna_directory)
        job.run()

        self.logger.debug("Job submitted %s" % self.job.id)

        self.edna_directory = os.path.dirname(input_file)
        self.input_file = os.path.basename(input_file)
        # self.results_file = self.fix_path(results_file)
        self.results_file = results_file
        self.logger.debug("Results file: %s" % self.results_file)

        state = self.wait_done()

        if state == "COMPLETED":
            self.logger.debug("Job completed")
            time.sleep(0.5)
            result = self.get_result()
        else:
            self.logger.debug("Job finished without success / state was %s" %
                              self.job.state)
            result = ""

        return result

    # TODO: Deprecated
    def fix_path(self, path):
        out_path = path.replace('PROCESS_DATA', 'PROCESS_DATA/RESULTS')
        # dirname = os.path.dirname(path)
        # basename = os.path.basename(path)
        # outpath = os.path.join(dirname,'RESULTS',basename)
        return out_path

    def wait_done(self):

        state = None
        time.sleep(0.5)
        self.logger.debug("Polling for Job state")

        try:
            state = self.job.state
            self.logger.debug("Job / is %s" % str(state))
        except Exception as e:
            self.logger.debug(
                "Polling for Job state, exception happened\n%s" % str(e))

        while state in ["RUNNING", "PENDING"]:
            self.logger.debug("Job / is %s" % state)
            time.sleep(0.5)
            state = self.job.state

        self.logger.debug("Returning %s" % str(state))
        return state

    def get_result(self):

        jobstatus = self.job.status

        self.logger.debug("Job COMPLETED")
        self.logger.debug("Status: %s" % jobstatus)
        self.logger.debug("Results file: %s" % self.results_file)
        if os.path.exists(self.results_file):
            result = XSDataResultMXCuBE.parseFile(self.results_file)
            self.logger.debug("EDNA Result loaded from file (type is %s" %
                              str(type(result)))
        else:
            self.logger.debug(
                "Cannot find output file, returning empty string.")
            result = ""

        return result


def test_hwo(hwo):
    ofile = "/tmp/edna/edna_result"
    odir = "/tmp/edna"
    test_input_file = "/beamlines/bl13/projects/cycle2018-I/2018012551-bcalisto/" \
                      "mx2018012551/DATA/20180131/PROCESS_DATA/" \
                      "characterisation_ref-Thrombin-TB-TTI1_A_run1_1/" \
                      "EDNAInput_2004391.xml"
    result = hwo.run_edna(test_input_file, ofile, odir)
    print(result)
