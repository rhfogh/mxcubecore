#! /usr/bin/env python
# encoding: utf-8
# 
"""
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
import numpy as np
import f90nml

__copyright__ = """ Copyright © 2016 -  2023 MXCuBE Collaboration."""
__license__ = "LGPLv3+"
__author__ = "rhfogh"
__date__ = "12/06/2023"

minikappa_xml_template = """<device class="MiniKappaCorrection">
   <kappa>
      <direction>%s</direction>
      <position>%s</position>
   </kappa>
   <phi>
      <direction>%s</direction>
      <position>%s</position>
   </phi>
</device>"""

def get_recen_data(transcal_file, instrumentation_file, diffractcal_file=None):
    """Read recentring data from GPhL files

    Args:
        transcal_file: tranccal.nml  input file
        instrumentation_file: instrumentation.nml input file
        diffractcal_file: diffrqctcal,.nl ioptional input file

    Returns: dict

"""
    result = {}

    if not (os.path.isfile(transcal_file) and os.path.isfile(instrumentation_file)):
        return None

    transcal_data = f90nml.read(transcal_file)["sdcp_instrument_list"]
    home_position = transcal_data["trans_home"]
    cross_sec_of_soc = transcal_data["trans_cross_sec_of_soc"]
    instrumentation_data = f90nml.read(instrumentation_file)["sdcp_instrument_list"]
    try:
        diffractcal_data = f90nml.read(diffractcal_file)["sdcp_instrument_list"]
    except:
        diffractcal_data = instrumentation_data

    ll0 = diffractcal_data["gonio_axis_dirs"]
    result["omega_axis"] = ll0[:3]
    result["kappa_axis"] = ll0[3:6]
    result["phi_axis"] = ll0[6:]
    ll0 = instrumentation_data["gonio_centring_axis_dirs"]
    result["trans_1_axis"] = ll0[:3]
    result["trans_2_axis"] = ll0[3:6]
    result["trans_3_axis"] = ll0[6:]
    result["gonio_centring_axis_names"] = instrumentation_data[
        "gonio_centring_axis_names"
    ]
    result["cross_sec_of_soc"] = cross_sec_of_soc
    result["home"] = home_position
    #
    return result

def make_minikappa_data(
    transcal_file, instrumentation_file, output=None, diffractcal_file=None
):
    if output is None:
        output = "minikappa-correction.xml"
    recen_data = get_recen_data(transcal_file, instrumentation_file, diffractcal_file)
    if not recen_data:
        raise ValueError(
            "Could not get data from files: %s, %s, %s"
            % (transcal_file, instrumentation_file, diffractcal_file)
        )

    home = np.array(recen_data["home"])
    cross_sec_of_soc = np.array(recen_data["cross_sec_of_soc"])
    posk0 = home + cross_sec_of_soc
    posp0 = home - cross_sec_of_soc
    tags = ["trans_1_axis","trans_2_axis", "trans_3_axis"]
    indices = list(
        recen_data["gonio_centring_axis_names"].index(tag)
        for tag in ("sampx", "sampy", "phiy")
    )
    ll0 = []
    posk = []
    posp = []
    for indx in indices:
        ll0 += recen_data[tags[indx]]
        posk.append(posk0[indx])
        posp.append(posp0[indx])
    transform = np.matrix(ll0)
    transform.shape = (3,3)

    text = minikappa_xml_template % (
        transform.dot( np.array(recen_data["kappa_axis"])).tolist()[0],
        posk,
        transform.dot( np.array(recen_data["phi_axis"])).tolist()[0],
        posp,
    )
    open(output,"w").write(text)
    print(text)

if __name__ == "__main__":

    from argparse import ArgumentParser, RawTextHelpFormatter

    parser = ArgumentParser(
        prog="Transcal2MiniKappa.py",
        formatter_class=RawTextHelpFormatter,
        prefix_chars="--",
        description="""
Conversion from GPhL recentring data to MiniKAppaCIOrrection recentring data

Requires an up-to-date transcal.nml file a matching instrumentation.nml file 
and preferably an up-to-date diffractcal.nml file
        """
    )

    parser.add_argument(
        "--transcal_file", metavar="transcal_file", help="transcal.nml file\n"
    )

    parser.add_argument(
        "--instrumentation_file",
        metavar="instrumentation_file",
        help="instrumentation.nml file\n"
    )

    parser.add_argument(
        "--diffractcal_file", metavar="diffractcal_file", help="diffractcal.nml file\n"
    )

    parser.add_argument(
        "--output",
        metavar="outputfile",
        help="Name of output xml file. Defaults to minikappa-correction.xml\n",
    )


    argsobj = parser.parse_args()
    options_dict = vars(argsobj)

    make_minikappa_data(**options_dict)
