#  encoding: utf-8
"""Experiment report generator

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
import hashlib
import json
import os
from pathlib import Path
import sys

from pyicat_plus.metadata.drac_report_builder import DracReportBuilder

# EDNA uses the ANGSTROM SIGN (U+212B), not LATIN CAPITAL LETTER A WITH RING ABOVE
ANGSTROM = "\u212b"
DIVIDER = 120 * "="

DEFAULT_DATA_DIR = Path(
    "/home/rhfogh/Projects/MASSIFreport/input/drac_report_builder_example2/drac_report_builder/data"
)
GPHL_FILE = "gphl_report_data.json"

def make_report(input, **options):

    input_dir = Path(input)
    output_dir = options.get("output") or input_dir / "output"
    image_dir = options.get("images") or DEFAULT_DATA_DIR / "images"

    data = load_data(input_dir, image_dir)

    report = build_report(data, input_dir, image_dir)
    os.makedirs(output_dir, exist_ok=True)
    path_to_json = report.render_json(output_dir)
    print ("JSON output:", path_to_json)
    path_to_html = report.render_html(output_dir, name_of_index_file="index.html")
    print ("HTML output:", path_to_html)
    return path_to_json, path_to_html

def build_report(data, input_dir, image_dir):
    report = DracReportBuilder(
        dataset_type="Characterisation",
        dataset_title="GPhL experiment summary",
    )
    report.add_info(" ")
    report.add_info(DIVIDER)
    report.add_info("Characterisation:")
    add_reference_images(report, data["images"], image_dir)
    add_image_quality_indicators(report)
    report.add_info(" ")
    report.add_info(DIVIDER)
    report.add_info("Indexing solution:")
    selected_solution = data.get("selected_solution")
    if selected_solution is None:
        report.add_warning(
            "WARNING, no indexing solution! Using fall-back Triclinic strategy"
        )
    else:
        add_indexing_table(report, data["indexing_solutions"])
        add_indexing_table(report, [data["indexing_solutions"][selected_solution]])
    report.add_info(" ")
    report.add_info(DIVIDER)
    report.add_info("Experiment:")
    add_input_parameters(report, data)
    add_experiment_parameters(report, data)
    add_strategy_parameters(report, data)
    #
    report.add_info(" ")
    report.add_info(DIVIDER)
    report.add_info("Log files:")
    title = "IDXREF.LP"
    report.add_log_file(
        title,
        "Final IDXREF.LP",
        str(input_dir / "characterisation" / "03_XDS" / f"{title}")
    )
    title = "stratcal.stdout"
    report.add_log_file(
        title,
        "Strategy calculation log",
        str(input_dir /  "characterisation" / "06_stratcal" / f"{title}")
    )

    return report

def load_data(input_dir, image_dir):
    data = json.load(open(input_dir / GPHL_FILE))
    data.update(load_stratcal(input_dir))
    data.update(load_image_data(image_dir))
    #
    return data

def load_image_data(imagedir):
    return (
        {
            "images": [
                {
                    "name": "ref-AnRBC-Nm_x02_-1_1_4_0001",
                    "score": 574.1,
                    "resolution": 1.2
                },
                {
                    "name": "ref-AnRBC-Nm_x02_-1_1_4_0002",
                    "score": 747.9,
                    "resolution": 1.2
                },
                {
                    "name": "ref-AnRBC-Nm_x02_-1_1_4_0003",
                    "score": 569.5,
                    "resolution": 1.2
                },
                {
                    "name": "ref-AnRBC-Nm_x02_-1_1_4_0004",
                    "score": 511.0,
                    "resolution": 1.4
                },
            ]
        }
    )

def add_image_quality_indicators(report):
    # Number -> (Tot integr signal, Spot total, In-Res Total, Good Bragg,
    #            Ice Rings, Meth 1 Res, Meth 2 Res, Max unit cell)
    PREFIX = "ref-AnRBC-Nm_x02_-1_1_4"
    DOZOR = {
        4: ("511.0", "1.4"),
        3: ("569.5", "1.2"),
        2: ("747.9", "1.2"),
        1: ("574.2", "1.2")
    }
    IMAGE_NUMBERS = [1, 2, 3, 4]
    more_data = {
        1: ("2856991", "445", "380", "341", "0", "1.58", "1.90", "107.3"),
        2: ("2503212", "290", "251", "220", "3", "1.63", "1.96", "127.0"),
        3: ("2170426", "339", "318", "288", "0", "1.86", "1.85", "107.3"),
        4: ("1972640", "219", "208", "180", "3", "1.98", "1.95", "129.5"),
    }
    data = [
        [f"{PREFIX}_{number:04d}.cbf", *DOZOR[number], *more_data[number]]
        for number in IMAGE_NUMBERS
    ]
    report.add_table(
        title="Image quality indicators",
        columns=[
            "File",
            "Dozor score (1)",
            f"Dozor visible res. [{ANGSTROM}]",
            "Tot integr signal (2)",
            "Spot total",
            "In-Res Total",
            "Good Bragg",
            "Ice Rings",
            "Meth 1 Res",
            "Meth 2 Res",
            "Max unit cell",
        ],
        data=data,
    )
    report.add_info(
        "1. Dozor score: criteria of diffraction signal strength that uses "
        "intensities over background vs resolution. Popov 2014, to be published."
    )
    report.add_info(
        "2. Total integrated signal, spot total etc: results from cctbx Spotfinder"
    )

def load_stratcal(input_dir ):
    result = {}
    for line in open(input_dir / "characterisation" / "06_stratcal" / "stratcal.stdout"):
        if line.startswith("STRATEGY:"):
            fields = line.split("\t")
            result["strategy_type"] = fields[1]
            result["strategy_variant"] = fields[2]
            result["strategy_symmetry"] = fields[3]
            result["strategy_subtype"] = fields[4]
            break
    return result

def add_reference_images(report, images, image_dir):
    """Row of reference (diffraction) images with Dozor score in the title."""
    report.start_image_list()
    for dd0 in images:
        name = dd0.get("name", "")
        score = dd0.get("score", 0.0)
        resolution = dd0.get("resolution", 0.0)
        report.add_image(
            str(image_dir / f"{name}.jpeg"),
            image_title=f"{name} (Dozor: {score:.1f}, {resolution:.3f} {ANGSTROM})",
            path_to_thumbnail_image=str(image_dir / f"{name}_thumbnail.jpeg"),
        )
    report.end_image_list()

def add_input_parameters(report, data):

    wfdata = data["gphl_workflow"]
    # Input parameters
    title = "Input parameters"
    columns = [
        "Expected Space group",
        "Crystal classes",
        "Relative radiation sensitivity",
        "Crystal thickness [µ]"
    ]
    rows = [
        [
            wfdata["input_space_group"],
             " ".join(wfdata["crystal_classes"]),
            f"{wfdata.get('relative_rad_sensitivity', 1.0):.3f}",
            f"{wfdata.get('crystal_thickness', 0.0):.1f}",
        ]
    ]
    #
    report.add_table(title, columns, rows)

def add_experiment_parameters(report, data):

    wfdata = data["gphl_workflow"]
    # Input parameters
    title = "Experiment parameters"
    wavelengths = wfdata["wavelengths"]
    columns = []
    row = []
    if len(wavelengths) == 1:
        columns.append(f"Wavelength [{ANGSTROM}]")
        row.append(f"{wavelengths[0]:.3f}")
    else:
        for indx in range(len(wavelengths)):
            columns.append(f"Wavelength{indx+1} [{ANGSTROM}]")
            row.append(f"{wavelengths[indx]:.3f}")

    columns.append(f"Resolution [{ANGSTROM}]")
    row.append(f"{wfdata['resolution']:.3f}")
    columns.append("Exposure [s]")
    row.append(f"{wfdata['exposure_time']:.4f}")
    columns.append("Image width [°]")
    row.append(f"{wfdata['image_width']:.2f}")
    columns.append("Transmission [%]")
    row.append(f"{wfdata['transmission']:.2f}")
    columns.append("Total dose [MGy]")
    row.append(f"{wfdata['total_radiation_dose']:.3f}")
    #
    report.add_table(title, columns, [row])


def add_strategy_parameters(report, data):
    wfdata = data["gphl_workflow"]
    title = "GPhL experimental strategy"
    columns = []
    row = []

    columns.append("Strategy")
    row.append(f"{wfdata['strategy']}")
    columns.append("Variant")
    row.append(f"{data['strategy_variant']}")
    columns.append("Subtype")
    row.append(f"{data['strategy_subtype']}")
    columns.append("Strategy symmetry")
    row.append(f"{data['strategy_symmetry']}")
    columns.append("Repetitions #")
    row.append(f"{wfdata['repetition_count']}")
    columns.append("Wavelengths #")
    row.append(f"{len(wfdata['wavelengths'])}")
    columns.append("Total length [°]")
    multiple = wfdata['repetition_count'] * len(wfdata['wavelengths'])
    row.append(f"{wfdata['strategy_length'] * multiple:.1f}")
    #
    report.add_table(title, columns, [row])

    report.add_info(f"Raw data directory:\t {data['data_directory']}")

    title = "Sweeps"
    columns = [
        "File name",
        "Kappa [°]",
        "Phi [°]",
        "Omega start [°]",
        "Width [°]",
        "First image number"
    ]
    if len(data["gphl_workflow"]["wavelengths"]) > 1:
        columns.append(f"Wavelength [{ANGSTROM}]")
    rows = []
    for sweep in data["sweeps"]:
        row = [
            f"{sweep['file_name']}",
            f"{sweep['kappa']:.1f}",
            f"{sweep['kappa_phi']:.1f}",
            f"{sweep['omega']:.1f}",
            f"{sweep['width']:.1f}",
            f"{sweep['image_start_num']}"
        ]
        if len(data["gphl_workflow"]["wavelengths"]) > 1:
            row.append(f"{sweep['wavelength']:.3f}")
        rows.append(row)
    #
    report.add_table(title, columns, rows)

def add_indexing_table(report, indexing_solutions):

    columns = [
        "Lattice character",
        "Bravais lattice",
        "Quality of fit",
        f"a [{ANGSTROM}]",
        f"b [{ANGSTROM}]",
        f"c [{ANGSTROM}]",
        "alpha [°]",
        "beta [°]",
        "gamma [°]",
    ]
    tags = [
        "lattice_character",
        "bravais_lattice",
        "quality_of_fit",
        "a",
        "b",
        "c",
        "alpha",
        "beta",
        "gamma",
    ]
    if len(indexing_solutions) == 1:
        title = "Selected indexing solution"
        rows = [list(indexing_solutions[0][tag] for tag in tags)]
    else:
        title = "Indexing solutions"
        count = len(list(dd0 for dd0 in indexing_solutions if dd0.get("is_consistent")))
        count += 3
        rows = list(list(row[tag] for tag in tags) for row in indexing_solutions[:count])
        endrow = list(" " for tag in tags)
        endrow[0] = "..."
        rows.append(endrow)
    report.add_table(title, columns, rows)

if __name__ == "__main__":

    from argparse import ArgumentParser, RawTextHelpFormatter

    parser = ArgumentParser(
        prog="report_generator.py",
        formatter_class=RawTextHelpFormatter,
        prefix_chars="--",
        description="""
Generation of MASSIF-1 experiment reports
        """,
    )

    parser.add_argument("--input", metavar="inputdir", help="input directory\n")

    parser.add_argument(
        "--output",
        metavar="outputdir",
        help="Name of output directory. Defaults to inputdir/output\n",
    )

    parser.add_argument(
        "--images",
        metavar="imagedir",
        help="Name of directory containin images.\n",
    )

    argsobj = parser.parse_args()
    options_dict = vars(argsobj)

    # print("CMD: {" ".join(sys.argv))}"

    make_report(**options_dict)

