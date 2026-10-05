import logging
import os
try:
    import urllib2 as urllib
except ImportError:
    import urllib.request as urllib

try:
    from cookielib import CookieJar
except ModuleNotFoundError:
    from http.cookiejar import CookieJar

from suds.transport.http import HttpAuthenticated
from suds.client import Client

from ISPyBClient import ISPyBClient, _CONNECTION_ERROR_MSG
import traceback
from collections import namedtuple
from mxcubecore import HardwareRepository as HWR

# The WSDL root is configured in the hardware object XML file.
# _WS_USERNAME, _WS_PASSWORD have to be configured in the HardwareObject XML file.
_WSDL_ROOT = ""
_WS_BL_SAMPLE_URL = _WSDL_ROOT + "ToolsForBLSampleWebService?wsdl"
_WS_SHIPPING_URL = _WSDL_ROOT + "ToolsForShippingWebService?wsdl"
_WS_COLLECTION_URL = _WSDL_ROOT + "ToolsForCollectionWebService?wsdl"
_WS_AUTOPROC_URL = _WSDL_ROOT + "ToolsForAutoprocessingWebService?wsdl"
_WS_USERNAME = None
_WS_PASSWORD = None
_TIMEOUT = 7

SampleReference = namedtuple(
    "SampleReference",
    ["code", "container_reference", "sample_reference", "container_code"],
)


class SOLEILISPyBClient(ISPyBClient):
    def __init__(self, name):
        ISPyBClient.__init__(self, name)
        logger = logging.getLogger("ispyb_client")
        try:
            formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
            hdlr = logging.FileHandler(
                "/home/experiences/proxima2a/com-proxima2a/MXCuBE_v2_logs/ispyb_client.log"
            )
            hdlr.setFormatter(formatter)
            logger.addHandler(hdlr)
        except Exception:
            pass

        logger.setLevel(logging.INFO)
        self.connection_ok = None

    def init(self):
        """
        Init method declared by HardwareObject.
        """
        self.authServerType = self.get_property("authServerType") or "ldap"
        if self.authServerType == "ldap":
            # Initialize ldap
            self.ldapConnection = self.get_object_by_role("ldapServer")
            if self.ldapConnection is None:
                logging.getLogger("HWR").debug("LDAP Server is not available")

        self.loginType = self.get_property("loginType") or "proposal"
        self.loginTranslate = self.get_property("loginTranslate") or True
        self.beamline_name = HWR.beamline.session.beamline_name
        logging.debug("self.beamline_name in init %s" % self.beamline_name)

        self.ws_root = self.get_property("ws_root")
        self.ws_username = self.get_property("ws_username")
        if not self.ws_username:
            self.ws_username = _WS_USERNAME
        self.ws_password = self.get_property("ws_password")
        if not self.ws_password:
            self.ws_password = _WS_PASSWORD

        self.ws_collection = self.get_property("ws_collection")
        self.ws_shipping = self.get_property("ws_shipping")
        self.ws_tools = self.get_property("ws_tools")

        logging.info("Initializing SOLEIL ISPyB Client")
        logging.info("   - using http_proxy = %s " % os.environ["http_proxy"])

        try:

            if self.ws_root:
                logging.debug(
                    "attempting to connect to %s" % self.ws_root
                )

                try:
                    self._shipping = self._wsdl_shipping_client()
                    self._collection = self._wsdl_collection_client()
                    self._tools_ws = self._wsdl_tools_client()
                    logging.debug(
                        "extracted from ISPyB values for shipping, collection and tools"
                    )
                    self.connection_ok = True
                except Exception:
                    logging.exception("%s" % _CONNECTION_ERROR_MSG)
                    self.connection_ok = False
                    return

        except Exception:
            self.connection_ok = False
            print(traceback.print_exc())
            logging.getLogger("HWR").exception(_CONNECTION_ERROR_MSG)
            return
        try:
            proposals = HWR.beamline.session["proposals"]

            for proposal in proposals:
                code = proposal.code
                self._translations[code] = {}
                try:
                    self._translations[code]["ldap"] = proposal.ldap
                except AttributeError:
                    pass
                try:
                    self._translations[code]["ispyb"] = proposal.ispyb
                except AttributeError:
                    pass
                try:
                    self._translations[code]["gui"] = proposal.gui
                except AttributeError:
                    pass
        except IndexError:
            pass
        except Exception:
            pass

    def is_connected(self):
        return self.connection_ok

    def translate(self, code, what):
        """
        Given a proposal code, returns the correct code to use in the GUI,
        or what to send to LDAP, user office database, or the ISPyB database.
        """
        if what == "ispyb":
            return "mx"
        return ""

    def _wsdl_shipping_client(self):
        return self._wsdl_client(self.ws_shipping)

    def _wsdl_tools_client(self):
        return self._wsdl_client(self.ws_tools)

    def _wsdl_collection_client(self):
        return self._wsdl_client(self.ws_collection)

    def _wsdl_client(self, service_name):
        # Handling of redirection at soleil needs cookie handling
        cj = CookieJar()

        url_opener = urllib.build_opener(urllib.HTTPCookieProcessor(cj))

        trans = HttpAuthenticated(username=self.ws_username, password=self.ws_password)
        logging.debug("_wsdl_client service_name %s - trans %s" % (service_name, trans))

        trans.urlopener = url_opener
        urlbase = service_name + "?wsdl"
        locbase = service_name

        ws_root = self.ws_root.strip()

        url = ws_root + urlbase
        loc = ws_root + locbase

        ws_client = Client(url, transport=trans, timeout=_TIMEOUT, location=loc, cache=None)

        return ws_client

    def prepare_collect_for_lims(self, mx_collect_dict):
        # Attention! directory passed by reference. modified in place

        prop = "EDNA_files_dir"
        path = mx_collect_dict[prop]
        ispyb_path = HWR.beamline.session.path_to_ispyb(path)
        mx_collect_dict[prop] = ispyb_path

        prop = "process_directory"
        path = mx_collect_dict["fileinfo"][prop]
        ispyb_path = HWR.beamline.session.path_to_ispyb(path)
        mx_collect_dict["fileinfo"][prop] = ispyb_path

        for i in range(4):
            try:
                prop = "xtalSnapshotFullPath%d" % (i + 1)
                path = mx_collect_dict[prop]
                ispyb_path = HWR.beamline.session.path_to_ispyb(path)
                logging.debug("%s is %s " % (prop, ispyb_path))
                mx_collect_dict[prop] = ispyb_path
            except Exception:
                pass

    def prepare_image_for_lims(self, image_dict):
        for prop in ["jpegThumbnailFileFullPath", "jpegFileFullPath"]:
            try:
                path = image_dict[prop]
                ispyb_path = HWR.beamline.session.path_to_ispyb(path)
                image_dict[prop] = ispyb_path
            except Exception:
                pass

    @staticmethod
    def from_data_collect_parameters(ws_client, mx_collect_dict):
        """
        Ceates a dataCollectionWS3VO from mx_collect_dict.
        :rtype: dataCollectionWS3VO
        """
        logging.debug('from_data_collect_parameters mx_collect_dict %s' % mx_collect_dict)
        if len(mx_collect_dict["oscillation_sequence"]) != 1:
            raise ISPyBArgumentError(
                "ISPyBServer: number of oscillations"
                + " must be 1 (until further notice...)"
            )
        data_collection = None

        try:

            data_collection = ws_client.factory.create("ns0:dataCollectionWS3VO")
        except Exception:
            raise

        osc_seq = mx_collect_dict["oscillation_sequence"][0]

        try:
            if "status" in mx_collect_dict:
                data_collection.runStatus = mx_collect_dict["status"]
            else:
                data_collection.runStatus = "Unknown"
            data_collection.axisStart = osc_seq["start"]

            data_collection.axisEnd = float(osc_seq["start"]) + (
                float(osc_seq["range"]) - float(osc_seq["overlap"])
            ) * float(osc_seq["number_of_images"])

            data_collection.axisRange = osc_seq["range"]
            data_collection.overlap = osc_seq["overlap"]
            data_collection.numberOfImages = osc_seq["number_of_images"]
            data_collection.startImageNumber = osc_seq["start_image_number"]
            data_collection.numberOfPasses = osc_seq["number_of_passes"]
            data_collection.exposureTime = osc_seq["exposure_time"]
            data_collection.imageDirectory = mx_collect_dict["fileinfo"]["directory"]

            if "kappaStart" in osc_seq:
                if osc_seq["kappaStart"] != 0 and osc_seq["kappaStart"] != -9999:
                    data_collection.rotationAxis = "Omega"
                    data_collection.omegaStart = osc_seq["start"]
                else:
                    data_collection.rotationAxis = "Phi"
            else:
                data_collection.rotationAxis = "Phi"
                osc_seq["kappaStart"] = -9999
                osc_seq["phiStart"] = -9999

            data_collection.kappaStart = osc_seq["kappaStart"]
            data_collection.phiStart = osc_seq["phiStart"]

        except KeyError as diag:
            err_msg = "ISPyBClient: error storing a data collection (%s)" % str(diag)
            raise ISPyBArgumentError(err_msg)

        data_collection.detector2theta = 0

        try:
            data_collection.dataCollectionId = int(mx_collect_dict["collection_id"])
        except KeyError:
            pass

        try:
            data_collection.wavelength = mx_collect_dict["wavelength"]
        except KeyError as diag:
            pass

        res_at_edge = None
        try:
            try:
                res_at_edge = float(mx_collect_dict["resolution"])
            except Exception:
                res_at_edge = float(mx_collect_dict["resolution"]["lower"])
        except KeyError:
            try:
                res_at_edge = float(mx_collect_dict["resolution"]["upper"])
            except Exception:
                pass
        if res_at_edge is not None:
            data_collection.resolution = res_at_edge

        try:
            data_collection.resolutionAtCorner = mx_collect_dict["resolutionAtCorner"]
        except KeyError:
            pass

        try:
            data_collection.detectorDistance = mx_collect_dict["detectorDistance"]
        except KeyError as diag:
            pass

        try:
            data_collection.xbeam = mx_collect_dict["xBeam"]
            data_collection.ybeam = mx_collect_dict["yBeam"]
        except KeyError as diag:
            pass

        try:
            data_collection.beamSizeAtSampleX = mx_collect_dict["beamSizeAtSampleX"]
            data_collection.beamSizeAtSampleY = mx_collect_dict["beamSizeAtSampleY"]
        except KeyError:
            pass

        try:
            data_collection.beamShape = mx_collect_dict["beamShape"]
        except KeyError:
            pass

        try:
            data_collection.slitGapHorizontal = mx_collect_dict["slitGapHorizontal"]
            data_collection.slitGapVertical = mx_collect_dict["slitGapVertical"]
        except KeyError:
            pass

        try:
            data_collection.imagePrefix = mx_collect_dict["fileinfo"]["prefix"]
        except KeyError as diag:
            pass

        try:
            data_collection.imageSuffix = mx_collect_dict["fileinfo"]["suffix"]
        except KeyError as diag:
            pass
        try:
            data_collection.fileTemplate = mx_collect_dict["fileinfo"]["template"]
        except KeyError as diag:
            pass

        try:
            data_collection.dataCollectionNumber = mx_collect_dict["fileinfo"][
                "run_number"
            ]
        except KeyError as diag:
            pass

        try:
            data_collection.synchrotronMode = mx_collect_dict["synchrotronMode"]
            data_collection.flux = mx_collect_dict["flux"]
        except KeyError as diag:
            pass

        try:
            data_collection.flux_end = mx_collect_dict["flux_end"]
        except KeyError as diag:
            pass

        try:
            data_collection.transmission = mx_collect_dict["transmission"]
        except KeyError:
            pass

        try:
            data_collection.undulatorGap1 = mx_collect_dict["undulatorGap1"]
            data_collection.undulatorGap2 = mx_collect_dict["undulatorGap2"]
            data_collection.undulatorGap3 = mx_collect_dict["undulatorGap3"]
        except KeyError:
            pass

        try:
            data_collection.xtalSnapshotFullPath1 = mx_collect_dict[
                "xtalSnapshotFullPath1"
            ]
        except KeyError:
            pass

        try:
            data_collection.xtalSnapshotFullPath2 = mx_collect_dict[
                "xtalSnapshotFullPath2"
            ]
        except KeyError:
            pass

        try:
            data_collection.xtalSnapshotFullPath3 = mx_collect_dict[
                "xtalSnapshotFullPath3"
            ]
        except KeyError:
            pass

        try:
            data_collection.xtalSnapshotFullPath4 = mx_collect_dict[
                "xtalSnapshotFullPath4"
            ]
        except KeyError:
            pass

        try:
            data_collection.centeringMethod = mx_collect_dict["centeringMethod"]
        except KeyError:
            pass

        try:
            data_collection.actualCenteringPosition = mx_collect_dict[
                "actualCenteringPosition"
            ]
        except KeyError:
            pass

        try:
            data_collection.dataCollectionGroupId = mx_collect_dict["group_id"]
        except KeyError:
            pass

        try:
            data_collection.detectorId = mx_collect_dict["detector_id"]
        except KeyError:
            pass

        try:
            data_collection.strategySubWedgeOrigId = mx_collect_dict[
                "screening_sub_wedge_id"
            ]
        except Exception:
            pass

        try:
            start_time = mx_collect_dict["collection_start_time"]
            start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            data_collection.startTime = start_time
        except Exception:
            pass

        data_collection.endTime = datetime.now()

        return data_collection

def test_hwo(hwo):
    proposal_code = "mx"
    proposal_number = "20100023"
    proposal_psd = "tisabet"

    # proposal_number = '20160745'
    # proposal_psd = '087D2P3252'

    print("Trying to login to ispyb")
    info = hwo.login(proposal_number, proposal_psd)
    print("logging in returns: ", str(info))


def test():
    HWR.init_hardware_repository('/usr/local/mxcube_2021/mxcubecore/mxcubecore/configuration/soleil/px2/production')
    hwr = HWR.get_hardware_repository()

    hwr.connect()

    db = HWR.beamline.lims

    print("db", db)
    print("dir(db)", dir(db))
    # print 'db._SOLEILISPyBClientShipping', db._SOLEILISPyBClientShipping
    # print 'db.Shipping', db.Shipping

    proposal_code = "mx"
    proposal_number = "20250023" #"20100023"
    proposal_psd = "tisabet"

    info = db.get_proposal(proposal_code, proposal_number)  # proposal_number)
    print(info)

    #info = db.login(proposal_number, proposal_psd)
    #print(info)


if __name__ == "__main__":
    test()
