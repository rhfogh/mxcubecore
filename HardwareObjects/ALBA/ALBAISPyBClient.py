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
[Name] ALBAISPyBClient

[Description]
HwObj used to interface the ISPyB database

[Signals]
- None
"""

from __future__ import print_function

import logging

from ISPyBClient2 import ISPyBClient2

__credits__ = ["ALBA Synchrotron"]
__version__ = "2.3"
__category__ = "General"


class ALBAISPyBClient(ISPyBClient2):
    
    def __init__(self, *args):
        ISPyBClient2.__init__(self, *args)
        self.logger = logging.getLogger("HWR.ALBAISPyBClient")
    
    def init(self):
        ISPyBClient2.init(self)
        self.logger.debug("Initializing {0}".format(self.__class__.__name__))
    
    def ldap_login(self, login_name, psd, ldap_connection):
        # overwrites standard ldap login  is ISPyBClient2.py
        #  to query for homeDirectory

        if ldap_connection is None:
            ldap_connection = self.ldapConnection

        ok, msg = ldap_connection.login(
            login_name, psd, fields=[
                "uid", "homeDirectory"])
        self.logger.debug("ALBA LDAP login success %s (msg: %s)" % (ok, msg))
        if ok:
            vals = ldap_connection.get_field_values()
            if 'homeDirectory' in vals:
                home_dir = vals['homeDirectory'][0]
                self.logger.debug("The homeDirectory for user %s is %s" %
                    (login_name, home_dir))
                self.session_hwobj.set_ldap_homedir(home_dir)
        else:
            home_dir = '/tmp'
            self.session_hwobj.set_ldap_homedir(home_dir)

        return ok, msg

    def translate(self, code, what):
        """
        Given a proposal code, returns the correct code to use in the GUI,
        or what to send to LDAP, user office database, or the ISPyB database.
        """
        self.logger.debug("Translating %s %s" % (code, what))
        if what == 'ldap':
            if code == 'mx':
                return 'u'

        if what == 'ispyb':
            if code == 'u' or code == 'uind-':
                return 'mx'

        return code

    def prepare_collect_for_lims(self, mx_collect_dict):
        # Attention! directory passed by reference. modified in place

        for i in range(4):
            try:
                prop = 'xtalSnapshotFullPath%d' % (i + 1)
                path = mx_collect_dict[prop]
                ispyb_path = self.session_hwobj.path_to_ispyb(path)
                logging.debug("%s = %s " % (prop, ispyb_path))
                mx_collect_dict[prop] = ispyb_path
            except BaseException as e:
                logging.debug("Error when preparing collection for LIMS\n%s" % str(e))

    def prepare_image_for_lims(self, image_dict):
        for prop in ['jpegThumbnailFileFullPath', 'jpegFileFullPath']:
            try:
                path = image_dict[prop]
                ispyb_path = self.session_hwobj.path_to_ispyb(path)
                image_dict[prop] = ispyb_path
            except BaseException as e:
                logging.debug("Error when preparing image for LIMS\n%s" % str(e))


def test_hwo(hwo):
    proposal = 'mx2018002222'
    pasw = '2222008102'

    info = hwo.login(proposal, pasw)
    # info = hwo.get_proposal(proposal_code, proposal_number)
    # info = hwo.get_proposal_by_username("u2020000007")
    print(info['status'])

    print("Getting associated samples")
    session_id = 58248
    proposal_id = 8250
    samples = hwo.get_samples(proposal_id, session_id)
    print(samples)
