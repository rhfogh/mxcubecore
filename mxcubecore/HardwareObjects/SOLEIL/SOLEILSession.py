import os
import time
import logging
import re
import Session

class SOLEILSession(Session.Session):

    def __init__(self, *args, **kwargs):
        Session.Session.__init__(self, *args, **kwargs)
        self.username = ""
        self.gid = ""
        self.uid = ""
        self.projuser = ""
        self.bag_ldaplogin = ""
        self.log = logging.getLogger("HWR")

    def path_to_ispyb(self, path):
        ispyb_base = self["file_info"].get_property("ispyb_base_directory") % {
            "projuser": self.projuser
        }
        path = path.replace(
            self["file_info"].get_property("base_directory"), ispyb_base
        )
        try:
            project_number = re.findall('.*/published-data/(\d*)/.*', path)[0]
            print("project_number %s" % project_number)
            path = path.replace('published-data/%s' % project_number, 'published-data')
        except:
            pass
        return path

    def is_master_user(self, username=None, password=None, check_password=False):
        return True

    def set_user_info(self, username, user_id=None, group_id=None, projuser=None):
        self.log.info('set_user_info: %s, %s, %s, %s' % (username, user_id, group_id, projuser))
        if username == '':
            self.log.info("User %s logged out. " % (self.username,))
        else:
            self.log.info("User %s logged in. gid=%s / uid=%s / projuser %s" % (username, group_id, user_id, projuser))

        self.username = username
        self.group_id = group_id
        self.user_id = user_id
        if projuser != None:
           self.projuser = projuser
        else:
           self.projuser = username

    def set_proposal(self, code="", number="", proposal_id="", session_id=1, bag_ldaplogin=""):
        self.proposal_code = code
        self.proposal_number = number #'20191314'
        self.proposal_id = proposal_id
        self.session_id = session_id
        self.bag_ldaplogin = bag_ldaplogin

        self.log.info("setting proposal to %s, %s, %s" % \
                   (self.proposal_code, self.proposal_number, self.bag_ldaplogin))

        # username should be ldap username. ldap can give more info
        username = "%s" % (self.proposal_number,) #(os.path.join(self.proposal_number, bag_ldaplogin),)

        self.set_user_info(username)

    def get_proposal_number(self):
        """
        :returns: The proposal, 'local-user' if no proposal is
                  available
        :rtype: str
        """

        if self.proposal_number:
            return "%s" % (self.proposal_number)
        else:
            try:
                login = os.getlogin()
            except OSError:
                import pwd
                login = pwd.getpwuid(os.getuid()).pw_name
            return login

    def get_base_data_directory(self):
        """
        Returns the base data directory taking the 'contextual'
        information into account, such as if the current user
        is inhouse.

        :returns: The base data path.
        :rtype: str
        """
        user_category = ""
        directory = ""

        if self.session_start_date:
            start_time = self.session_start_date.split(" ")[0]
        else:
            # PL. To avoid mixing users directory if they restart the application
            # after midnight but before 8 AM, the directory date doesn't change:
            _local_time = time.localtime()
            if _local_time[3] > 7:
                start_time = time.strftime("%Y-%m-%d")
            else:
                # substract 8 hours to current date to get yesterday's date.
                _local_time = time.localtime((time.time() - 8 * 60 * 60))
                start_time = time.strftime("%Y-%m-%d", _local_time)

        if self.is_inhouse():
            directory = os.path.join(self.base_directory, self.get_proposal_number(), start_time)
        else:
            if self.get_proposal_number().isdigit() and self.bag_ldaplogin != "" and len(self.get_proposal_number()) > 8:
                directory = os.path.join(self.base_directory, self.get_proposal_number()[:8], self.bag_ldaplogin, start_time)
            else:
                directory = os.path.join(self.base_directory, self.get_proposal_number(), start_time)

        return directory

    def get_archive_directory(self, directory=None):
        if directory is None:
            thedir = self.get_base_data_directory()
        else:
            thedir = directory

        if "RAW_DATA" in thedir:
            thedir = thedir.replace("RAW_DATA", "ARCHIVE")
        else:
            thedir = os.path.join(thedir, "ARCHIVE")

        return thedir

    def get_ruche_info(self, path):

        if self.is_inhouse(self.username):
           usertype = "soleil"
        else:
           usertype = "users"

        basedir = os.path.dirname(path)
        ruchepath = basedir.replace(
            self["file_info"].get_property("base_directory"), ""
        )
        if ruchepath and ruchepath[0] == os.path.sep:
            ruchepath = ruchepath[1:]

        infostr = "%s %s %s %s %s %s\n" % (
            usertype,
            self.username,
            self.user_id,
            self.group_id,
            basedir,
            ruchepath,
        )
        return infostr


def test():
    from mxcubecore import HardwareRepository as HWR
    #hwr_directory = '/usr/local/bin/mxcube_local/ExampleFiles/HardwareObjects.xml/soleil_px2/singleton_objects'
    hwr = HWR.get_hardware_repository()
    hwr.connect()

    sess = HWR.beamline.session

    sess.set_user_info("mx2014", "143301", "14330", "20100023")
    #path = "/927bis/ccd/2015_Run2/visitor/mx2014/px2/20150120/ARCHIVE/mx2014/mx2014_2_4.snapshot.jpeg"
    path = "/nfs/data3/2020_Run1/20200018/2020-02-06/ARCHIVE/MUS81/MUS81-CD027556_A05-3_AD026A-03/MUS81-CD027556_A05-3_AD026A-03_1_1.snapshot.jpeg"
    ispyb_path = sess.path_to_ispyb(path)
    print(path)
    print("  will become ")
    print(ispyb_path)

    print(sess.get_ruche_info(path))

if __name__ == "__main__":
    test()
