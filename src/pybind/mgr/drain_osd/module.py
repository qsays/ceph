"""
TODO
"""

from typing import List
from mgr_module import MgrModule, HandleCommandResult
from threading import Event
import json
import errno

class Test(MgrModule):
    # these are CLI commands we implement
    COMMANDS = [
        {
            "cmd": "drain name=osd_ids,type=CephInt,req=true,n=N",
            "desc": "drain osd ids",
            "perm": "r"
        },
    ]

    # These are module options we understand.  These can be set with
    #
    #   ceph config set global mgr/hello/<name> <value>
    #
    # e.g.,
    #
    #   ceph config set global mgr/hello/place Earth
    #
    MODULE_OPTIONS = [
        {
            'name': 'config_option_a',
            'default': ''
        }
    ]

    # These are "native" Ceph options that this module cares about.
    NATIVE_OPTIONS = [
        'mgr_tick_period',
        # TODO: query for initial osd weight
    ]

    osd_ids = []

    def __init__(self, *args, **kwargs):
        super(Test, self).__init__(*args, **kwargs)

        # set up some members to enable the serve() method and shutdown()
        self.run = True
        self.event = Event()

        # ensure config options members are initialized; see config_notify()
        self.config_notify()

    def config_notify(self):
        """
        This method is called whenever one of our config options is changed.
        """
        # This is some boilerplate that stores MODULE_OPTIONS in a class
        # member, so that, for instance, the 'emphatic' option is always
        # available as 'self.emphatic'.
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']) or opt['default'])
            self.log.debug(' mgr option %s = %s',
                           opt['name'], getattr(self, opt['name']))
        # Do the same for the native options.
        for opt in self.NATIVE_OPTIONS:
            setattr(self,
                    opt,
                    self.get_ceph_option(opt))
            self.log.debug(' native option %s = %s', opt, getattr(self, opt))

    def handle_command(self, inbuf, cmd):
        ret = 0
        out = ''
        err = ''
        _ = inbuf
        if cmd['prefix'] == 'drain':
            if not cmd['osd_ids']:
                self.log.error(f'OSD_IDS are empty. fail here')
            self.osd_ids = cmd['osd_ids']
            self.log.debug(f'Got OSDs <{self.osd_ids}> from the commandline')
            # 1) check if all provided osds can be stopped
            # 2) if yes, set weight to 0
            # 2.1) if not, split in half until can be stopped.. if not possible return
            # 3) wait for osds to be empty initially and then send to background
            # 4) report
            self.all_empty(self.osd_ids)
        elif cmd['prefix'] == 'placeholder':
            pass
        else:
            return (-errno.EINVAL, '',
                    "Command not found '{0}'".format(cmd['prefix']))

        return HandleCommandResult(
            retval=ret,   # exit code
            stdout=out,   # stdout
            stderr=err)

    def serve(self):
        """
        This method is called by the mgr when the module starts and can be
        used for any background activity.
        """
        self.log.info("Starting")
        while self.run:
            # Do some useful background work here.

            # Use mgr_tick_period (default: 2) here just to illustrate
            # consuming native ceph options.  Any real background work
            # would presumably have some more appropriate frequency.
            sleep_interval = self.mgr_tick_period
            # TODO: add set_health_checks
            self.log.debug('Sleeping for %d seconds', sleep_interval)
            ret = self.event.wait(sleep_interval)
            self.event.clear()

    def shutdown(self):
        """
        This method is called by the mgr when the module needs to shut
        down (i.e., when the serve() function needs to exit).
        """
        self.log.info('Stopping')
        self.run = False
        self.event.set()

    def osds_in_cluster(self, osd_ids: List[int]) -> bool:
        self.log.info(f"Checking if provided osds <{osd_ids}> exist in the cluster")
        osd_map = self.get_osdmap()
        cluster_osds = [x.get('osd') for x in osd_map.dump().get('osds', [])]
        for osd_id in osd_ids:
            if int(osd_id) not in cluster_osds:
                self.log.error(f"Could not find {osd_id} in cluster")
                return False
        return True

    def all_empty(self, osd_ids: List[int]) -> bool:
        for osd_id in osd_ids:
            if not self.is_empty(osd_id):
                return False
        return True

    def is_empty(self, osd_id: int) -> bool:
        base_cmd = 'osd df'
        ret, out, err = self.mon_command({
            'prefix': base_cmd,
            'format': 'json'
        })
        osd_df = json.loads(out)
        osd_nodes = osd_df.get('nodes', [])
        for osd_node in osd_nodes:
            if osd_node.get('id', None) == int(osd_id):
                pgs = osd_node.get('pgs')
                if pgs != 0:
                    self.log.info(f"osd: {osd_id} still has {pgs} PGs.")
                    return False
        self.log.info(f"osd: {osd_id} has no PGs anymore")
        return True

    def reweight_osd(self, osd_id: int, weight: float = 0.0) -> bool:
        base_cmd = 'osd crush reweight'
        ret, out, err = self.mon_command({
            'prefix': base_cmd,
            'name': f'osd.{osd_id}',
            'weight': weight
        })
        cmd = f"{base_cmd} on osd.{osd_id} to weight: {weight}"
        self.log.debug(f"running cmd: {cmd}")
        if ret != 0:
            self.log.error(f"command: {cmd} failed with: {err}")
            return False
        self.log.info(f"command: {cmd} succeeded with: {out}")
        return True

    def ok_to_stop(self, osd_ids: List[int]) -> bool:
        base_cmd = "osd ok-to-stop"
        ret, out, err = self.mon_command({
            'prefix': base_cmd,
            'ids': osd_ids
        })
        self.log.debug(f"running cmd: {base_cmd} on ids {osd_ids}")
        if ret != 0:
            self.log.error(f"command: {base_cmd} failed with: {err}")
            return False
        self.log.info(f"command: {base_cmd} succeeded with: {out}")
        return True



