import json
import errno
import logging
from threading import Event

import cephfs

from .fs_util import listdir

from .operations.volume import ConnectionPool, open_volume, create_volume, \
    delete_volume, list_volumes
from .operations.group import open_group, create_group, remove_group
from .operations.subvolume import open_subvol, create_subvol, remove_subvol

from .vol_spec import VolSpec
from .exception import VolumeException
from .purge_queue import ThreadPoolPurgeQueueMixin

log = logging.getLogger(__name__)

def octal_str_to_decimal_int(mode):
    try:
        return int(mode, 8)
    except ValueError:
        raise VolumeException(-errno.EINVAL, "Invalid mode '{0}'".format(mode))

def name_to_json(names):
    """
    convert the list of names to json
    """
    namedict = []
    for i in range(len(names)):
        namedict.append({'name': names[i].decode('utf-8')})
    return json.dumps(namedict, indent=4, sort_keys=True)

class VolumeClient(object):
    def __init__(self, mgr):
        self.mgr = mgr
        self.stopping = Event()
        # volume specification
        self.volspec = VolSpec(mgr.rados.conf_get('client_snapdir'))
        self.connection_pool = ConnectionPool(self.mgr)
        # TODO: make thread pool size configurable
        self.purge_queue = ThreadPoolPurgeQueueMixin(self, 4)
        # on startup, queue purge job for available volumes to kickstart
        # purge for leftover subvolume entries in trash. note that, if the
        # trash directory does not exist or if there are no purge entries
        # available for a volume, the volume is removed from the purge
        # job list.
        fs_map = self.mgr.get('fs_map')
        for fs in fs_map['filesystems']:
            self.purge_queue.queue_purge_job(fs['mdsmap']['fs_name'])

    def is_stopping(self):
        return self.stopping.isSet()

    def shutdown(self):
        log.info("shutting down")
        # first, note that we're shutting down
        self.stopping.set()
        # second, ask purge threads to quit
        self.purge_queue.cancel_all_jobs()
        # third, delete all libcephfs handles from connection pool
        self.connection_pool.del_all_handles()

    def cluster_log(self, msg, lvl=None):
        """
        log to cluster log with default log level as WARN.
        """
        if not lvl:
            lvl = self.mgr.CLUSTER_LOG_PRIO_WARN
        self.mgr.cluster_log("cluster", lvl, msg)

    def volume_exception_to_retval(self, ve):
        """
        return a tuple representation from a volume exception
        """
        return ve.to_tuple()

    ### volume operations -- create, rm, ls

    def create_fs_volume(self, volname):
        if self.is_stopping():
            return -errno.ESHUTDOWN, "", "shutdown in progress"
        return create_volume(self.mgr, volname)

    def delete_fs_volume(self, volname, confirm):
        if self.is_stopping():
            return -errno.ESHUTDOWN, "", "shutdown in progress"

        if confirm != "--yes-i-really-mean-it":
            return -errno.EPERM, "", "WARNING: this will *PERMANENTLY DESTROY* all data " \
                "stored in the filesystem '{0}'. If you are *ABSOLUTELY CERTAIN* " \
                "that is what you want, re-issue the command followed by " \
                "--yes-i-really-mean-it.".format(volname)

        self.purge_queue.cancel_purge_job(volname)
        self.connection_pool.del_fs_handle(volname, wait=True)
        return delete_volume(self.mgr, volname)

    def list_fs_volumes(self):
        if self.stopping.isSet():
            return -errno.ESHUTDOWN, "", "shutdown in progress"
        volumes = list_volumes(self.mgr)
        return 0, json.dumps(volumes, indent=4, sort_keys=True), ""

    ### subvolume operations

    def _create_subvolume(self, fs_handle, volname, group, subvolname, **kwargs):
        size       = kwargs['size']
        pool       = kwargs['pool_layout']
        uid        = kwargs['uid']
        gid        = kwargs['gid']
        mode       = kwargs['mode']

        oct_mode = octal_str_to_decimal_int(mode)
        try:
            create_subvol(
                fs_handle, self.volspec, group, subvolname, size, False, pool, oct_mode, uid, gid)
        except VolumeException as ve:
            # kick the purge threads for async removal -- note that this
            # assumes that the subvolume is moved to trashcan for cleanup on error.
            self.purge_queue.queue_purge_job(volname)
            raise ve

    def create_subvolume(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    try:
                        with open_subvol(fs_handle, self.volspec, group, subvolname):
                            # idempotent creation -- valid.
                            pass
                    except VolumeException as ve:
                        if ve.errno == -errno.ENOENT:
                            self._create_subvolume(fs_handle, volname, group, subvolname, **kwargs)
                        else:
                            raise
        except VolumeException as ve:
            # volume/group does not exist or subvolume creation failed
            ret = self.volume_exception_to_retval(ve)
        return ret

    def remove_subvolume(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']
        force      = kwargs['force']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    remove_subvol(fs_handle, self.volspec, group, subvolname)
                    # kick the purge threads for async removal -- note that this
                    # assumes that the subvolume is moved to trash can.
                    # TODO: make purge queue as singleton so that trash can kicks
                    # the purge threads on dump.
                    self.purge_queue.queue_purge_job(volname)
        except VolumeException as ve:
            if not (ve.errno == -errno.ENOENT and force):
                ret = self.volume_exception_to_retval(ve)
        return ret

    def resize_subvolume(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        newsize    = kwargs['new_size']
        noshrink   = kwargs['no_shrink']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(fs_handle, self.volspec, group, subvolname) as subvolume:
                        nsize, usedbytes = subvolume.resize(newsize, noshrink)
                        ret = 0, json.dumps(
                            [{'bytes_used': usedbytes},{'bytes_quota': nsize},
                             {'bytes_pcent': "undefined" if nsize == 0 else '{0:.2f}'.format((float(usedbytes) / nsize) * 100.0)}],
                            indent=4, sort_keys=True), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def subvolume_getpath(self, **kwargs):
        ret        = None
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(fs_handle, self.volspec, group, subvolname) as subvolume:
                        subvolpath = subvolume.path
                        ret = 0, subvolpath.decode("utf-8"), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def list_subvolumes(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    subvolumes = group.list_subvolumes()
                    ret = 0, name_to_json(subvolumes), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    ### subvolume snapshot

    def create_subvolume_snapshot(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        snapname   = kwargs['snap_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(fs_handle, self.volspec, group, subvolname) as subvolume:
                        subvolume.create_snapshot(snapname)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def remove_subvolume_snapshot(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        snapname   = kwargs['snap_name']
        groupname  = kwargs['group_name']
        force      = kwargs['force']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(fs_handle, self.volspec, group, subvolname) as subvolume:
                        subvolume.remove_snapshot(snapname)
        except VolumeException as ve:
            if not (ve.errno == -errno.ENOENT and force):
                ret = self.volume_exception_to_retval(ve)
        return ret

    def list_subvolume_snapshots(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(fs_handle, self.volspec, group, subvolname) as subvolume:
                        snapshots = subvolume.list_snapshots()
                        ret = 0, name_to_json(snapshots), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    ### group operations

    def create_subvolume_group(self, **kwargs):
        ret       = 0, "", ""
        volname    = kwargs['vol_name']
        groupname = kwargs['group_name']
        pool      = kwargs['pool_layout']
        uid       = kwargs['uid']
        gid       = kwargs['gid']
        mode      = kwargs['mode']

        try:
            with open_volume(self, volname) as fs_handle:
                try:
                    with open_group(fs_handle, self.volspec, groupname):
                        # idempotent creation -- valid.
                        pass
                except VolumeException as ve:
                    if ve.errno == -errno.ENOENT:
                        oct_mode = octal_str_to_decimal_int(mode)
                        create_group(fs_handle, self.volspec, groupname, pool, oct_mode, uid, gid)
                    else:
                        raise
        except VolumeException as ve:
            # volume does not exist or subvolume group creation failed
            ret = self.volume_exception_to_retval(ve)
        return ret

    def remove_subvolume_group(self, **kwargs):
        ret       = 0, "", ""
        volname    = kwargs['vol_name']
        groupname = kwargs['group_name']
        force     = kwargs['force']

        try:
            with open_volume(self, volname) as fs_handle:
                remove_group(fs_handle, self.volspec, groupname)
        except VolumeException as ve:
            if not (ve.errno == -errno.ENOENT and force):
                ret = self.volume_exception_to_retval(ve)
        return ret

    def getpath_subvolume_group(self, **kwargs):
        volname    = kwargs['vol_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    return 0, group.path.decode('utf-8'), ""
        except VolumeException as ve:
            return self.volume_exception_to_retval(ve)

    def list_subvolume_groups(self, **kwargs):
        volname = kwargs['vol_name']
        ret     = 0, '[]', ""
        try:
            with open_volume(self, volname) as fs_handle:
                groups = listdir(fs_handle, self.volspec.base_dir)
                ret = 0, name_to_json(groups), ""
        except VolumeException as ve:
            if not ve.errno == -errno.ENOENT:
                ret = self.volume_exception_to_retval(ve)
        return ret

    ### group snapshot

    def create_subvolume_group_snapshot(self, **kwargs):
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        groupname = kwargs['group_name']
        snapname  = kwargs['snap_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    group.create_snapshot(snapname)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def remove_subvolume_group_snapshot(self, **kwargs):
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        groupname = kwargs['group_name']
        snapname  = kwargs['snap_name']
        force     = kwargs['force']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    group.remove_snapshot(snapname)
        except VolumeException as ve:
            if not (ve.errno == -errno.ENOENT and force):
                ret = self.volume_exception_to_retval(ve)
        return ret

    def list_subvolume_group_snapshots(self, **kwargs):
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        groupname = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    snapshots = group.list_snapshots()
                    ret = 0, name_to_json(snapshots), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret
