#
# levelpy/leveldb_module_shims.py
#
"""
Shims for normalizing modules.
"""

import logging

print(dir(logging))
log = logging.getLogger(__name__)


def NormalizeBackend(db):
    full_classname = "%s.%s" % (db.__class__.__module__, db.__class__.__name__)

    log.debug('NormalizeBackend: %s' % (full_classname))

    # get the function to normalize
    normalizer = {
        'plyvel._plyvel.DB': plyvel_database,
    }.get(full_classname, lambda db_: None)

    log.debug('NormalizeBackend: Found normalizer %s' % (normalizer))

    # normalize the database object
    normalizer(db)


def plyvel_database(db):
    log.debug('plyvel_database: %s' % (db))
