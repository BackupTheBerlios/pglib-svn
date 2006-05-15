"""PostgreSQL High level frontend interface

$Id$

THIS SOFTWARE IS UNDER MIT LICENSE.
(C) 2006 Perillo Manlio (manlio.perillo@gmail.com)

Read LICENSE file for more informations.
"""


class PgConnectionOption(object):
    """Class for storing connection options.

    XXX TODO
    """

    def __init__(self, **kwargs):
        self.keywords = kwargs
        self.envvars = {}
        self.compiled = {}
        self.options = {}


def connect(host, port=5432):
    """Connect to a PostgreSQL database, returns a deferred.

    host can be a TCP address or Unix domain socket... see libpq.

    XXX TODO
    """

    pass
