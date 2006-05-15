"""PostgreSQL Interface definitions.

$Id$

THIS SOFTWARE IS UNDER MIT LICENSE.
(C) 2006 Perillo Manlio (manlio.perillo@gmail.com)

Read LICENSE file for more informations.
"""

from zope.interface import Interface, Attribute



# XXX choose better names
class IFastPath(Interface):
    """The Fast Path Interface to server functions.
    """

    def fn(fnid, fformat, *args):
        """Execute the function, given its oid.
        """

#     transactionStatus = Attribute(
#         "The status of the current transaction"
#         )

class IHandler(Interface):
    """Handler for asyncronous messages.
    """

    def notice(notice):
        """Handle a notice.
        """

    def notify(notify):
        """Handle a notification.
        """

class IConsumer(Interface):
    """A file like object, capable of reading/consume data.
    """

    def write(data):
        """Read data.
        """

    def close():
        """We have no more data to give to you.
        """

class IProducer(Interface):
    """A file object, capable of writing/producing data.
    """

    def read():
        """Request some data.
        
        Return the empty string when no more data is available.
        """

class IRowConsumer(Interface):
    """An object that handles row data.

    Note that the data given is the raw data as returned by the
    backend.
    It is responsibility to this interface to do parsing (thus it can
    be optimized).
    """

    def description(data):
        """Hanle the row description.
        """

    def row(data):
        """Handle the row data.
        """

    def complete(data):
        """The command has complete, no more data.

        Return an object implementing the IPgResult interface, with
        the result of the query.
        """


class IRowDescription(Interface):
    """A row description.
    """

    ntuples = Attribute(
        "The number of rows (tuples) in the query result"
        )
    nfields = Attribute(
        "The number of columns (fields) in each row in the" \
        "query result"
        )
    fnames = Attribute("Columns name (a list)")
#    fnumber = Attribute("
#    ftable = Attribute("the OID of the table from witch
#    ftablecol
#    fformat
#    ftype
#    fmod
#    fsize
#    binaryTuples

    
class IPgResult(Interface):
    """The result of a query.
    """

    description = Attribute("An object implementing IRowDescription")
    status = Attribute("The result status of the command")
    
    rows = Attribute(
        """A list of list, containig the rows.
        The implementation can choose to return raw strings, or Python
        objects
        """
        )


#
# Large Objects support
# (implementations in fefs.py)
#
class ILargeObjectFactory(Interface):
    """How to crate or open a large object on the database.

    XXX TODO.

    To obtain a large object, do:
    protocol = ...

    loFactory = ILargeObjectFactory(protocol)
    lo = loFactory.creat(...)
    or
    lo = loFactory.open(...)
    """

class ILargeObject(Interface):
    """A large object.

    XXX TODO.
    """
