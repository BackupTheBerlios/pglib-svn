"""Interface definitions for pglib.

$Id$

THIS SOFTWARE IS UNDER MIT LICENSE.
Copyright (c) 2006 Perillo Manlio (manlio.perillo@gmail.com)

Read LICENSE file for more informations.
"""

from zope.interface import Interface, Attribute



class IFastPath(Interface):
    """The Fast-Path Interface to server functions.
    """

    def fn(fnid, fformat, *args):
        """Execute the function, given its oid.

        @param fnid: is the OID of the function to be executed
        @type fnid: int
        
        @param fformat: the type format; can be 0 (text) or 1 (binary)
        @type fformat: int
        
        @note: Arguments must be strings.
        
        @return: a deferred that will fire with the result of the
                 function, as a C{str}
        @rtype: L{twisted.internet.defer.Deferred}
        
        @todo: In the current implementation all
               arguments (and the return value) are forced to be of
               the same format.

        """

#     transactionStatus = Attribute(
#         "The status of the current transaction"
#         )

class IHandler(Interface):
    """Handler for asyncronous messages.
    """

    def notice(notice):
        """Handle a notice.

        @param notice: the notice
        @type notice: dict
        """

    def notify(notify):
        """Handle a notification.

        @param notice: the notification
        @type notice: L{pglib.protocol.Notification}
        """

class IConsumer(Interface):
    """A file like object, capable of reading/consume data.
    """

    def description(ntuples, binaryTuples):
        """Description.

        @param ntuples: the number of columns in the data to be
                        copied
        @type ntuples: int
        
        @param binaryTuples: 0 when the overall COPY format is textual,
                             1 when binary
        @type binaryTuples: int
        
        @todo: In the current implementation, we ignore format codes.
               All columns have the same format.
               This feature is supported in the protocol but not by
               the backend.
        """
        
    def write(data):
        """Read/Consume data.

        @param data: the data sent by the backend
        @type data: str

        @note: The backend always send one row at a time.
        """

    def close():
        """COPY transfer completed.

        @return: an object implementing the L{pglib.ipg.IResult}
                 interface
        """

class IProducer(Interface):
    """A file like object, capable of writing/producing data.
    """

    def description(ntuples, binaryTuples):
        """Description.
        
        @param ntuples: the number of columns in the data to be copied.
        @type ntuples: int
        
        @param binaryTuples: 0 when the overall COPY format is textual,
                             1 when binary.
        @type binaryTuples: int
        
        @todo: In the current implementation, we ignore firmat codes.
               All columns have the same format.
               This feature is supported in the protocol but not by
               the backend.
        """
    
    def read():
        """Request/Produce some data.
        
        @return: the data to be copied in the backend.
                 Return the empty string when no more data is available.

        @note: It is not required to send one row at a time.
        """

    def close():
        """COPY transfer completed.

        @return: an object implementing the L{pglib.ipg.IResult}
                 interface
        """

class IRowConsumer(Interface):
    """An object that handles row data.

    @note: the data given is the raw data as returned by the
           backend.
           It is the responsibility to this interface to do parsing 
           (thus it can be optimized).
    """

    def description(data):
        """Handle the row description.

        This method will be called only if the command returns rows
        data.

        @param data: the (raw) data containing the description of the
                     result set. 
                     Read the protocol specification for more info.
        @type data: str
        """

    def row(data):
        """Handle the row data.

        @param data: the (raw) data containing a row from a result
                     set. 
                     Read the protocol specification for more info. 
        @type data: str
        """

    # cmdStatus, oidValue, cmdTuples
    def complete(status, oid, rows):
        """The command has complete, no more data.

        @param status: the command status flag (usually the name of the
                       command)
        @type status: str
        
        @param oid: the OID of the inserted row, if available
        @type oid: int
        
        @param rows: the number of rows affected by the command
        @type rows: int
        
        @note: for commands that return no rows, this will be the
               only method to be called.
        
        @return: an object implementing the L{pglib.ipg.IResult}
                 interface, with the result of the query.
        """


class IRowDescription(Interface):
    """A row description.
    """

    fname = Attribute("The field name")
    ftable = Attribute(
        """The OID of the table, if the field can be identified as a
        column, 0 otherwise"""
        )
    ftablecol = Attribute(
        """The attribute number of the column, if the field can be
        identified as a column, 0 otherwise"""
        )
    ftype = Attribute("The object ID of the field's data type")
    fsize = Attribute(
        """"The data type size. Negative values denotes
        variable-width types"""
        )
    fmod = Attribute("The type modifier")
    fformat = Attribute(
        """The format code being used for the field.
        
        In the current implementation the format is the same for all
        columns, so you can safely check the binaryTuples attribute of
        the IResult"""
        )
    
class IResult(Interface):
    """The result of a query.
    """
    
    # XXX these two are not really useful
    ntuples = Attribute(
        "The number of rows (tuples) in the query result"
        )
    nfields = Attribute(
        """The number of columns (fields) in each row in the query
        result"""
        )
    binaryTuples = Attribute(
        """This is 1 if all columns are in binary format, 0 for
        text"""
        )
    
    descriptions = Attribute(
        "A list of objects implementing IRowDescription"
        )
    
    status = Attribute("The status of the SQL command")
    cmdStatus = Attribute(
        "The command status tag (usually the name of the command)"
        )
    cmdTuples = Attribute(
        "The number of the rows affected by the SQL command"
        )
    oidValue = Attribute("The OID of the inserted row, if available")
    
    rows = Attribute(
        """A list of list, containig the rows.
        The implementation can choose to return raw strings, or Python
        objects
        """
        )


#
# Large Objects support
# (implementation in fefs.py, or fe.py)
#
class ILargeObjectFactory(Interface):
    """How to create or open a large object on the database.

    To obtain a large object, do:

      >>> protocol = ...
      >>> loFactory = ILargeObjectFactory(protocol)
      >>> lo = loFactory.creat(...)

    or
    
      >>> lo = loFactory.open(...)

    @todo: 
    """

class ILargeObject(Interface):
    """A large object.

    @todo: 
    """
