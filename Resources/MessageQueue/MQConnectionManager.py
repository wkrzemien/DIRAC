""" Class to manage connections for the Message Queue resources.
    Also, set of 'private' helper functions to access and modify the message queue connection storage.
    They are ment to be used only internally by the MQConnectionManager, which should
    assure thread-safe access to it and standard S_OK/S_ERROR error handling.
    MQConnection storage is a dict structure that contains the MQ connections used and reused for
    producer/consumer communication. Example structure:
    {
      mardirac3.in2p3.fr: {'MQConnector':StompConnector, 'destinations':{'/queue/test1':['consumer1', 'producer1'],
                                                                         '/queue/test2':['consumer1', 'producer1']}},
      blabal.cern.ch:     {'MQConnector':None,           'destinations':{'/queue/test2':['consumer2', 'producer2',]}}
    }
"""

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.LockRing import LockRing
from DIRAC.Resources.MessageQueue.Utilities import getMQService
from DIRAC.Resources.MessageQueue.Utilities import getDestinationAddress
from DIRAC.Resources.MessageQueue.Utilities import createMQConnector
import re


class MQConnectionManager(object):
  """Manages connections for the Message Queue resources in form of the interal connection storage."""
  def __init__(self, connectionStorage = None):
    self.log = gLogger.getSubLogger( self.__class__.__name__ )
    self._lock = None
    if connectionStorage:
      self._connectionStorage = connectionStorage
    else:
      self._connectionStorage = {}

  @property
  def lock( self ):
    """ Lock to assure thread-safe access to the internal connection storage.
    """
    if not self._lock:
      self._lock = LockRing().getLock( self.__class__.__name__, recursive = True )
    return self._lock

  def startConnection(self, mqURI, params, messangerType):
    """ Function adds or updates the MQ connection. If the connection
        does not exists, MQconnector is created and added.
    Args:
      mqURI(str):
      params(dict): parameters to initialize the MQConnector.
      messangerType(str): 'consumer' or 'producer'.
    Returns:
      S_OK/S_ERROR: with the value of the messanger Id in S_OK.
    """
    self.lock.acquire()
    try:
      conn = getMQService(mqURI)
      if _connectionExists(self._connectionStorage, conn):
        return self.addNewMessanger(mqURI = mqURI, messangerType = messangerType)
      else: #Connection does not exist so we create the connector and we add a new connection
        result =  self.addNewMessanger(mqURI = mqURI, messangerType = messangerType)
        if not result['OK']:
          return result
        mId = result['Value']
        result = self.createConnectorAndConnect(parameters = params)
        if not result['OK']:
          return result
        if _getConnector(self._connectionStorage, conn):
          return S_ERROR("Failed to setup connection! The connector already exists!")
        _setConnector(self._connectionStorage, conn, result['Value'])
        return S_OK(mId)
    finally:
      self.lock.release()

  def addNewMessanger(self, mqURI, messangerType):
    """ Function updates the MQ connection by adding the messanger Id to the internal connection storage.
    Args:
      mqURI(str):
      messangerType(str): 'consumer' or 'producer'.
    Returns:
      S_OK: with the value of the messanger Id or S_ERROR if the messanger was not added,
            cause the same id already exists.
    """
    # 'consumer1' ->1
    # 'producer21' ->21
    msgIdToInt = lambda msgIds, msgType : [int(m.replace(msgType,'')) for m in msgIds]
    # The messangerId is str e.g.  'consumer5' or 'producer3'
    generateMessangerId = lambda strg, conn, dest, msgT: msgT +str(max(msgIdToInt(_getMessangersIdWithType(strg, conn, dest, msgT), msgT) or [0]) + 1)
    self.lock.acquire()
    try:
      conn = getMQService(mqURI)
      dest = getDestinationAddress(mqURI)
      mId = generateMessangerId(self._connectionStorage, conn, dest, messangerType)
      if _addMessanger(cStorage = self._connectionStorage, mqConnection = conn, destination = dest, messangerId = mId):
        return S_OK(mId)
      return S_ERROR("Failed to update the connection, the messanger "+str(mId)+ "  already exists")
    finally:
      self.lock.release()

  def createConnectorAndConnect(self, parameters):
    result = createMQConnector(parameters = parameters)
    if not result['OK']:
      return result
    connector = result['Value']
    result = connector.setupConnection(parameters = parameters)
    if not result['OK']:
      return result
    result = connector.connect()
    if not result['OK']:
      return result
    return S_OK(connector)

  def disconnect(self, connector):
    if not connector:
      return S_ERROR('Failed to disconnect! Connector is None!')
    return connector.disconnect()

  def unsubscribe(self, connector, destination, messangerId):
    if not connector:
      return S_ERROR('Failed to unsubscribe! Connector is None!')
    return connector.unsubscribe(parameters = {'destination':destination, 'messangerId':messangerId})

  def getConnector(self, mqConnection):
    """ Function returns MQConnector assigned to the mqURI.
    Args:
      mqConnection(str): connection name.
    Returns:
      S_OK/S_ERROR: with the value of the MQConnector in S_OK if not None
    """
    self.lock.acquire()
    try:
      connector = _getConnector(self._connectionStorage, mqConnection)
      if not connector:
        return S_ERROR('Failed to get the MQConnector!')
      return S_OK(connector) 
    finally:
      self.lock.release()
    
  def stopConnection(self, mqURI, messangerId):
    """ Function 'stops' the connection for given messanger, which means
        it removes it from the messanger list. If this is the consumer, the
        unsubscribe() connector method is called. If it is the last messanger
        of this destination (queue or topic), then the destination is removed.
        If it is the last destination from this connection. The disconnect function
        is called and the connection is removed.
    Args:
      mqURI(str):
      messangerId(str): e.g. 'consumer1' or 'producer10'.
    Returns:
      S_OK: with the value of the messanger Id or S_ERROR if the messanger was not added,
            cause the same id already exists.
    """
    getMessangerType = lambda mId: next((mType for mType in ['consumer', 'producer'] if mType in mId), None)
    self.lock.acquire()
    try:
      conn = getMQService(mqURI)
      dest = getDestinationAddress(mqURI)
      connector = _getConnector(self._connectionStorage, conn)

      if not _removeMessanger(cStorage = self._connectionStorage, mqConnection = conn, destination = dest, messangerId = messangerId):
        return S_ERROR('Failed to stop the connection!The messanger:'+ messangerId + ' does not exists!')
      else:
        if 'consumer' in messangerId:
          result = self.unsubscribe(connector, destination = dest, messangerId = messangerId)
          if not result['OK']:
            return result
      if not _connectionExists(self._connectionStorage, conn):
        return self.disconnect(connector)
      return S_OK()
    finally:
      self.lock.release()

  def getAllMessangers(self):
    """ Function returns a list of all messangers registered in connection storage.
    Returns:
      S_OK/S_ERROR: with the list of strings in the pseudo-path format e.g.
            ['blabla.cern.ch/queue/test1/consumer1','blabal.cern.ch/topic/test2/producer2']
    """
    self.lock.acquire()
    try:
      return S_OK(_getAllMessangersInfo(cStorage = self._connectionStorage))
    finally:
      self.lock.release()

  def removeAllConnections(self):
    """ Function removes all existing connections and calls the disconnect
        for connectors.
    Returns:
      S_OK/S_ERROR:
    """
    self.lock.acquire()
    try:
      connections = _getAllConnections(cStorage = self._connectionStorage)
      for conn in connections:
        connector =_getConnector(self._connectionStorage, conn)
        if connector:
          self.disconnect(connector)
      self._connectionStorage = {}
      return S_OK()
    finally:
      self.lock.release()

# Set of 'private' helper functions to access and modify the message queue connection storage.

def _getConnection(cStorage, mqConnection):
  """ Function returns message queue connection from the storage.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
  Returns:
    dict:
  """
  return cStorage.get(mqConnection, {})

def _getAllConnections(cStorage):
  """ Function returns a list of all connection names in the storage
  Args:
    cStorage(dict): message queue connection storage.
  Returns:
    list:
  """
  return cStorage.keys()

def _getConnector(cStorage, mqConnection):
  """ Function returns MQConnector from the storage.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
  Returns:
    MQConnector or None
  """
  return _getConnection(cStorage, mqConnection).get("MQConnector", None)

def _setConnector(cStorage, mqConnection, connector):
  """ Function returns MQConnector from the storage.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
    connector(MQConnector):
  Returns:
    bool: False if connection does not exit
  """
  if not _getConnection(cStorage, mqConnection):
    return False
  _getConnection(cStorage, mqConnection)["MQConnector"] = connector
  return True

def _getDestinations(cStorage, mqConnection):
  """ Function returns dict with destinations (topics and queues) for given connection.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
  Returns:
    dict: of form {'/queue/queue1':['producer1','consumer2']} or {}
  """
  return _getConnection(cStorage, mqConnection).get("destinations", {})

def _getMessangersId(cStorage, mqConnection, mqDestination):
  """ Function returns list of messangers for given connection and given destination.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
    mqDestination(str): message queue or topic name e.g. '/queue/myQueue1' .
  Returns:
    list: of form ['producer1','consumer2'] or []
  """
  return _getDestinations(cStorage, mqConnection).get(mqDestination, [])

def _getMessangersIdWithType(cStorage, mqConnection, mqDestination, messangerType):
  """ Function returns list of messnager for given connection, destination and type.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
    mqDestination(str): message queue or topic name e.g. '/queue/myQueue1' .
    messangerType(str): 'consumer' or 'producer'
  Returns:
    list: of form ['producer1','producer2'], ['consumer8', 'consumer20'] or []
  """
  return [p for p in _getMessangersId(cStorage, mqConnection, mqDestination) if messangerType in p]

def _getAllMessangersInfo(cStorage):
  """ Function returns list of all messangers in the pseudo-path format.
  Args:
    cStorage(dict): message queue connection storage.
  Returns:
    list: of form ['blabla.cern.ch/queue/myQueue1/producer1','bibi.in2p3.fr/topic/myTopic331/consumer3'] or []
  """
  output = lambda connection,dest,messanger: str(connection)+str(dest)+'/'+ str(messanger)
  return [output(c, d, m) for c in cStorage.keys() for d in _getDestinations(cStorage,c)  for m in _getMessangersId(cStorage,c, d)]

def _connectionExists(cStorage, mqConnection):
  """ Function checks if given connection exists in the connection storage.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
  Returns:
    bool:
  """
  return mqConnection in cStorage

def _destinationExists(cStorage, mqConnection, mqDestination):
  """ Function checks if given destination(queue or topic) exists in the connection storage.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
    mqDestination(str): message queue or topic name e.g. '/queue/myQueue1' .
  Returns:
    bool:
  """
  return mqDestination in _getDestinations(cStorage, mqConnection)

def _MessangerExists(cStorage, mqConnection, mqDestination, messangerId):
  """ Function checks if given messanger(producer or consumer) exists in the connection storage.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
    mqDestination(str): message queue or topic name e.g. '/queue/myQueue1' .
    messangerId(str): messanger name e.g. 'consumer1', 'producer4' .
  Returns:
    bool:
  """
  return messangerId in _getMessangersId(cStorage, mqConnection, mqDestination)

def _addMessanger(cStorage, mqConnection, destination, messangerId):
  """ Function adds a messanger(producer or consumer) to given connection and destination.
      If connection or/and destination do not exist, they are created as well.
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
    mqDestination(str): message queue or topic name e.g. '/queue/myQueue1' .
    messangerId(str): messanger name e.g. 'consumer1', 'producer4'.
  Returns:
    bool: True if messanger is added or False if the messanger already exists.
  """
  if _MessangerExists(cStorage, mqConnection, destination, messangerId):
    return False
  if _connectionExists(cStorage,mqConnection):
    if _destinationExists(cStorage,mqConnection, destination):
      _getMessangersId(cStorage, mqConnection, destination).append(messangerId)
    else:
      _getDestinations(cStorage,mqConnection)[destination] = [messangerId]
  else:
    cStorage[mqConnection]={"MQConnector":None,"destinations":{destination:[messangerId]}}
  return True

def _removeMessanger(cStorage, mqConnection, destination, messangerId):
  """ Function removes  messanger(producer or consumer) from given connection and destination.
      If it is the last messanger in given destination and/or connection they are removed as well..
  Args:
    cStorage(dict): message queue connection storage.
    mqConnection(str): message queue connection name.
    mqDestination(str): message queue or topic name e.g. '/queue/myQueue1' .
    messangerId(str): messanger name e.g. 'consumer1', 'producer4'.
  Returns:
    bool: True if messanger is removed or False if the messanger was not in the storage.
  """
  messangers = _getMessangersId(cStorage, mqConnection, destination)
  destinations = _getDestinations(cStorage,mqConnection)
  if messangerId in messangers:
    messangers.remove(messangerId)
    if not messangers: #If no more messangers we remove the destination.
      destinations.pop(destination)
      if not destinations: #If no more destinations we remove the connection
        cStorage.pop(mqConnection)
    return True
  else:
    return False #messanger was not in the storage