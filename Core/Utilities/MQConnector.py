class MQConnector( object ):
  def __init__( self ):
    """ Function must be overriden in the implementation
    """
    raise NotImplementedError('That should be implemented')

  def connectBlocking( self, system, queueName, receive = False, messageCallback = None ):
    """ Function must be overriden in the implementation
    """
    raise NotImplementedError('That should be implemented')
  
