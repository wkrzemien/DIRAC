import unittest
import time
import mock
from mock import MagicMock
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.Core.Utilities.CFG import CFG
from DIRAC.Resources.MessageQueue.MQCommunication import createConsumer
from DIRAC.Resources.MessageQueue.MQCommunication import createProducer
import DIRAC.Resources.MessageQueue.MQCommunication as MQComm
from DIRAC.Resources.MessageQueue.StompMQConnector import StompMQConnector as MyStompConnector
from DIRAC import S_OK, S_ERROR
import DIRAC.Resources.MessageQueue.Utilities as moduleUtils
import logging
import os
import sys

# root = logging.getLogger()
# root.setLevel(logging.DEBUG)

# handler = logging.StreamHandler(sys.stdout)
# handler.setLevel(logging.DEBUG)

TEST_CONFIG = """
Resources
{
  MQServices
  {
    mardirac3.in2p3.fr
    {
      MQType = Stomp
      Host = localhost
      VHost = /
      Port = 61613
      User = guest
      Password = guest
      Queues
      {
        test1
        {
          Acknowledgement = False
        }

        test2
        {
          Acknowledgement = False
        }
      }
      Topics
      {
        test1
        {
          Acknowledgement = False
        }
      }
    }
    testdir.blabla.ch
    {
      MQType = Stomp
      VHost = /
      Host = localhost
      Port = 61613
      SSLVersion = TLSv1
      HostCertificate =
      HostKey =
      User = guest
      Password = guest
      Queues
      {
        test4
        {
          Acknowledgement = False
        }
      }
    }
  }
}
"""


def pseudoCS(mqURI):
  paramsQ1 = {
      'VHost': '/',
      'Queues': 'test1',
      'MQType': 'Stomp',
      'Host': 'localhost',
      'Port': '61613',
      'User': 'guest',
      'Password': 'guest',
      'Acknowledgement ': 'False'}
  paramsQ2 = {
      'VHost': '/',
      'Queues': 'test2',
      'MQType': 'Stomp',
      'Host': 'localhost',
      'Port': '61613',
      'User': 'guest',
      'Password': 'guest',
      'Acknowledgement ': 'False'}
  paramsT3 = {
      'VHost': '/',
      'Topics': 'test1',
      'MQType': 'Stomp',
      'Host': 'localhost',
      'Port': '61613',
      'User': 'guest',
      'Password': 'guest',
      'Acknowledgement ': 'False'}
  paramsQ4 = {
      'VHost': '/',
      'Queues': 'test4',
      'MQType': 'Stomp',
      'Host': 'localhost',
      'Port': '61613',
      'User': 'guest',
      'Password': 'guest',
      'Acknowledgement ': 'False'}
  if mqURI == 'mardirac3.in2p3.fr::Queue::test1':
    return S_OK(paramsQ1)
  elif mqURI == 'mardirac3.in2p3.fr::Queue::test2':
    return S_OK(paramsQ2)
  elif mqURI == 'mardirac3.in2p3.fr::Topic::test1':
    return S_OK(paramsT3)
  elif mqURI == 'testdir.blabla.ch::Queue::test4':
    return S_OK(paramsT3)
  else:
    return S_ERROR("Bad mqURI")


def pseudoReconnect():
  TestMQCommunication_myProducer.reconnectWasCalled = True


def pseudocreateMQConnector(parameters=None):
  obj = MyStompConnector(parameters)
  return S_OK(obj)

def stopServer():
  os.system("rabbitmqctl stop_app") 

def startServer():
  os.system("rabbitmqctl start_app") 

class TestMQCommunication(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass


class TestMQCommunication_myProducer(TestMQCommunication):

  reconnectWasCalled = False

  def setUp(self):
    MQComm.connectionManager.removeAllConnections()
    TestMQCommunication_myProducer.reconnectWasCalled = False

  def tearDown(self):
    MQComm.connectionManager.removeAllConnections()
    TestMQCommunication_myProducer.reconnectWasCalled = False

  @mock.patch('DIRAC.Resources.MessageQueue.StompMQConnector.StompMQConnector.reconnect', side_effect=pseudoReconnect)
  @mock.patch('DIRAC.Resources.MessageQueue.MQConnectionManager.createMQConnector', side_effect=pseudocreateMQConnector)
  @mock.patch('DIRAC.Resources.MessageQueue.MQCommunication.getMQParamsFromCS', side_effect=pseudoCS)
  def test_success(self, mock_getMQParamsFromCS, mock_createMQConnector, mock_reconnect):
    result = createProducer(mqURI='mardirac3.in2p3.fr::Queue::test1')
    self.assertTrue(result['OK'])
    producer = result['Value']
    result = producer.put('blabla')
    self.assertTrue(result['OK'])
    result = producer.put('blabla')
    self.assertTrue(result['OK'])
    connections = MQComm.connectionManager._MQConnectionManager__getAllConnections()
    self.assertEqual(sorted(connections), sorted(['mardirac3.in2p3.fr']))

    result = producer.close()
    self.assertTrue(result['OK'])
    self.assertFalse(TestMQCommunication_myProducer.reconnectWasCalled)
    result = producer.close()
    self.assertFalse(TestMQCommunication_myProducer.reconnectWasCalled)

    connections = MQComm.connectionManager._MQConnectionManager__getAllConnections()
    self.assertEqual(sorted(connections), sorted([]))
    result = producer._connectionManager.getAllMessengers()
    self.assertTrue(result['OK'])
    messengers = result['Value']
    expected = []
    self.assertEqual(sorted(messengers), sorted(expected))

    for i in xrange(20):
      result = createProducer(mqURI='mardirac3.in2p3.fr::Queue::test1')
      self.assertTrue(result['OK'])
      producer = result['Value']
      result = producer.put('blabla')
      self.assertTrue(result['OK'])
      result = producer.put('blabla')
      self.assertTrue(result['OK'])
      result = producer.close()
      self.assertTrue(result['OK'])
      time.sleep(1)

    self.assertFalse(TestMQCommunication_myProducer.reconnectWasCalled)
    time.sleep(20)
    self.assertFalse(TestMQCommunication_myProducer.reconnectWasCalled)

class TestMQCommunication_myProducer2(TestMQCommunication):


  def setUp(self):
    MQComm.connectionManager.removeAllConnections()

  def tearDown(self):
    MQComm.connectionManager.removeAllConnections()

  @mock.patch('DIRAC.Resources.MessageQueue.MQCommunication.getMQParamsFromCS', side_effect=pseudoCS)
  def test_success(self, mock_getMQParamsFromCS):
    result = createProducer(mqURI='mardirac3.in2p3.fr::Queue::test1')
    self.assertTrue(result['OK'])
    producer = result['Value']
    result = producer.put('blabla')
    self.assertTrue(result['OK'])
    stopServer()
    time.sleep(5)
    startServer()
    result = producer.put('blabla')
    self.assertTrue(result['OK'])

    result = producer.close()
    self.assertTrue(result['OK'])
    result = producer._connectionManager.getAllMessengers()
    self.assertTrue(result['OK'])
    messengers = result['Value']
    expected = []
    self.assertEqual(sorted(messengers), sorted(expected))

class TestMQCommunication_myProducer3(TestMQCommunication):


  def setUp(self):
    MQComm.connectionManager.removeAllConnections()

  def tearDown(self):
    MQComm.connectionManager.removeAllConnections()

  @mock.patch('DIRAC.Resources.MessageQueue.MQCommunication.getMQParamsFromCS', side_effect=pseudoCS)
  def test_success(self, mock_getMQParamsFromCS):
    result = createProducer(mqURI='mardirac3.in2p3.fr::Queue::test1')
    self.assertTrue(result['OK'])
    producer = result['Value']
    result = producer.put('blabla')
    self.assertTrue(result['OK'])
    stopServer()
    result = producer.put('blabla')
    time.sleep(5)
    startServer()

    result = producer.close()
    self.assertTrue(result['OK'])
    result = producer.connectionManager.getAllMessengers()
    self.assertTrue(result['OK'])
    messengers = result['Value']
    expected = []
    self.assertEqual(sorted(messengers), sorted(expected))

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestMQCommunication)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestMQCommunication_myProducer))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestMQCommunication_myProducer2))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestMQCommunication_myProducer3))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
