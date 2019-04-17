"""Unit tests of MQCommunication interface in the DIRAC.Resources.MessageQueue.MQCommunication
"""

import logging
import sys
import unittest
import time
import mock
from mock import MagicMock
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.Core.Utilities.CFG import CFG
from DIRAC.Resources.MessageQueue.MQCommunication import createConsumer
from DIRAC.Resources.MessageQueue.MQCommunication import createProducer
import DIRAC.Resources.MessageQueue.MQCommunication as MQComm
from DIRAC import S_OK, S_ERROR
import DIRAC.Resources.MessageQueue.Utilities as moduleUtils

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


class TestMQCommunication(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass


class TestMQCommunication_myProducer(TestMQCommunication):
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
    result = producer.put('blable')
    result = createProducer(mqURI='mardirac3.in2p3.fr::Queue::test2')
    self.assertTrue(result['OK'])
    producer2 = result['Value']
    result = producer2.put('blabla2')
    self.assertTrue(result['OK'])
    result = producer2.put('blable2')
    self.assertTrue(result['OK'])
    result = createProducer(mqURI='testdir.blabla.ch::Queue::test4')
    self.assertTrue(result['OK'])
    producer3 = result['Value']
    result = producer3.put('blabla3')
    self.assertTrue(result['OK'])
    result = producer3.put('blable3')
    self.assertTrue(result['OK'])

    result = producer._connectionManager.getAllMessengers()
    self.assertTrue(result['OK'])
    messengers = result['Value']
    expected = [
        'mardirac3.in2p3.fr/queue/test1/producer1',
        'mardirac3.in2p3.fr/queue/test2/producer2',
        'testdir.blabla.ch/queue/test4/producer3']
    self.assertEqual(sorted(messengers), sorted(expected))

    result = producer2.close()
    self.assertTrue(result['OK'])
    result = producer3.close()
    self.assertTrue(result['OK'])

    result = producer._connectionManager.getAllMessengers()
    self.assertTrue(result['OK'])
    messengers = result['Value']
    expected = ['mardirac3.in2p3.fr/queue/test1/producer1']
    self.assertEqual(sorted(messengers), sorted(expected))

    result = producer.close()
    self.assertTrue(result['OK'])

    result = producer._connectionManager.getAllMessengers()
    self.assertTrue(result['OK'])
    messengers = result['Value']
    expected = []
    self.assertEqual(sorted(messengers), sorted(expected))


class TestMQCommunication_myConsumer(TestMQCommunication):
  def setUp(self):
    MQComm.connectionManager.removeAllConnections()

  def tearDown(self):
    MQComm.connectionManager.removeAllConnections()

  @mock.patch('DIRAC.Resources.MessageQueue.MQCommunication.getMQParamsFromCS', side_effect=pseudoCS)
  def test_success(self, mock_getMQParamsFromCS):
    consumer = createConsumer(mqURI='mardirac3.in2p3.fr::Queue::test1')['Value']
    producer = createProducer(mqURI='mardirac3.in2p3.fr::Queue::test1')['Value']
    result = producer.put('blabla2')
    self.assertTrue(result['OK'])
    result = producer.put('blable3')
    self.assertTrue(result['OK'])

    result = producer._connectionManager.getAllMessengers()
    self.assertTrue(result['OK'])
    messengers = result['Value']
    expected = ['mardirac3.in2p3.fr/queue/test1/producer1', 'mardirac3.in2p3.fr/queue/test1/consumer1']
    self.assertEqual(sorted(messengers), sorted(expected))

    time.sleep(5)  # because in connect method there is 1 second sleep introduced
    result = consumer.get()
    self.assertTrue(result['OK'])
    time.sleep(30)

    result = consumer.get()
    self.assertTrue(result['OK'])

    result = producer.put('blable4')
    self.assertTrue(result['OK'])
    time.sleep(2)  # necause in connect method there is 1 second sleep introduced
    result = consumer.get()
    self.assertTrue(result['OK'])

    result = consumer.close()
    self.assertTrue(result['OK'])
    result = producer.close()
    self.assertTrue(result['OK'])

    result = producer._connectionManager.getAllMessengers()
    self.assertTrue(result['OK'])
    messengers = result['Value']
    expected = []
    self.assertEqual(sorted(messengers), sorted(expected))

    result = producer.close()
    self.assertFalse(result['OK'])


class TestMQCommunication_myConsumer2(TestMQCommunication):

  def setUp(self):
    MQComm.connectionManager.removeAllConnections()

  def tearDown(self):
    MQComm.connectionManager.removeAllConnections()

  @mock.patch('DIRAC.Resources.MessageQueue.MQCommunication.getMQParamsFromCS', side_effect=pseudoCS)
  def test_success(self, mock_getMQParamsFromCS):
    result = createConsumer(mqURI='mardirac3.in2p3.fr::Queue::test1')
    self.assertTrue(result['OK'])
    consumer = result['Value']

    result = createConsumer(mqURI='mardirac3.in2p3.fr::Queue::test1')
    self.assertTrue(result['OK'])
    consumer2 = result['Value']

    result = consumer._connectionManager.getAllMessengers()
    self.assertTrue(result['OK'])
    messengers = result['Value']
    expected = ['mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2']
    self.assertEqual(sorted(messengers), sorted(expected))

    result = createProducer(mqURI='mardirac3.in2p3.fr::Queue::test1')
    self.assertTrue(result['OK'])
    producer = result['Value']

    result = producer.put('blabla')
    self.assertTrue(result['OK'])
    time.sleep(2)

    result = createConsumer(mqURI='mardirac3.in2p3.fr::Queue::test2')
    self.assertTrue(result['OK'])
    consumer3 = result['Value']

    result = consumer._connectionManager.getAllMessengers()
    self.assertTrue(result['OK'])
    messengers = result['Value']
    expected = [
        'mardirac3.in2p3.fr/queue/test1/consumer1',
        'mardirac3.in2p3.fr/queue/test1/consumer2',
        'mardirac3.in2p3.fr/queue/test1/producer1',
        'mardirac3.in2p3.fr/queue/test2/consumer3']
    self.assertEqual(sorted(messengers), sorted(expected))

    result = createProducer(mqURI='mardirac3.in2p3.fr::Queue::test2')
    self.assertTrue(result['OK'])
    producer2 = result['Value']

    result = producer2.put('blabla2')
    self.assertTrue(result['OK'])

    result = consumer.close()
    self.assertTrue(result['OK'])
    result = consumer2.close()
    self.assertTrue(result['OK'])
    time.sleep(4)
    time.sleep(30)

    recMsgs = []
    while True:
      result = consumer3.get()
      if result['OK']:
        recMsgs.append(result['Value'])
      else:
        break
    self.assertTrue(set(['blabla', 'blabla2']) <= set(recMsgs))

    result = consumer3.close()
    self.assertTrue(result['OK'])

    producer.close()
    producer2.close()


class TestMQCommunication_myConsumer3(TestMQCommunication):

  def setUp(self):
    MQComm.connectionManager.removeAllConnections()

  def tearDown(self):
    MQComm.connectionManager.removeAllConnections()

  @mock.patch('DIRAC.Resources.MessageQueue.MQCommunication.getMQParamsFromCS', side_effect=pseudoCS)
  def test_success(self, mock_getMQParamsFromCS):
    result = createConsumer(mqURI='mardirac3.in2p3.fr::Queue::test1')
    self.assertTrue(result['OK'])
    consumer = result['Value']

    result = createConsumer(mqURI='mardirac3.in2p3.fr::Queue::test2')
    self.assertTrue(result['OK'])
    consumer3 = result['Value']

    time.sleep(1)

    result = consumer._connectionManager.getAllMessengers()
    self.assertTrue(result['OK'])
    messengers = result['Value']
    expected = ['mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2']
    self.assertEqual(sorted(messengers), sorted(expected))


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

    time.sleep(30)
    # now we shutdown the server

    result = producer.put('blabla')
    self.assertTrue(result['OK'])

    result = producer._connectionManager.getAllMessengers()
    self.assertTrue(result['OK'])
    messengers = result['Value']
    expected = ['mardirac3.in2p3.fr/queue/test1/producer1']

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

    time.sleep(30)
    # now we shutdown the server

    result = producer.put('blabla')
    self.assertTrue(result['OK'])

    result = producer._connectionManager.getAllMessengers()
    self.assertTrue(result['OK'])
    messengers = result['Value']
    expected = ['mardirac3.in2p3.fr/queue/test1/producer1']

    result = producer.close()
    self.assertTrue(result['OK'])

    result = producer._connectionManager.getAllMessengers()
    self.assertTrue(result['OK'])
    messengers = result['Value']
    expected = []
    self.assertEqual(sorted(messengers), sorted(expected))

class TestMQCommunication_myProducer4(TestMQCommunication):
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
    result = producer._connectionManager.getAllMessengers()
    self.assertTrue(result['OK'])
    messengers = result['Value']
    expected = ['mardirac3.in2p3.fr/queue/test1/producer1']

    result = producer.close()
    self.assertTrue(result['OK'])
    result = producer._connectionManager.getAllMessengers()
    self.assertTrue(result['OK'])
    messengers = result['Value']
    expected = []
    self.assertEqual(sorted(messengers), sorted(expected))

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestMQCommunication)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestMQCommunication_myConsumer))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestMQCommunication_myConsumer2))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestMQCommunication_myConsumer3))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestMQCommunication_myProducer2))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestMQCommunication_myProducer4))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
