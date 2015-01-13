# -*- coding: utf-8 -*-
#
# Copyright 2015 Rackspace
# All Rights Reserved
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import mock
import socket
import unittest

import metricslogging


class MockedMetricsLogger(metricslogging.MetricsLogger):
    _format_name = mock.Mock(return_value="mocked_format_name")
    _gauge = mock.Mock()
    _counter = mock.Mock()
    _timer = mock.Mock()


class TestNestedConfig(unittest.TestCase):
    def setUp(self):
        super(TestNestedConfig, self).setUp()

        self.parent_config = metricslogging.NestedConfig()
        self.child_config = metricslogging.NestedConfig(parent=self.parent_config)

        self.parentSetConfig, self.parentGetConfig = self.parent_config.add_config("config")
        self.parentSetConfigDefault, self.parentGetConfigDefault = self.parent_config.add_config("configdefault", default="default")

        self.childSetConfig, self.childGetConfig = self.child_config.add_config("config", override=True)

    def test_add_config(self):
        pass


class TestMetricsLogger(unittest.TestCase):
    def setUp(self):
        super(TestMetricsLogger, self).setUp()
        metricslogging.setGlobalPrefix("globalprefix")
        metricslogging.setStatsdDelimiter(".")
        metricslogging.setHost("host.example.com")
        metricslogging.setPrependHost(True)
        metricslogging.setPrependHostReverse(True)

        self.ml = MockedMetricsLogger()
        self.ml.setPrefix("testprefix")

    def test_format_name_prepend_host_no_reverse(self):
        self.ml._format_name.reset_mock()

        self.ml.setPrependHost(True)
        self.ml.setPrependHostReverse(False)

        self.ml.format_name("metric")
        self.ml._format_name.assert_called_once_with("globalprefix", ["host", "example", "com"], "testprefix", "metric")

    def test_format_name_prepend_host_reverse(self):
        self.ml._format_name.reset_mock()

        self.ml.setPrependHost(True)
        self.ml.setPrependHostReverse(True)

        self.ml.format_name("metric")
        self.ml._format_name.assert_called_once_with("globalprefix", ["com", "example", "host"], "testprefix", "metric")

    def test_format_name_no_prepend_host(self):
        self.ml._format_name.reset_mock()

        self.ml.setPrependHost(False)

        self.ml.format_name("metric")
        self.ml._format_name.assert_called_once_with("globalprefix", [], "testprefix", "metric")

    def test_gauge(self):
        self.ml.gauge("metric", 10)
        self.ml._gauge.assert_called_once_with("mocked_format_name", 10)

    def test_counter(self):
        self.ml.counter("metric", 10)
        self.ml._counter.assert_called_once_with(
            "mocked_format_name", 10,
            sample_rate=None)
        self.ml._counter.reset_mock()

        # TODO(Alex Weeks): Verify that sample_rates != 1.0 result in
        # probabilistic behavior as expected
        self.ml.counter("metric", 10, sample_rate=1.0)
        self.ml._counter.assert_called_once_with(
            "mocked_format_name", 10,
            sample_rate=1.0)
        self.ml._counter.reset_mock()

        self.ml.counter("metric", 10, sample_rate=0.0)
        self.assertFalse(self.ml._counter.called)

        self.assertRaises(ValueError, self.ml.counter,
            "metric", 10, sample_rate=-0.1)
        self.assertRaises(ValueError, self.ml.counter,
            "metric", 10, sample_rate=1.1)

    def test_timer(self):
        self.ml.timer("metric", 10)
        self.ml._timer.assert_called_once_with("mocked_format_name", 10)

    @mock.patch("metricslogging.metricslogging._time")
    @mock.patch("metricslogging.metricslogging.MetricsLogger.timer")
    def test_time_fn(self, mock_timer, mock_time):
        mock_time.side_effect=[1, 43]

        @self.ml.time_fn("foo", "bar", "baz")
        def func(x):
            return x * x

        func(10)
        mock_timer.assert_called_once_with(("foo", "bar", "baz"), 42*1000)


class TestStatsdMetricsLogger(unittest.TestCase):
    def setUp(self):
        super(TestStatsdMetricsLogger, self).setUp()
        self.ml = metricslogging.StatsdMetricsLogger()
        self.ml.setStatsdDelimiter(".")
        self.ml.setStatsdHost("testhost")
        self.ml.setStatsdPort(4321)

    def test__format_name(self):
        self.assertEqual(
            self.ml._format_name("globalprefix", "testhost", "testprefix", "testmetric"),
            "globalprefix.testhost.testprefix.testmetric")

    def test__format_name_with_lists(self):
        self.assertEqual(
            self.ml._format_name(["global", "prefix"], "testhost", "testprefix", "testmetric"),
            "global.prefix.testhost.testprefix.testmetric")
        self.assertEqual(
            self.ml._format_name("globalprefix", ["test", "host"], "testprefix", "testmetric"),
            "globalprefix.test.host.testprefix.testmetric")
        self.assertEqual(
            self.ml._format_name("globalprefix", "testhost", ["test", "prefix"], "testmetric"),
            "globalprefix.testhost.test.prefix.testmetric")
        self.assertEqual(
            self.ml._format_name("globalprefix", "testhost", "testprefix", ["test", "metric"]),
            "globalprefix.testhost.testprefix.test.metric")

    def test__format_name_with_empty_lists(self):
        self.assertEqual(
            self.ml._format_name([], "testhost", "testprefix", "testmetric"),
            "testhost.testprefix.testmetric")
        self.assertEqual(
            self.ml._format_name("globalprefix", [], "testprefix", "testmetric"),
            "globalprefix.testprefix.testmetric")
        self.assertEqual(
            self.ml._format_name("globalprefix", "testhost", [], "testmetric"),
            "globalprefix.testhost.testmetric")
        self.assertEqual(
            self.ml._format_name("globalprefix", "testhost", "testprefix", []),
            "globalprefix.testhost.testprefix")

    def test__format_name_with_empty_strings(self):
        self.assertEqual(
            self.ml._format_name("", "testhost", "testprefix", "testmetric"),
            "testhost.testprefix.testmetric")
        self.assertEqual(
            self.ml._format_name("globalprefix", "", "testprefix", "testmetric"),
            "globalprefix.testprefix.testmetric")
        self.assertEqual(
            self.ml._format_name("globalprefix", "testhost", "", "testmetric"),
            "globalprefix.testhost.testmetric")
        self.assertEqual(
            self.ml._format_name("globalprefix", "testhost", "testprefix", ""),
            "globalprefix.testhost.testprefix")

    @mock.patch("metricslogging.metricslogging.StatsdMetricsLogger._send")
    def test_gauge(self, mock_send):
        self.ml._gauge("metric", 10)
        mock_send.assert_called_once_with("metric", 10, "g")

    @mock.patch("metricslogging.metricslogging.StatsdMetricsLogger._send")
    def test__counter(self, mock_send):
        self.ml._counter("metric", 10)
        mock_send.assert_called_once_with("metric", 10, "c", sample_rate=None)
        mock_send.reset_mock()

        self.ml._counter("metric", 10, sample_rate=1.0)
        mock_send.assert_called_once_with("metric", 10, "c", sample_rate=1.0)

    @mock.patch("metricslogging.metricslogging.StatsdMetricsLogger._send")
    def test__timer(self, mock_send):
        self.ml._timer("metric", 10)
        mock_send.assert_called_once_with("metric", 10, "ms")


    @mock.patch("socket.socket")
    def test__open_socket(self, mock_socket_constructor):
        self.ml._open_socket()
        mock_socket_constructor.assert_called_once_with(
            socket.AF_INET,
            socket.SOCK_DGRAM)

    @mock.patch("socket.socket")
    def test__send(self, mock_socket_constructor):
        mock_socket = mock.Mock()
        mock_socket_constructor.return_value = mock_socket

        self.ml._send("metric", 2, "type")
        mock_socket.sendto.assert_called_once_with(
            "metric:2|type",
            ("testhost", 4321))
        mock_socket.close.assert_called()
        mock_socket.reset_mock()

        self.ml._send("metric", 3.14159, "type")
        mock_socket.sendto.assert_called_once_with(
            "metric:3.14159|type",
            ("testhost", 4321))
        mock_socket.close.assert_called()
        mock_socket.reset_mock()

        self.ml._send("metric", 5, "type")
        mock_socket.sendto.assert_called_once_with(
            "metric:5|type",
            ("testhost", 4321))
        mock_socket.close.assert_called()
        mock_socket.reset_mock()

        self.ml._send("metric", 5, "type", sample_rate=0.5)
        mock_socket.sendto.assert_called_once_with(
            "metric:5|type@0.5",
            ("testhost", 4321))
        mock_socket.close.assert_called()

    @mock.patch("socket.socket")
    def test__send_prohibited_chars(self, mock_socket_constructor):
        mock_socket = mock.Mock()
        mock_socket_constructor.return_value = mock_socket

        self.ml._send("m|e@t:ric", 2, "type")
        mock_socket.sendto.assert_called_once_with(
            "m-e-t-ric:2|type",
            ("testhost", 4321))


class TestGetLogger(unittest.TestCase):
    def setUp(self):
        super(TestGetLogger, self).setUp()

    def test_get_noop_logger(self):
        metricslogging.setLoggerClass(metricslogging.NoopMetricsLogger)
        logger = metricslogging.getLogger("foo")
        self.assertTrue(isinstance(logger, metricslogging.NoopMetricsLogger))

    def test_get_statsd_logger(self):
        metricslogging.setLoggerClass(metricslogging.StatsdMetricsLogger)

        logger = metricslogging.getLogger("bar")
        self.assertTrue(isinstance(logger, metricslogging.StatsdMetricsLogger))
        self.assertEqual(logger.getPrefix(), "bar")


if __name__ == "__main__":
    unittest.main()