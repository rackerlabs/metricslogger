# -*- coding: utf-8 -*-
#
#  Copyright 2014 Rackspace Hosting
# All Rights Reserved.
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

import abc
import contextlib
import functools
import itertools
import random
import six
import socket
import time


def _time():
    return time.time()


def _to_list(name):
    if isinstance(name, basestring):
        return [name]
    elif isinstance(name, tuple):
        return list(name)
    else:
        return name


def _list_join(delimiter, *names):
    return delimiter.join(itertools.chain([_to_list(n) for n in names if n is not None]))


@six.add_metaclass(abc.ABCMeta)
class MetricsLogger(object):
    """Abstract class representing a metrics logger."""

    loggers = dict()
    logger_class = None
    global_prefix = ''
    prepend_host = False
    prepend_host_reverse = False
    host = socket.gethostname().split('.')

    def __init__(self, prefix):
        self.prefix = prefix

    def format_name(self, name):
        host = ''
        if getPrependHost():
            if getPrependHostReverse():
                host = _to_list(reversed(getHost()))
            else:
                host = getHost()

        return self._format_name(getGlobalPrefix(), host, _to_list(self.prefix), _to_list(name))

    def gauge(self, name, value):
        """Send gauge metric data."""
        self._gauge(self.format_name(name), value)

    def counter(self, name, value, sample_rate=None):
        """Send counter metric data.

        Optionally, specify sample_rate in the interval [0.0, 1.0] to
        sample data probabilistically where:

            P(send metric data) = sample_rate

        If sample_rate is None, then always send metric data, but do not
        have the backend send sample rate information (if supported).
        """
        if sample_rate is not None and \
            (sample_rate < 0.0 or sample_rate > 1.0):
            raise ValueError("sample_rate must be None, or in the interval "
                             "[0.0, 1.0]")

        if sample_rate is None or random.random() < sample_rate:
            return self._counter(self.format_name(name), value,
                                 sample_rate=sample_rate)

    def timer(self, name, value):
        """Send timer data."""
        self._timer(self.format_name(name), value)

    def meter(self, name, value):
        """Send meter data."""
        self._meter(self.format_name(name), value)

    @abc.abstractmethod
    def _format_name(self, global_prefix, host, prefix, name):
        """Abstract method for backends to implement metric behavior."""

    @abc.abstractmethod
    def _gauge(self, name, value):
        """Abstract method for backends to implement gauge behavior."""

    @abc.abstractmethod
    def _counter(self, name, value, sample_rate=None):
        """Abstract method for backends to implement counter behavior."""

    @abc.abstractmethod
    def _timer(self, name, value):
        """Abstract method for backends to implement timer behavior."""

    @abc.abstractmethod
    def _meter(self, name, value):
        """Abstract method for backends to implement meter behavior."""

    def instrument(self, *name):
        """Returns a decorator that instruments a function, bound to this
        MetricsLogger.  For example:

        from ironic.common import metrics

        METRICS = metrics.getLogger()

        @METRICS.instrument('foo')
        def foo(bar, baz):
            print bar, baz
        """
        def decorator(f):
            @functools.wraps(f)
            def wrapped(*args, **kwargs):
                start = _time()
                result = f(*args, **kwargs)

                # Call duration in seconds
                duration = _time() - start

                # Log the timing data as a timer (in ms)
                self.timer(name, duration * 1000)
                return result
            return wrapped
        return decorator


class NoopMetricsLogger(MetricsLogger):
    """MetricsLogger that ignores all metric data."""
    def __init__(self, prefix, delimiter):
        super(NoopMetricsLogger, self).__init__(prefix, delimiter)

    def _gauge(self, m_name, m_value):
        pass

    def _counter(self, m_name, m_value, sample_rate=None):
        pass

    def _timer(self, m_name, m_value):
        pass

    def _meter(self, m_name, m_value):
        pass

    def _format_name(self, global_prefix, name):
        pass


class StatsdMetricsLogger(MetricsLogger):
    """MetricsLogger that sends data via the statsd protocol."""

    GAUGE_TYPE = 'g'
    COUNTER_TYPE = 'c'
    TIMER_TYPE = 'ms'
    METER_TYPE = 'm'

    prefix = []
    delimiter = '.'
    statsd_host = 'localhost'
    statsd_port = 8125

    def __init__(self, prefix=None, delimiter=None, statsd_host=None, statsd_port=None):
        """Initialize a StatsdMetricsLogger"""

        self._prefix_override = prefix
        self._delimiter_override = delimiter
        self._statsd_host_override = statsd_host
        self._statsd_port_override = statsd_port

    def _send(self, m_name, m_value, m_type, sample_rate=None):
        if sample_rate is None:
            metric = '%s:%s|%s' % (m_name, m_value, m_type)
        else:
            metric = '%s:%s|%s@%s' % (m_name, m_value, m_type, sample_rate)

        # Ideally, we'd cache a sending socket in self, but that
        # results in a socket getting shared by multiple green threads.
        with contextlib.closing(self._open_socket()) as sock:
            return sock.sendto(metric, self.getStatsdTarget())

    def _open_socket(self):
        return socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def _gauge(self, m_name, m_value):
        return self._send(m_name, m_value, self.GAUGE_TYPE)

    def _counter(self, m_name, m_value, sample_rate=None):
        return self._send(m_name, m_value, self.COUNTER_TYPE,
                          sample_rate=sample_rate)

    def _timer(self, m_name, m_value):
        return self._send(m_name, m_value, self.TIMER_TYPE)

    def _meter(self, m_name, m_value):
        return self._send(m_name, m_value, self.METER_TYPE)

    def _format_name(self, global_prefix, host, prefix, name):
        return _list_join(self.getDelimiter(), global_prefix, host, name)

    def getDelimiter(self):
        if self._delimiter_override is not None:
            return self._delimiter_override
        else:
            return self._delimiter

    def getPrefix(self):
        if self._prefix_override is not None:
            return self._prefix_override
        else:
            return self._prefix

    def getStatsdHost(self):
        if self._statsd_host_override is not None:
            return self._statsd_host_override
        else:
            return self._statsd_host

    def getStatsdPort(self):
        if self._statsd_port_override is not None:
            return self._statsd_port_override
        else:
            return self._statsd_port

    def getStatsdTarget(self):
        return self.getStatsdHost(), self.getStatsdPort()

    def setDelimiter(self, delimiter):
        self._delimiter_override = delimiter

    def setPrefix(self, prefix):
        self._prefix_override = prefix

    def setStatsdHost(self, host):
        self._statsd_host_override = host

    def setStatsdPort(self, port):
        self._statsd_port_override = port


class InstrumentContext(object):
    """Metrics instrumentation context manager"""
    def __init__(self, prefix, *parts):
        self.logger = getLogger(prefix)
        self.parts = parts

    def __enter__(self):
        self.start_time = time.time()
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = (time.time() - self.start_time) * 1000
        # Log the timing data
        self.logger.timer(self.parts, duration)


def setLoggerClass(klass):
    MetricsLogger.logger_class = klass


def getLoggerClass():
    return MetricsLogger.logger_class


def setGlobalPrefix(prefix):
    MetricsLogger.global_prefix = _to_list(prefix)


def getGlobalPrefix():
    return MetricsLogger.global_prefix


def setPrependHost(value):
    MetricsLogger.prepend_host = value


def getPrependHost():
    return MetricsLogger.prepend_host


def setPrependHostReverse(value):
    MetricsLogger.prepend_host_reverse = value


def getPrependHostReverse():
    return MetricsLogger.prepend_host_reverse


def setHost(host):
    MetricsLogger.host = host


def getHost():
    return MetricsLogger.host


def initLogger(name):
    LoggerCls = getLoggerClass()

    return LoggerCls()


def getLogger(name):
    """Return a MetricsLogger with the specified name."""

    if name not in MetricsLogger.loggers:
        MetricsLogger.loggers[name] = initLogger(name)

    return MetricsLogger.loggers[name]

MetricsLogger.logger_class = StatsdMetricsLogger
