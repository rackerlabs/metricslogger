# -*- coding: utf-8 -*-
#
# Copyright 2015 Rackspace Hosting
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
import string
import time


def _time():
    return time.time()


def _to_list(parts):
    if parts is None:
        return []
    elif isinstance(parts, list):
        return parts
    elif isinstance(parts, basestring):
        return [parts]
    elif isinstance(parts, tuple):
        return list(parts)
    else:
        raise TypeError("Can only operate on lists, strings, or tuples")


def _list_chain(skip_empty, *parts):
    return filter(lambda s: not skip_empty or s != "", itertools.chain(*[_to_list(p) for p in parts]))


def _list_join(delimiter, skip_empty, *parts):
    return delimiter.join(_list_chain(skip_empty, *parts))


def _get_host_parts(host):
    if isinstance(host, basestring):
        return _to_list(host.split('.'))
    else:
        return _to_list(host)

class NestedConfig(object):
    def __init__(self, parent=None):
        self._config = dict()
        self._parent = parent

    def set_config(self, name, value):
        self._config[name] = value

    def get_config(self, name):
        if name in self._config:
            return self._config[name]
        elif self._parent:
            return self._parent.get_config(name)
        else:
            return None

    def reset_config(self):
        self._config = dict()

    def add_config(self, name, default=None, override=False):
        def setter_fn(value):
            return self.set_config(name, value)

        def getter_fn():
            return self.get_config(name)

        if not override:
            self.set_config(name, default)

        return setter_fn, getter_fn


# Global config options
_global_config = NestedConfig()

# Public global config setters and getters
setGlobalPrefix, getGlobalPrefix = _global_config.add_config('global_prefix', '')
setPrependHost, getPrependHost = _global_config.add_config('prepend_host', False)
setPrependHostReverse, getPrependHostReverse = _global_config.add_config('prepend_host_reverse', False)
setHost, getHost = _global_config.add_config('host', socket.gethostname())

setStatsdDelimiter, getStatsdDelimiter = _global_config.add_config('statsd_delimiter', '.')
setStatsdHost, getStatsdHost = _global_config.add_config('statsd_host', 'localhost')
setStatsdPort, getStatsdPort = _global_config.add_config('statsd_port', 8125)


@six.add_metaclass(abc.ABCMeta)
class MetricsLogger(object):
    """Abstract class representing a metrics logger."""

    def __init__(self):
        self._config_override = NestedConfig(_global_config)

        # Add getters for non-overridable options
        _, self.getLoggerClass = _global_config.add_config('logger_class', override=True)
        _, self.getGlobalPrefix = _global_config.add_config('global_prefix', override=True)

        # Add setters and getters for instance-overridable options
        self.setPrefix, self.getPrefix = self._config_override.add_config('prefix', override=True)
        self.setPrependHost, self.getPrependHost = self._config_override.add_config('prepend_host', override=True)
        self.setPrependHostReverse, self.getPrependHostReverse = self._config_override.add_config('prepend_host_reverse', override=True)
        self.setHost, self.getHost = self._config_override.add_config('host', override=True)

    def format_name(self, name):
        if self.getPrependHost():
            host = _get_host_parts(self.getHost())
        else:
            host = []

        if self. getPrependHostReverse():
            host = list(reversed(host))

        return self._format_name(self.getGlobalPrefix(), host, self.getPrefix(), name)

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

    def fn_time(self, *name):
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
    def __init__(self):
        super(NoopMetricsLogger, self).__init__()

    def _format_name(self, m_name, m_value):
        pass

    def _gauge(self, name, value):
        pass

    def _counter(self,name, value, sample_rate=None):
        pass

    def _timer(self, name, value):
        pass

    def _format_name(self, global_prefix, host, prefix, name):
        pass


class StatsdMetricsLogger(MetricsLogger):
    """MetricsLogger that sends data via the statsd protocol."""

    GAUGE_TYPE = 'g'
    COUNTER_TYPE = 'c'
    TIMER_TYPE = 'ms'

    PROHIBITED_CHARS = ':|@'
    REPLACE_CHARS = '---'

    def __init__(self):
        """Initialize a StatsdMetricsLogger"""

        super(StatsdMetricsLogger, self).__init__()

        # Add setters and getters for instance-overridable options
        self.setStatsdDelimiter, self.getStatsdDelimiter = self._config_override.add_config('statsd_delimiter', override=True)
        self.setStatsdHost, self.getStatsdHost = self._config_override.add_config('statsd_host', override=True)
        self.setStatsdPort, self.getStatsdPort = self._config_override.add_config('statsd_port', override=True)

    def _send(self, name, value, type, sample_rate=None):
        if sample_rate is None:
            metric = '%s:%s|%s' % (self._sanitize(name), self._sanitize(value), self._sanitize(type))
        else:
            metric = '%s:%s|%s@%s' % (self._sanitize(name), self._sanitize(value),
                                      self._sanitize(type), self._sanitize(sample_rate))

        print metric

        with contextlib.closing(self._open_socket()) as sock:
            return sock.sendto(metric, (self.getStatsdHost(), self.getStatsdPort()))

    @staticmethod
    def _sanitize(s):
        return str(s).translate(string.maketrans(StatsdMetricsLogger.PROHIBITED_CHARS, StatsdMetricsLogger.REPLACE_CHARS))

    @staticmethod
    def _open_socket():
        return socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def _format_name(self, global_prefix, host, prefix, name):
        return _list_join(self.getStatsdDelimiter(), True, global_prefix, host, prefix, name)

    def _gauge(self, m_name, m_value):
        return self._send(m_name, m_value, self.GAUGE_TYPE)

    def _counter(self, m_name, m_value, sample_rate=None):
        return self._send(m_name, m_value, self.COUNTER_TYPE,
                          sample_rate=sample_rate)

    def _timer(self, m_name, m_value):
        return self._send(m_name, m_value, self.TIMER_TYPE)


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


def initLogger(name):
    LoggerCls = getLoggerClass()
    logger = LoggerCls()

    logger.setPrefix(name)
    return logger

_loggers = dict()


def getLogger(name):
    """Return a MetricsLogger with the specified name."""

    if name not in _loggers:
        _loggers[name] = initLogger(name)

    return _loggers[name]


setLoggerClass, getLoggerClass = _global_config.add_config('logger_class', StatsdMetricsLogger)
