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
import contextlib2
import itertools
import pprint
import random
import six
import socket
import string
import time
import wrapt


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
    return filter(
        lambda s: not skip_empty or s != "",
        itertools.chain(*[_to_list(p) for p in parts]))


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
setGlobalPrefix, getGlobalPrefix = \
    _global_config.add_config('global_prefix', '')
setPrependHost, getPrependHost = \
    _global_config.add_config('prepend_host', False)
setPrependHostReverse, getPrependHostReverse = \
    _global_config.add_config('prepend_host_reverse', False)
setHost, getHost = \
    _global_config.add_config('host', socket.gethostname())

setStatsdDelimiter, getStatsdDelimiter = \
    _global_config.add_config('statsd_delimiter', '.')
setStatsdHost, getStatsdHost = \
    _global_config.add_config('statsd_host', 'localhost')
setStatsdPort, getStatsdPort =\
    _global_config.add_config('statsd_port', 8125)


class TimerContextDecorator(contextlib2.ContextDecorator):
    """
    Combination decorator and context manager to time functions or code blocks.
    Emits a timer metric to the specified logger.  Recommended to be
    instantiated by the timer_cd() convenience function on a MetricLogger.
    """
    def __init__(self, logger, name):
        self.logger = logger
        self.name = name

    def __enter__(self):
        self.start_time = _time()
        return self

    def __exit__(self, *exc):
        duration = (_time() - self.start_time) * 1000
        self.logger.timer(self.name, duration)


class CounterContextDecorator(contextlib2.ContextDecorator):
    """
    Combination decorator and context manager to count function calls or code
    block executions.  Emits a timer metric to the specified logger.
    Recommended to be instantiated by the counter_cd() convenience function on
    a MetricLogger.
    """
    def __init__(self, logger, name, sample_rate):
        self.logger = logger
        self.name = name
        self.sample_rate = sample_rate

    def __enter__(self):
        self.logger.counter(self.name, 1, sample_rate=self.sample_rate)
        return self

    def __exit__(self, *exc):
        pass


@six.add_metaclass(abc.ABCMeta)
class MetricsLogger(object):
    """Abstract class representing a metrics logger."""

    def __init__(self):
        self._config_override = NestedConfig(_global_config)

        # Add getters for non-overridable options
        _, self.getLoggerClass = \
            _global_config.add_config('logger_class', override=True)
        _, self.getGlobalPrefix = \
            _global_config.add_config('global_prefix', override=True)

        # Add setters and getters for instance-overridable options
        self.setPrefix, self.getPrefix = \
            self._config_override.add_config('prefix', override=True)
        self.setPrependHost, self.getPrependHost = \
            self._config_override.add_config('prepend_host', override=True)
        self.setPrependHostReverse, self.getPrependHostReverse = \
            self._config_override.add_config('prepend_host_reverse',
                                             override=True)
        self.setHost, self.getHost = \
            self._config_override.add_config('host', override=True)

    def format_name(self, name):
        """Format a given metric name in the context of the settings for this
        MetricsLogger.

        :param name: Metric name
        """
        if self.getPrependHost():
            host = _get_host_parts(self.getHost())
        else:
            host = []

        if self. getPrependHostReverse():
            host = list(reversed(host))

        return self._format_name(self.getGlobalPrefix(), host,
                                 self.getPrefix(), name)

    def gauge(self, name, value):
        """Send gauge metric data.

        :param name: Metric name
        :param value: Metric value
        """
        self._gauge(self.format_name(name), value)

    def counter(self, name, value, sample_rate=None):
        """Send counter metric data.

        Optionally, specify sample_rate in the interval [0.0, 1.0], or None to
        sample data probabilistically where:

            P(send metric data) = sample_rate

        If sample_rate is None, then always send metric data, but do not
        have the backend send sample rate information (if supported).

        :param name: Metric name
        :param value: Metric value
        :param sample_rate: Sample rate in interval [0.0, 1.0], or None
        """
        if sample_rate is not None and (sample_rate < 0.0 or
                                        sample_rate > 1.0):
            raise ValueError(
                "sample_rate must be None, or in the interval [0.0, 1.0]")

        if sample_rate is None or random.random() < sample_rate:
            return self._counter(self.format_name(name), value,
                                 sample_rate=sample_rate)

    def timer(self, name, value):
        """Send timer data.

        :param name: Metric name
        :param value: Metric value
        """
        self._timer(self.format_name(name), value)

    @abc.abstractmethod
    def _format_name(self, global_prefix, host, prefix, name):
        """Abstract method for backends to implement metric name formatting.

        :param global_prefix: Global prefix (None if not set)
        :param host: Logger host
        :param prefix: Logger prefix
        :param name: Metric name
        """

    @abc.abstractmethod
    def _gauge(self, name, value):
        """Abstract method for backends to implement gauge behavior.

        :param name: Metric name
        :param value: Metric value
        """

    @abc.abstractmethod
    def _counter(self, name, value, sample_rate=None):
        """Abstract method for backends to implement counter behavior.

        This function is called with P(call) = sample_rate, as described in
        counter().

        :param name: Metric name
        :param value: Metric value
        :param sample_rate: Sample rate in interval [0.0, 1.0], or None
        """

    @abc.abstractmethod
    def _timer(self, name, value):
        """Abstract method for backends to implement timer behavior.

        :param name: Metric name
        :param value: Metric value
        """

    def timer_cd(self, name):
        """
        Returns a TimerContextDecorator bound to this MetricsLogger for use
        timing function calls, or code blocks.  Can be used either as a
        decorator, or a context manager.  For example:

        METRICS = getLogger("name")

        @METRICS.timer_cd("foo")
        def foo():
            do_something()

        with METRICS.timer_cd("bar) as _:
            do_something()

        :param name: Metric name
        """
        return TimerContextDecorator(self, name)

    def counter_cd(self, name, sample_rate=None):
        """
        Returns a CounterContextDecorator bound to this MetricsLogger for use
        counting function calls, or code block executions.  Can be used either
        as a decorator, or a context manager.  For example:

        METRICS = getLogger("name")

        @METRICS.counter_cd("foo")
        def foo():
            do_something()

        with METRICS.counter_cd("bar) as _:
            do_something()

        :param name: Metric name
        :param sample_rate: Sample rate to be passed to counter()
        """
        return CounterContextDecorator(self, name, sample_rate)

    def return_val_gauge_d(self, name):
        """
        Returns a decorator bound to this metrics MetricsLogger that emits the
        return value of the function it wraps as a gauge each time it is
        called.
        :param name: Metric name
        """
        @wrapt.decorator
        def wrapper(wrapped, instance, args, kwargs):
            result = wrapped(*args, **kwargs)
            self.gauge(name, result)
            return result
        return wrapper


class NoopMetricsLogger(MetricsLogger):
    """MetricsLogger that ignores all metric data."""
    def __init__(self):
        super(NoopMetricsLogger, self).__init__()

    def _format_name(self, *args, **kwargs):
        pass

    def _gauge(self, *args, **kwargs):
        pass

    def _counter(self, *args, **kwargs):
        pass

    def _timer(self, *args, **kwargs):
        pass


class DebugMetricsLogger(MetricsLogger):
    """MetricsLogger that prints all calls for debugging purposes"""
    def __init__(self):
        super(DebugMetricsLogger, self).__init__()

    def _format_name(self, *args, **kwargs):
        pprint.pprint(("_format_name call:", args, kwargs))

    def _gauge(self, *args, **kwargs):
        pprint.pprint(("_gauge call:", args, kwargs))

    def _counter(self, *args, **kwargs):
        pprint.pprint(("_counter call:", args, kwargs))

    def _timer(self, *args, **kwargs):
        pprint.pprint(("_timer call:", args, kwargs))


class StatsdMetricsLogger(MetricsLogger):
    """MetricsLogger that sends data via the statsd protocol."""

    GAUGE_TYPE = 'g'
    COUNTER_TYPE = 'c'
    TIMER_TYPE = 'ms'

    PROHIBITED_CHARS = ':|@\n'
    REPLACE_CHARS = '----'

    def __init__(self):
        super(StatsdMetricsLogger, self).__init__()

        # Add setters and getters for instance-overridable options
        self.setStatsdDelimiter, self.getStatsdDelimiter = \
            self._config_override.add_config('statsd_delimiter', override=True)
        self.setStatsdHost, self.getStatsdHost = \
            self._config_override.add_config('statsd_host', override=True)
        self.setStatsdPort, self.getStatsdPort = \
            self._config_override.add_config('statsd_port', override=True)

    def _send(self, name, value, type, sample_rate=None):
        if sample_rate is None:
            metric = '%s:%s|%s' % (self._sanitize(name),
                                   self._sanitize(value),
                                   self._sanitize(type))
        else:
            metric = '%s:%s|%s@%s' % (self._sanitize(name),
                                      self._sanitize(value),
                                      self._sanitize(type),
                                      self._sanitize(sample_rate))

        with contextlib.closing(self._open_socket()) as sock:
            return sock.sendto(metric, (self.getStatsdHost(),
                                        self.getStatsdPort()))

    @staticmethod
    def _sanitize(s):
        return str(s).translate(string.maketrans(
            StatsdMetricsLogger.PROHIBITED_CHARS,
            StatsdMetricsLogger.REPLACE_CHARS))

    @staticmethod
    def _open_socket():
        return socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def _format_name(self, global_prefix, host, prefix, name):
        return _list_join(self.getStatsdDelimiter(), True,
                          global_prefix, host, prefix, name)

    def _gauge(self, m_name, m_value):
        return self._send(m_name, m_value, self.GAUGE_TYPE)

    def _counter(self, m_name, m_value, sample_rate=None):
        return self._send(m_name, m_value, self.COUNTER_TYPE,
                          sample_rate=sample_rate)

    def _timer(self, m_name, m_value):
        return self._send(m_name, m_value, self.TIMER_TYPE)


def initLogger(prefix):
    """
    Instantiate a MetricsLogger of the type specified by setLoggerClass, with
    the given prefix.

    :param prefix: Prefix to set on MetricsLogger
    """
    LoggerCls = getLoggerClass()
    logger = LoggerCls()

    logger.setPrefix(prefix)
    return logger

_loggers = dict()


def getLogger(prefix):
    """
    Get a MetricsLogger with the given prefix.  If a logger with that prefix
    has already been created, return it.  Otherwise, return a new one via
    initLogger()

    :param prefix: Prefix to set on MetricsLogger
    """

    if prefix not in _loggers:
        _loggers[prefix] = initLogger(prefix)

    return _loggers[prefix]


setLoggerClass, getLoggerClass = \
    _global_config.add_config('logger_class', StatsdMetricsLogger)
