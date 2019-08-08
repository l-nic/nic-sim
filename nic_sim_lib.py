#!/usr/bin/env python2

import argparse
import simpy
import pandas as pd
import numpy as np
import os
import abc

# default cmdline args
cmd_parser = argparse.ArgumentParser()
cmd_parser.add_argument('--cores', type=int, help='Number of cores to use in the simulation', default=3)
cmd_parser.add_argument('--num_requests', type=int, help='Number of requests to simulate', default=3)
cmd_parser.add_argument('--service_time', type=int, help='Service time of each request', default=100)
cmd_parser.add_argument('--delay', type=int, help='Delay between generation of requests', default=10)

class Logger(object):
    debug = False
    def __init__(self, env):
        self.env = env

    def log(self, s):
        if Logger.debug:
            print '{}: {}'.format(self.env.now, s)


class Request(object):
    """This class represents a request to be scheduled/executed on a core 
    """
    count = 0
    def __init__(self, service_time, start_time):
        self.start_time = start_time
        self.service_time = service_time
        self.ID = Request.count
        Request.count += 1

    def __str__(self):
        return "Request: service_time={}".format(self.service_time)


class Core(object):
    """Abstract base class which represents a core to service requests"""
    __metaclass__ = abc.ABCMeta
    count = 0
    def __init__(self, env, args, logger):
        self.env = env
        self.args = args
        self.logger = logger
        self.queue = simpy.Store(env)
        self.ID = Core.count
        Core.count += 1
        self.env.process(self.start())

    @abc.abstractmethod
    def start(self):
        """Receive and process messages"""
        pass


class Dispatcher(object):
    """Abstract base class which represents the request dispatcher that schedules requests to cores"""
    __metaclass__ = abc.ABCMeta
    def __init__(self, env, args, logger):
        self.env = env
        self.args = args
        self.logger = logger
        self.queue = simpy.Store(env)
        self.cores = []
        self.env.process(self.start())

    @abc.abstractmethod
    def start(self):
        """Start scheduling requests"""
        pass


class LoadGenerator(object):
    """This class generates a load for the dispatcher
    """
    def __init__(self, env, args, logger, queue, request_cls):
        self.env = env
        self.args = args
        self.logger = logger
        # this queue will be drained by the dispatcher
        self.queue = queue
        self.request_cls = request_cls

    def start(self):
        """Start generating requests"""
        for i in range(self.args.num_requests):
            self.logger.log('Generating request')
            # put the request in the core's queue
            self.queue.put(self.request_cls(self.args.service_time, self.env.now))
            yield self.env.timeout(self.args.delay)


class NicSimulator(object):
    """This class controls the simulation"""
    complete = False
    finish_time = 0
    sample_period = 10
    request_cnt = 0
    out_dir = 'out'
    completion_times = {}
    def __init__(self, env, args, core_cls, dispatcher_cls, request_cls=Request, logger_cls=Logger):
        self.env = env
        self.args = args
        self.logger = logger_cls(env)
        self.dispatcher = dispatcher_cls(self.env, self.args, self.logger)
        self.generator = LoadGenerator(self.env, self.args, self.logger, self.dispatcher.queue, request_cls)

        # create cores
        self.cores = []
        for i in range(self.args.cores):
            self.cores.append(core_cls(self.env, self.args, self.logger))

        # connect cores to dispatcher
        self.dispatcher.cores += self.cores
        
        self.init_sim()

    def init_sim(self):
        # initialize stats
        self.q_sizes = {c.ID:[] for c in self.cores}
        self.q_sizes['time'] = []
        self.q_sizes['dispatcher'] = []
        NicSimulator.completion_times = {'all':[]}
        # start generating requests
        self.env.process(self.generator.start())
        # start logging
        self.env.process(self.sample_queues())

    def sample_queues(self):
        """Sample avg core queue occupancy at every time"""
        while not NicSimulator.complete:
            self.q_sizes['time'].append(self.env.now)
            self.q_sizes['dispatcher'].append(len(self.dispatcher.queue.items))
            for c in self.cores:
                self.q_sizes[c.ID].append(len(c.queue.items))
            yield self.env.timeout(NicSimulator.sample_period)
            
    def dump_logs(self):
        """Dump any logs recorded during the simulation"""
        out_dir = os.path.join(os.getcwd(), NicSimulator.out_dir)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        # log the measured avg queue sizes
        df = pd.DataFrame(self.q_sizes)
        self.write_csv(df, 'q_sizes.csv')

        # log the measured request completion times
        df = pd.DataFrame(NicSimulator.completion_times)
        self.write_csv(df, 'completion_times.csv')

    def write_csv(self, df, filename):
        out_dir = os.path.join(os.getcwd(), NicSimulator.out_dir)
        with open(os.path.join(out_dir, filename), 'w') as f:
                f.write(df.to_csv(index=False))

