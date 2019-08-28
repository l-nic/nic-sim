#!/usr/bin/env python2

import argparse
import simpy
import pandas as pd
import numpy as np
import sys, os
import abc
import random
import json

# default cmdline args
cmd_parser = argparse.ArgumentParser()
cmd_parser.add_argument('--config', type=str, help='JSON config file to control the simulations', required=True)

class Logger(object):
    debug = False
    def __init__(self, env):
        self.env = env

    @staticmethod
    def init_params():
        pass

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

    @staticmethod
    def init_params():
        pass

    def __str__(self):
        return "Request: service_time={}".format(self.service_time)

class Core(object):
    """Abstract base class which represents a core to service requests"""
    __metaclass__ = abc.ABCMeta
    count = 0
    def __init__(self, env, logger, dispatcher):
        self.env = env
        self.logger = logger
        self.dispatcher = dispatcher
        self.queue = simpy.Store(env)
        self.ID = Core.count
        Core.count += 1
        self.env.process(self.start())

    @staticmethod
    def init_params():
        pass

    @abc.abstractmethod
    def start(self):
        """Receive and process messages"""
        pass


class Dispatcher(object):
    """Abstract base class which represents the request dispatcher that schedules requests to cores"""
    __metaclass__ = abc.ABCMeta
    def __init__(self, env, logger):
        self.env = env
        self.logger = logger
        self.queue = simpy.Store(env)
        self.cores = []
        self.env.process(self.start())

    @staticmethod
    def init_params():
        pass

    @abc.abstractmethod
    def start(self):
        """Start scheduling requests"""
        pass

    def add_cores(self, cores):
        self.cores += cores

def DistGenerator(dist, **kwargs):
    if dist == 'bimodal':
        bimodal_samples = map(int, list(np.random.normal(kwargs['lower_mean'], kwargs['lower_stddev'], kwargs['lower_samples']))
                                   + list(np.random.normal(kwargs['upper_mean'], kwargs['upper_stddev'], kwargs['upper_samples'])))
    while True:
        if dist == 'uniform':
            yield random.randint(kwargs['min'], kwargs['max'])
        elif dist == 'normal':
            yield int(np.random.normal(kwargs['mean'], kwargs['stddev']))
        elif dist == 'poisson':
            yield np.random.poisson(kwargs['lambda']) 
        elif dist == 'lognormal':
            yield int(np.random.lognormal(kwargs['mean'], kwargs['sigma']))
        elif dist == 'exponential':
            yield int(np.random.exponential(kwargs['lambda']))
        elif dist == 'fixed':
            yield kwargs['value']
        elif dist == 'bimodal':
            yield random.choice(bimodal_samples)
        else:
            print 'ERROR: Unsupported distrbution: {}'.format(dist)
            sys.exit(1)

class LoadGenerator(object):
    """This class generates a load for the dispatcher
    """
    def __init__(self, env, logger, queue, request_cls):
        self.env = env
        self.logger = logger
        # this queue will be drained by the dispatcher
        self.queue = queue
        self.request_cls = request_cls

        self.service_time = NicSimulator.config['service_time'].next()
        # initialize service time distribution params
        kwargs = {}
        if self.service_time == 'uniform':
            kwargs['min'] = NicSimulator.config['service_time_min'].next()
            kwargs['max'] = NicSimulator.config['service_time_max'].next()
        elif self.service_time == 'normal':
            kwargs['mean'] = NicSimulator.config['service_time_mean'].next()
            kwargs['stddev'] = NicSimulator.config['service_time_stddev'].next()
        elif self.service_time == 'poisson':
            kwargs['lambda'] = NicSimulator.config['service_time_lambda'].next()
        elif self.service_time == 'lognormal':
            kwargs['mean'] = NicSimulator.config['service_time_mean'].next()
            kwargs['sigma'] = NicSimulator.config['service_time_sigma'].next()
        elif self.service_time == 'exponential':
            kwargs['lambda'] = NicSimulator.config['service_time_lambda'].next()
        elif self.service_time == 'fixed':
            kwargs['value'] = NicSimulator.config['service_time_value'].next()
        elif self.service_time == 'bimodal':
            kwargs['lower_mean'] = NicSimulator.config['service_time_lower_mean'].next()
            kwargs['lower_stddev'] = NicSimulator.config['service_time_lower_stddev'].next()
            kwargs['lower_samples'] = NicSimulator.config['service_time_lower_samples'].next()
            kwargs['upper_mean'] = NicSimulator.config['service_time_upper_mean'].next()
            kwargs['upper_stddev'] = NicSimulator.config['service_time_upper_stddev'].next()
            kwargs['upper_samples'] = NicSimulator.config['service_time_upper_samples'].next()
        self.service_time_dist = DistGenerator(self.service_time, **kwargs)

        self.arrival_delay = NicSimulator.config['arrival_delay'].next()
        # initialize arrival delay distribution params
        kwargs = {}
        if self.arrival_delay == 'uniform':
            kwargs['min'] = NicSimulator.config['arrival_delay_min'].next()
            kwargs['max'] = NicSimulator.config['arrival_delay_max'].next()
        elif self.arrival_delay == 'normal':
            kwargs['mean'] = NicSimulator.config['arrival_delay_mean'].next()
            kwargs['stddev'] = NicSimulator.config['arrival_delay_stddev'].next()
        elif self.arrival_delay == 'poisson':
            kwargs['lambda'] = NicSimulator.config['arrival_delay_lambda'].next()
        elif self.arrival_delay == 'lognormal':
            kwargs['mean'] = NicSimulator.config['arrival_delay_mean'].next()
            kwargs['sigma'] = NicSimulator.config['arrival_delay_sigma'].next()
        elif self.arrival_delay == 'exponential':
            kwargs['scale'] = NicSimulator.config['arrival_delay_scale'].next()
        elif self.arrival_delay == 'fixed':
            kwargs['value'] = NicSimulator.config['arrival_delay_value'].next()
        elif self.arrival_delay == 'bimodal':
            kwargs['lower_mean'] = NicSimulator.config['arrival_delay_lower_mean'].next()
            kwargs['lower_stddev'] = NicSimulator.config['arrival_delay_lower_stddev'].next()
            kwargs['lower_samples'] = NicSimulator.config['arrival_delay_lower_samples'].next()
            kwargs['upper_mean'] = NicSimulator.config['arrival_delay_upper_mean'].next()
            kwargs['upper_stddev'] = NicSimulator.config['arrival_delay_upper_stddev'].next()
            kwargs['upper_samples'] = NicSimulator.config['arrival_delay_upper_samples'].next()
        self.arrival_delay_dist = DistGenerator(self.arrival_delay, **kwargs)

    def start(self):
        """Start generating requests"""
        for i in range(NicSimulator.num_requests):
            self.logger.log('Generating request')
            # generate and record service time
            service_time = self.service_time_dist.next()
            NicSimulator.service_times['all'].append(service_time)
            # generate and record arrival delay
            arrival_delay = self.arrival_delay_dist.next()
            NicSimulator.arrival_delays['all'].append(arrival_delay)
            # put the request in the core's queue
            self.queue.put(self.request_cls(service_time, self.env.now))
            yield self.env.timeout(arrival_delay)


class NicSimulator(object):
    """This class controls the simulation"""
    config = {} # user specified input
    out_dir = 'out'
    out_run_dir = 'out/run-0'
    # run local variables
    complete = False
    finish_time = 0
    request_cnt = 0
    service_times = {'all':[]}
    arrival_delays = {'all':[]}
    completion_times = {'all':[]}
    # global logs (across runs)
    tail_completion_times = {'99pc':[], '90pc':[]}
    avg_throughput = {'all':[]}
    def __init__(self, env, core_cls, dispatcher_cls, request_cls=Request, logger_cls=Logger):
        self.env = env
        self.num_cores = NicSimulator.config['num_cores'].next()
        self.sample_period = NicSimulator.config['sample_period'].next()
        NicSimulator.num_requests = NicSimulator.config['num_requests'].next()
        self.logger = logger_cls(env)
        self.dispatcher = dispatcher_cls(self.env, self.logger)
        self.generator = LoadGenerator(self.env, self.logger, self.dispatcher.queue, request_cls)

        # create cores
        self.cores = []
        for i in range(self.num_cores):
            self.cores.append(core_cls(self.env, self.logger, self.dispatcher))

        # connect cores to dispatcher
        self.dispatcher.add_cores(self.cores)
        
        self.init_sim()

    def init_sim(self):
        # initialize run local variables
        self.q_sizes = {c.ID:[] for c in self.cores}
        self.q_sizes['time'] = []
        self.q_sizes['dispatcher'] = []
        NicSimulator.complete = False
        NicSimulator.request_cnt = 0
        NicSimulator.finish_time = 0
        NicSimulator.completion_times = {'all':[]}
        NicSimulator.service_times = {'all':[]}
        NicSimulator.arrival_delays = {'all':[]}
        # start generating requests
        self.env.process(self.generator.start())
        # start logging
        if self.sample_period > 0:
            self.env.process(self.sample_queues())

    def sample_queues(self):
        """Sample avg core queue occupancy at every time"""
        while not NicSimulator.complete:
            self.q_sizes['time'].append(self.env.now)
            self.q_sizes['dispatcher'].append(len(self.dispatcher.queue.items))
            for c in self.cores:
                self.q_sizes[c.ID].append(len(c.queue.items))
            yield self.env.timeout(self.sample_period)

    @staticmethod
    def check_done(now):
        if NicSimulator.request_cnt == NicSimulator.num_requests:
            NicSimulator.complete = True
            NicSimulator.finish_time = now

    def dump_run_logs(self):
        """Dump any logs recorded during this run of the simulation"""
        out_dir = os.path.join(os.getcwd(), NicSimulator.out_run_dir)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        # log the measured avg queue sizes
        df = pd.DataFrame(self.q_sizes)
        write_csv(df, os.path.join(NicSimulator.out_run_dir, 'q_sizes.csv'))

        # log the measured request completion times
        df = pd.DataFrame(NicSimulator.completion_times)*1e-3 # microseconds
        write_csv(df, os.path.join(NicSimulator.out_run_dir, 'completion_times.csv'))

        # log the generated service times
        df = pd.DataFrame(NicSimulator.service_times) # nanoseconds
        write_csv(df, os.path.join(NicSimulator.out_run_dir, 'service_times.csv'))

        # log the generated arrival delays
        df = pd.DataFrame(NicSimulator.arrival_delays) # nanoseconds
        write_csv(df, os.path.join(NicSimulator.out_run_dir, 'arrival_delays.csv'))

        # record tail latencies for this run
        tail99 = np.percentile(NicSimulator.completion_times['all'], 99)*1e-3 # microseconds
        tail90 = np.percentile(NicSimulator.completion_times['all'], 90)*1e-3 # microseconds
        NicSimulator.tail_completion_times['99pc'].append(tail99)
        NicSimulator.tail_completion_times['90pc'].append(tail90)

        # record avg throughput for this run
        throughput = float(NicSimulator.num_requests)*1e3/(NicSimulator.finish_time) # MRPS
        NicSimulator.avg_throughput['all'].append(throughput)

    @staticmethod
    def dump_global_logs():
        # log tail completion_times
        df = pd.DataFrame(NicSimulator.tail_completion_times)
        write_csv(df, os.path.join(NicSimulator.out_dir, 'tail_completion_times.csv'))

        # log avg throughput
        df = pd.DataFrame(NicSimulator.avg_throughput)
        write_csv(df, os.path.join(NicSimulator.out_dir, 'avg_throughput.csv'))

def write_csv(df, filename):
    with open(filename, 'w') as f:
            f.write(df.to_csv(index=False))

def param(x):
    while True:
        yield x

def param_list(L):
    for x in L:
        yield x

def parse_config(config_file):
    """ Convert each parameter in the JSON config file into a generator
    """
    with open(config_file) as f:
        config = json.load(f)

    for p, val in config.iteritems():
        if type(val) == list:
            config[p] = param_list(val)
        else:
            config[p] = param(val)

    return config

def run_nic_sim(cmdline_args, *args):
    NicSimulator.config = parse_config(cmdline_args.config)
    # make sure output directory exists
    NicSimulator.out_dir = NicSimulator.config['out_dir'].next()
    out_dir = os.path.join(os.getcwd(), NicSimulator.out_dir)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    # copy config file into output directory
    os.system('cp {} {}'.format(cmdline_args.config, out_dir))
    # run the simulations
    run_cnt = 0
    try:
        while True:
            print 'Running simulation {} ...'.format(run_cnt)
            # initialize random seed
            random.seed(1)
            np.random.seed(1)
            # init params for this run on all classes
            for cls in args:
                cls.init_params()
            NicSimulator.out_run_dir = os.path.join(NicSimulator.out_dir, 'run-{}'.format(run_cnt))
            run_cnt += 1
            env = simpy.Environment()
            s = NicSimulator(env, *args)
            env.run()
            s.dump_run_logs()
    except StopIteration:
        NicSimulator.dump_global_logs()
        print 'All Simulations Complete!'

