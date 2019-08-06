#!/usr/bin/env python2

import argparse
import simpy
import numpy as np
import os

DEBUG = True
#DEBUG = False

def print_debug(s):
    if DEBUG:
        print s

class Request(object):
    """This class represents a request to be scheduled/executed on a core 
    """
    count = 0
    def __init__(self, service_time):
        self.service_time = service_time
        self.ID = Request.count
        Request.count += 1

    def __str__(self):
        return "Request: service_time={}".format(self.service_time)

class Core(object):
    """This class represents a core to service requests"""
    count = 0
    def __init__(self, env, args):
        self.env = env
        self.args = args
        self.queue = simpy.Store(env)
        self.ID = Core.count
        Core.count += 1
        self.env.process(self.start_rcv())

    def start_rcv(self):
        """Start listening for incomming messages"""
        while not NicSchedulingSimulator.complete:
            msg = yield self.queue.get()
            print_debug('{}: Received msg at core {}:\n\t"{}"'.format(self.env.now, self.ID, str(msg)))
            yield self.env.timeout(msg.service_time)
            print_debug('{}: Finished Processing msg at core {}:\n\t"{}"'.format(self.env.now, self.ID, str(msg)))
            NicSchedulingSimulator.request_cnt += 1
            if NicSchedulingSimulator.request_cnt == self.args.num_requests:
                NicSchedulingSimulator.complete = True

class Dispatcher(object):
    """This class represents the request dispatcher which schedules requests to cores"""
    def __init__(self, env, args):
        self.env = env
        self.args = args
        self.queue = simpy.Store(env)
        self.cores = []
        self.env.process(self.start_dispatching())

    def add_cores(self, cores):
        self.cores += cores

    def start_dispatching(self):
        while not NicSchedulingSimulator.complete:
            msg = yield self.queue.get()
            print_debug('{}: Dispatching msg\n\t"{}"'.format(self.env.now, str(msg)))
            # This hashing function leads to perfect hashing if there are enough cores
            dst = msg.ID % len(self.cores)
            # put the request in the core's queue
            self.cores[dst].queue.put(msg)

class LoadGenerator(object):
    """This class generates a load for the dispatcher
    """
    def __init__(self, env, args, queue):
        self.env = env
        self.args = args
        # this queue will be drained by the dispatcher
        self.queue = queue

    def start_generating(self):
        for i in range(self.args.num_requests):
            print_debug('{}: Generating request'.format(self.env.now))
            # put the request in the core's queue
            self.queue.put(Request(self.args.service_time))
            yield self.env.timeout(self.args.delay)

class NicSchedulingSimulator(object):
    """This class controls the Othello simulation"""
    complete = False
    finish_time = 0
    sample_period = 10
    request_cnt = 0
    def __init__(self, env, args):
        self.env = env
        self.args = args
        self.dispatcher = Dispatcher(self.env, self.args)
        self.generator = LoadGenerator(self.env, self.args, self.dispatcher.queue)

        self.cores = []
        self.create_cores()
        self.dispatcher.add_cores(self.cores)
        
        self.init_sim()

        self.avg_q_times = []
        self.avg_q_samples = []
        self.start_logging()

    def create_cores(self):
        for i in range(self.args.cores):
            self.cores.append(Core(self.env, self.args))

    def init_sim(self):
        self.env.process(self.generator.start_generating())

    def start_logging(self):
        self.env.process(self.sample_host_queues())

    def sample_host_queues(self):
        """Sample avg core queue occupancy at every time"""
        while not NicSchedulingSimulator.complete:
            self.avg_q_times.append(self.env.now)
            q_samples = [len(c.queue.items) for c in self.cores]
            self.avg_q_samples.append(np.average(q_samples))
            yield self.env.timeout(NicSchedulingSimulator.sample_period)
            
    def dump_logs(self):
        """Dump any logs recorded during the simulation"""
        out_dir = os.path.join(os.getcwd(), NicSchedulingSimulator.out_dir)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        # log the measured avg queue sizes
        with open(os.path.join(out_dir, 'avg_q_samples.csv'), 'w') as f:
            for t, q in zip(self.avg_q_times, self.avg_q_samples):
                f.write('{}, {}\n'.format(t, q))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cores', type=int, help='Number of cores to use in the simulation', default=2)
    parser.add_argument('--num_requests', type=int, help='Number of requests to simulate', default=3)
    parser.add_argument('--service_time', type=int, help='Service time of each request', default=100)
    parser.add_argument('--delay', type=int, help='Delay between generation of requests', default=10)
    args = parser.parse_args()

    # Setup and start the simulation
    print 'Running Simulation ...'
    env = simpy.Environment() 
    s = NicSchedulingSimulator(env, args)
    env.run()
#    s.dump_logs()

if __name__ == '__main__':
    main()
