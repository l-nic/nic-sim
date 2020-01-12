#!/usr/bin/env python2

import argparse
import simpy
import pandas as pd
import numpy as np
import random

from nic_sim_lib import cmd_parser, Request, Core, Dispatcher, Logger, NicSimulator, run_nic_sim

Logger.debug = False

class JBSQCore(Core):
    """Core which processes requests to completion"""
    @staticmethod
    def init_params():
        JBSQCore.comm_delay = NicSimulator.config['comm_delay'].next()

    def start(self):
        while not NicSimulator.complete:
            msg = yield self.queue.get()
            self.logger.log('Received msg at core {}:\n\t"{}"'.format(self.ID, str(msg)))
            yield self.env.timeout(msg.service_time)
            self.logger.log('Finished Processing msg at core {}:\n\t"{}"'.format(self.ID, str(msg)))
            # asynchronously notify dispatcher that this core is available for another msg
            self.env.process(self.notify_dispatcher(msg))

    def notify_dispatcher(self, msg):
        yield self.env.timeout(JBSQCore.comm_delay)
        self.dispatcher.idle_cores.put(self)
        NicSimulator.completion_times['all'].append(self.env.now - msg.start_time)
        NicSimulator.request_cnt += 1
        NicSimulator.check_done(self.env.now)

class JBSQDispatcher(Dispatcher):
    """Dispatch a bounded number of requests to each core"""
    @staticmethod
    def init_params():
        JBSQDispatcher.queue_bound = NicSimulator.config['queue_bound'].next()

    def __init__(self, *args):
        super(JBSQDispatcher, self).__init__(*args)
        self.idle_cores = simpy.Store(self.env)

    # override base class method
    def add_cores(self, cores):
        self.cores += cores
        # add each core to the list of idle_cores queue_bound times
        for i in range(JBSQDispatcher.queue_bound):
            for c in self.cores:
                self.idle_cores.put(c)

    def start(self):
        while not NicSimulator.complete:
            # wait for a msg to arrive
            msg = yield self.queue.get()
            # wait for a core to become idle
            core = yield self.idle_cores.get()
            self.logger.log('Dispatching msg to core {}:\n\t"{}"'.format(core.ID, str(msg)))
            # put the request in the core's queue
            core.queue.put(msg)

def main():
    args = cmd_parser.parse_args()
    # Run the simulation
    run_nic_sim(args, JBSQCore, JBSQDispatcher)

if __name__ == '__main__':
    main()
