#!/usr/bin/env python2

import argparse
import simpy
import pandas as pd
import numpy as np
import random

from nic_sim_lib import cmd_parser, Request, Core, Dispatcher, Logger, NicSimulator, run_nic_sim

Logger.debug = False

class PREJBSQRequest(Request):
    """Custom request class for the preemptive JBSQ scheduling policy"""
    def __init__(self, *args):
        super(PREJBSQRequest, self).__init__(*args)
        self.update_service_time()

    @staticmethod
    def init_params():
        PREJBSQRequest.preemp = NicSimulator.config['preemp'].next()

    def update_service_time(self):
        # runtime is how long to run the request for at the core before it is preempted
        # service_time is the remaining service time for the request
        if self.service_time > PREJBSQRequest.preemp:
            self.runtime = PREJBSQRequest.preemp
            self.service_time -= PREJBSQRequest.preemp
        else:
            self.runtime = self.service_time
            self.service_time = 0

class PREJBSQCore(Core):
    """Core which processes requests until preempted"""
    @staticmethod
    def init_params():
        PREJBSQCore.comm_delay = NicSimulator.config['comm_delay'].next()

    def start(self):
        while not NicSimulator.complete:
            msg = yield self.queue.get()
            self.logger.log('Received msg at core {}:\n\t"{}"'.format(self.ID, str(msg)))
            # service the request at least once
            yield self.env.timeout(msg.runtime)
            msg.update_service_time()
            # continue servicing the request while the dispatcher queue is empty and local queue is empty
            while len(self.dispatcher.queue.items) == 0 and len(self.queue.items) == 0 and msg.runtime > 0:
                yield self.env.timeout(msg.runtime)
                msg.update_service_time()
            self.logger.log('Stopped Processing msg at core {}:\n\t"{}"'.format(self.ID, str(msg)))
            # add this core to the list of idle cores
            yield self.env.timeout(PREJBSQCore.comm_delay)
            self.dispatcher.idle_cores.put(self)
            if msg.runtime > 0:
                # the request needs to be processed for longer
                self.dispatcher.queue.put(msg)
            else:
                NicSimulator.completion_times['all'].append(self.env.now - msg.start_time)
                NicSimulator.request_cnt += 1
                NicSimulator.check_done(self.env.now)

class PREJBSQDispatcher(Dispatcher):
    """Centralized dispatcher that waits until a core becomes available"""
    @staticmethod
    def init_params():
        PREJBSQDispatcher.queue_bound = NicSimulator.config['queue_bound'].next()

    def __init__(self, *args):
        super(PREJBSQDispatcher, self).__init__(*args)
        self.idle_cores = simpy.Store(self.env)

    # override base class method
    def add_cores(self, cores):
        self.cores += cores
        # add each core to the list of idle_cores queue_bound times
        for i in range(PREJBSQDispatcher.queue_bound):
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
    run_nic_sim(args, PREJBSQCore, PREJBSQDispatcher, PREJBSQRequest)

if __name__ == '__main__':
    main()
