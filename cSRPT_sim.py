#!/usr/bin/env python2

import argparse
import simpy
import pandas as pd
import numpy as np
import random

from nic_sim_lib import cmd_parser, Request, Core, Dispatcher, Logger, NicSimulator, run_nic_sim

Logger.debug = False

class cSRPTRequest(Request):
    """Custom request class for centralized SRPT scheduling policy"""
    def __init__(self, *args):
        super(cSRPTRequest, self).__init__(*args)

    def __lt__(self, other):
        """Highest priority element is the one with the smallest service time"""
        return self.service_time < other.service_time

class cSRPTCore(Core):
    """Core which processes requests to completion"""
    def __init__(self, *args):
        super(cSRPTCore, self).__init__(*args)
        # add this core to the list of idle cores
        self.dispatcher.idle_cores.put(self)

    @staticmethod
    def init_params():
        cSRPTCore.comm_delay = NicSimulator.config['comm_delay'].next()

    def start(self):
        while not NicSimulator.complete:
            msg = yield self.queue.get()
            self.logger.log('Received msg at core {}:\n\t"{}"'.format(self.ID, str(msg)))
            yield self.env.timeout(msg.service_time)
            self.logger.log('Finished Processing msg at core {}:\n\t"{}"'.format(self.ID, str(msg)))
            # add this core to the list of idle cores
            yield self.env.timeout(cSRPTCore.comm_delay)
            self.dispatcher.idle_cores.put(self)
            NicSimulator.completion_times['all'].append(self.env.now - msg.start_time)
            NicSimulator.request_cnt += 1
            NicSimulator.check_done(self.env.now)

class cSRPTDispatcher(Dispatcher):
    """Use priority queue to schedule requests"""
    def __init__(self, *args):
        super(cSRPTDispatcher, self).__init__(*args)
        # override queue attribute with a priority queue
        self.queue = simpy.PriorityStore()
        self.idle_cores = simpy.Store(self.env)

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
    # Setup and run the simulation
    NicSimulator.out_dir = 'out/cSRPT'
    run_nic_sim(args, cSRPTCore, cSRPTDispatcher, cSRPTRequest)

if __name__ == '__main__':
    main()
