#!/usr/bin/env python2

import argparse
import simpy
import pandas as pd
import numpy as np
import random

from nic_sim_lib import cmd_parser, Request, Core, Dispatcher, Logger, NicSimulator, run_nic_sim

Logger.debug = False

class cPRESRPTRequest(Request):
    """Custom request class for centralized SRPT scheduling policy"""
    def __init__(self, *args):
        super(cPRESRPTRequest, self).__init__(*args)
        self.update_service_time()

    @staticmethod
    def init_params():
        cPRESRPTRequest.preemp = NicSimulator.config['preemp'].next()

    def update_service_time(self):
        # runtime is how long to run the request for at the core before it is preempted
        # service_time is the remaining service time for the request
        if self.service_time > cPRESRPTRequest.preemp:
            self.runtime = cPRESRPTRequest.preemp
            self.service_time -= cPRESRPTRequest.preemp
        else:
            self.runtime = self.service_time
            self.service_time = 0

    def __lt__(self, other):
        """Highest priority element is the one with the smallest total service time"""
        return self.runtime + self.service_time < other.runtime + other.service_time

class cPRESRPTCore(Core):
    """Core which processes requests until preempted"""
    def __init__(self, *args):
        super(cPRESRPTCore, self).__init__(*args)
        # add this core to the list of idle cores
        self.dispatcher.idle_cores.put(self)

    @staticmethod
    def init_params():
        cPRESRPTCore.comm_delay = NicSimulator.config['comm_delay'].next()

    def start(self):
        while not NicSimulator.complete:
            msg = yield self.queue.get()
            self.logger.log('Received msg at core {}:\n\t"{}"'.format(self.ID, str(msg)))
            # service the request at least once
            yield self.env.timeout(msg.runtime)
            msg.update_service_time()
            # Continue servicing the request while either the following two conditions are met:
            #  1. The dispatcher queue is empty and this msg still needs to be serviced
            #  2. The dispatcher queue is not empty and this msg is higher priority than the msg at the head of the queue
            while (len(self.dispatcher.queue.items) == 0 and msg.runtime > 0)
                     or (len(self.dispatcher.queue.items) > 0 and msg < self.dispatcher.queue.items[0] and msg.runtime > 0):
                yield self.env.timeout(msg.runtime)
                msg.update_service_time()
            self.logger.log('Stopped Processing msg at core {}:\n\t"{}"'.format(self.ID, str(msg)))
            # add this core to the list of idle cores
            yield self.env.timeout(cPRESRPTCore.comm_delay)
            self.dispatcher.idle_cores.put(self)
            if msg.runtime > 0:
                # the request needs to be processed for longer
                self.dispatcher.queue.put(msg)
            else:
                NicSimulator.completion_times['all'].append(self.env.now - msg.start_time)
                NicSimulator.request_cnt += 1
                NicSimulator.check_done(self.env.now)

class cPRESRPTDispatcher(Dispatcher):
    """Use priority queue to schedule requests"""
    def __init__(self, *args):
        super(cPRESRPTDispatcher, self).__init__(*args)
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
    NicSimulator.out_dir = 'out/cPRESRPT'
    run_nic_sim(args, cPRESRPTCore, cPRESRPTDispatcher, cPRESRPTRequest)

if __name__ == '__main__':
    main()
