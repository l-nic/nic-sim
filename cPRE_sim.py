#!/usr/bin/env python2

import argparse
import simpy
import pandas as pd
import numpy as np
import random

from nic_sim_lib import cmd_parser, Request, Core, Dispatcher, Logger, NicSimulator, run_nic_sim

Logger.debug = False

class cPRERequest(Request):
    """Custom request class for the centralized preemptive scheduling policy"""
    def __init__(self, *args):
        super(cPRERequest, self).__init__(*args)
        self.update_service_time()

    @staticmethod
    def init_params():
        cPRERequest.preemp = NicSimulator.config['preemp'].next()

    def update_service_time(self):
        # runtime is how long to run the request for at the core before it is preempted
        # service_time is the remaining service time for the request
        if self.service_time > cPRERequest.preemp:
            self.runtime = cPRERequest.preemp
            self.service_time -= cPRERequest.preemp
        else:
            self.runtime = self.service_time
            self.service_time = 0

class cPRECore(Core):
    """Core which processes requests to completion"""
    def __init__(self, *args):
        super(cPRECore, self).__init__(*args)
        # add this core to the list of idle cores
        self.dispatcher.idle_cores.put(self)

    @staticmethod
    def init_params():
        cPRECore.comm_delay = NicSimulator.config['comm_delay'].next()

    def start(self):
        while not NicSimulator.complete:
            msg = yield self.queue.get()
            self.logger.log('Received msg at core {}:\n\t"{}"'.format(self.ID, str(msg)))
            # service the request at least once
            yield self.env.timeout(msg.runtime)
            msg.update_service_time()
            # continue servicing the request while the dispatcher queue is empty
            while len(self.dispatcher.queue.items) == 0 and msg.runtime > 0:
                yield self.env.timeout(msg.runtime)
                msg.update_service_time()
            self.logger.log('Stopped Processing msg at core {}:\n\t"{}"'.format(self.ID, str(msg)))
            # add this core to the list of idle cores
            yield self.env.timeout(cPRECore.comm_delay)
            self.dispatcher.idle_cores.put(self)
            if msg.runtime > 0:
                # the request needs to be processed for longer
                self.dispatcher.queue.put(msg)
            else:
                NicSimulator.completion_times['all'].append(self.env.now - msg.start_time)
                NicSimulator.request_cnt += 1
                NicSimulator.check_done(self.env.now)

class cPREDispatcher(Dispatcher):
    """Centralized dispatcher that waits until a core becomes available"""
    def __init__(self, *args):
        super(cPREDispatcher, self).__init__(*args)
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
    NicSimulator.out_dir = 'out/cPRE'
    run_nic_sim(args, cPRECore, cPREDispatcher, cPRERequest)

if __name__ == '__main__':
    main()
