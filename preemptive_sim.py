#!/usr/bin/env python2

import argparse
import simpy
import pandas as pd
import numpy as np
import random

from nic_sim_lib import cmd_parser, Request, Core, Dispatcher, Logger, NicSimulator, run_nic_sim

Logger.debug = False

class PreempRequest(Request):
    """Custom request class for preemptive scheduling policy"""
    def __init__(self, *args):
        super(PreempRequest, self).__init__(*args)
        self.update_service_time()

    @staticmethod
    def init_params():
        PreempRequest.preemp = NicSimulator.config['preemp'].next()

    def update_service_time(self):
        # runtime is how long to run the request for at the core before it is preempted
        # service_time is the remaining service time for the request 
        if self.service_time > PreempRequest.preemp:
            self.runtime = PreempRequest.preemp
            self.service_time -= PreempRequest.preemp
        else:
            self.runtime = self.service_time
            self.service_time = 0

class PreempCore(Core):
    """Core which processes requests """
    def __init__(self, *args):
        super(PreempCore, self).__init__(*args)

    def start(self):
        while not NicSimulator.complete:
            msg = yield self.queue.get()
            self.logger.log('Received msg at core {}:\n\t"{}"'.format(self.ID, str(msg)))
            yield self.env.timeout(msg.runtime)
            self.logger.log('Stopped processing msg at core {}:\n\t"{}"'.format(self.ID, str(msg)))
            if msg.service_time > 0:
                # the request needs to be processed for longer
                msg.update_service_time()
                self.dispatcher.queue.put(msg)
            else:
                NicSimulator.completion_times['all'].append(self.env.now - msg.start_time)
                NicSimulator.request_cnt += 1
                NicSimulator.check_done(self.env.now)

class PreempDispatcher(Dispatcher):
    """Randomly dispatch requests to cores"""
    def __init__(self, *args):
        super(PreempDispatcher, self).__init__(*args)

    def start(self):
        while not NicSimulator.complete:
            # wait for a msg to arrive
            msg = yield self.queue.get()
            self.logger.log('Dispatching msg:\n\t"{}"'.format(str(msg)))
            # Pick a random core
            core = random.choice(self.cores)
            # put the request in the core's queue
            core.queue.put(msg)

def main():
    args = cmd_parser.parse_args()
    # Setup and run the simulation
    NicSimulator.out_dir = 'out/preemptive'
    run_nic_sim(args, PreempCore, PreempDispatcher, PreempRequest)

if __name__ == '__main__':
    main()
