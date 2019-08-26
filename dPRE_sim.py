#!/usr/bin/env python2

import argparse
import simpy
import pandas as pd
import numpy as np
import random

from nic_sim_lib import cmd_parser, Request, Core, Dispatcher, Logger, NicSimulator, run_nic_sim

Logger.debug = False

class dPRERequest(Request):
    """Custom request class for preemptive scheduling policy"""
    def __init__(self, *args):
        super(dPRERequest, self).__init__(*args)
        self.update_service_time()

    @staticmethod
    def init_params():
        dPRERequest.preemp = NicSimulator.config['preemp'].next()

    def update_service_time(self):
        # runtime is how long to run the request for at the core before it is preempted
        # service_time is the remaining service time for the request 
        if self.service_time > dPRERequest.preemp:
            self.runtime = dPRERequest.preemp
            self.service_time -= dPRERequest.preemp
        else:
            self.runtime = self.service_time
            self.service_time = 0

class dPRECore(Core):
    """Core which processes requests """
    def __init__(self, *args):
        super(dPRECore, self).__init__(*args)

    def start(self):
        while not NicSimulator.complete:
            msg = yield self.queue.get()
            self.logger.log('Received msg at core {}:\n\t"{}"'.format(self.ID, str(msg)))
            # service the msg
            yield self.env.timeout(msg.runtime)
            msg.update_service_time()
            self.logger.log('Stopped processing msg at core {}:\n\t"{}"'.format(self.ID, str(msg)))
            if msg.runtime > 0:
                # the request needs to be processed for longer
                self.dispatcher.queue.put(msg)
            else:
                NicSimulator.completion_times['all'].append(self.env.now - msg.start_time)
                NicSimulator.request_cnt += 1
                NicSimulator.check_done(self.env.now)

class dPREDispatcher(Dispatcher):
    """Randomly dispatch requests to cores"""
    def __init__(self, *args):
        super(dPREDispatcher, self).__init__(*args)

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
    # Run the simulation
    run_nic_sim(args, dPRECore, dPREDispatcher, dPRERequest)

if __name__ == '__main__':
    main()
