#!/usr/bin/env python2

import argparse
import simpy
import pandas as pd
import numpy as np
import random

from nic_sim_lib import cmd_parser, Request, Core, Dispatcher, Logger, NicSimulator, run_nic_sim

Logger.debug = False

class dFCFSCore(Core):
    """Core which processes requests to completion"""
    def start(self):
        while not NicSimulator.complete:
            msg = yield self.queue.get()
            self.logger.log('Received msg at core {}:\n\t"{}"'.format(self.ID, str(msg)))
            yield self.env.timeout(msg.service_time)
            self.logger.log('Finished Processing msg at core {}:\n\t"{}"'.format(self.ID, str(msg)))
            NicSimulator.completion_times['all'].append(self.env.now - msg.start_time)
            NicSimulator.request_cnt += 1
            NicSimulator.check_done(self.env.now)

class dFCFSDispatcher(Dispatcher):
    """Randomly dispatch requests to cores"""
    def start(self):
        while not NicSimulator.complete:
            msg = yield self.queue.get()
            self.logger.log('Dispatching msg\n\t"{}"'.format(str(msg)))
            # Pick a random core
            c = random.choice(self.cores)
            # put the request in the core's queue
            c.queue.put(msg)

def main():
    args = cmd_parser.parse_args()
    # Run the simulation
    run_nic_sim(args, dFCFSCore, dFCFSDispatcher)

if __name__ == '__main__':
    main()
