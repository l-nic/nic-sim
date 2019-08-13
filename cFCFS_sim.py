#!/usr/bin/env python2

import argparse
import simpy
import pandas as pd
import numpy as np
import random

from nic_sim_lib import cmd_parser, Request, Core, Dispatcher, Logger, NicSimulator

Logger.debug = False

class cFCFSCore(Core):
    """Core which processes requests to completion"""
    def __init__(self, *args):
        super(cFCFSCore, self).__init__(*args)
        # add this core to the list of idle cores
        self.dispatcher.idle_cores.put(self)

    def start(self):
        while not NicSimulator.complete:
            msg = yield self.queue.get()
            self.logger.log('Received msg at core {}:\n\t"{}"'.format(self.ID, str(msg)))
            yield self.env.timeout(msg.service_time)
            self.logger.log('Finished Processing msg at core {}:\n\t"{}"'.format(self.ID, str(msg)))
            # add this core to the list of idle cores
            self.dispatcher.idle_cores.put(self)
            NicSimulator.completion_times['all'].append(self.env.now - msg.start_time)
            NicSimulator.request_cnt += 1
            if NicSimulator.request_cnt == self.args.num_requests:
                NicSimulator.complete = True

class cFCFSDispatcher(Dispatcher):
    """Randomly dispatch requests to cores"""
    def __init__(self, *args):
        super(cFCFSDispatcher, self).__init__(*args)
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
    # Setup and start the simulation
    print 'Running Simulation ...'
    NicSimulator.out_dir = 'out/cFCFS'
    env = simpy.Environment() 
    s = NicSimulator(env, args, cFCFSCore, cFCFSDispatcher)
    env.run()
    s.dump_logs()

if __name__ == '__main__':
    main()