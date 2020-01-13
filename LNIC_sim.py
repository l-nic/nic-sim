#!/usr/bin/env python2

import argparse
import simpy
import pandas as pd
import numpy as np
import random

from nic_sim_lib import cmd_parser, Request, Core, Dispatcher, Logger, NicSimulator, run_nic_sim

Logger.debug = True

class LNICRequest(Request):
    """Custom request class for LNIC requests"""
    def __init__(self, *args):
        super(LNICRequest, self).__init__(*args)
        # indicate which core this request should be sent to
        self.core = random.randint(0, NicSimulator.num_cores-1)

    def __lt__(self, other):
        """Highest priority element is the one with the smallest priority value"""
        return self.priority < other.priority

class LNICCore(Core):
    """Core which processes requests until a higher priority request arrives"""
    def __init__(self, *args):
        super(LNICCore, self).__init__(*args)
        # override queue attribute with a priority queue
        self.queue = simpy.PriorityStore(self.env)

        # current priority of the request being processed on the core
        self.cur_priority = 0xffffffff

        # create an event which will be triggered when a msg of higher priority
        # than the one currently being processed is enqueued
        self.high_priority_enq = self.env.event()

    @staticmethod
    def init_params():
        LNICCore.context_switch = NicSimulator.config['context_switch'].next()

    def process_request(self, msg):
        self.logger.log('Starting request processing at core {}:\n\t"{}"'.format(self.ID, str(msg)))
        try:
            yield self.env.timeout(msg.service_time)
            self.logger.log('Completed request processing at core {}:\n\t"{}"'.format(self.ID, str(msg)))
        except simpy.Interrupt as i:
            self.logger.log('Request processing interrupted at core {}:\n\t"{}"'.format(self.ID, str(msg)))

    def check_top_priority(self):
        # check if a higher priority msg has arrived
        if self.queue.items[0].priority < self.cur_priority:
            self.high_priority_enq.succeed()
            self.high_priority_enq = self.env.event()

    def start(self):
        while not NicSimulator.complete:
            # Receive request from queue
            # Continue processing request until either:
            #  (1) A higher priority request arrives, the current request must be preempted and put back into the queue with updated service time
            #  (2) The request service time is complete
            msg = yield self.queue.get()
            if self.cur_priority != msg.priority:
                # need to perform a context switch
                yield self.env.timeout(LNICCore.context_switch)
            self.cur_priority = msg.priority
            processing = self.env.process(self.process_request(msg))
            start = self.env.now
            yield processing | self.high_priority_enq
            if not processing.triggered:
                # Interrupt low priority request processing
                processing.interrupt('Higher priority msg arrival!')
                # Update msg service time and put it back in the queue
                msg.service_time -= self.env.now - start
                self.queue.put(msg)
            else:
                # Request processing completed
                NicSimulator.completion_times[msg.priority].append(self.env.now - msg.start_time)
                NicSimulator.request_cnt += 1
                NicSimulator.check_done(self.env.now)

class LNICDispatcher(Dispatcher):
    """Send requests to the core indicated in the message"""
    def __init__(self, *args):
        super(LNICDispatcher, self).__init__(*args)
        # TODO(sibanez): do I need this here?

    def start(self):
        while not NicSimulator.complete:
            # wait for a msg to arrive
            msg = yield self.queue.get()
            assert msg.core < len(self.cores), "msg for an unconnected core: {}".format(msg.core)
            core = self.cores[msg.core]
            self.logger.log('Dispatching msg to core {}:\n\t"{}"'.format(core.ID, str(msg)))
            # put the request in the core's queue
            core.queue.put(msg)
            core.check_top_priority()

def main():
    args = cmd_parser.parse_args()
    # Run the simulation
    run_nic_sim(args, LNICCore, LNICDispatcher, LNICRequest)

if __name__ == '__main__':
    main()
