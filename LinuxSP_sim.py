#!/usr/bin/env python2

import argparse
import simpy
import pandas as pd
import numpy as np
import random

from nic_sim_lib import cmd_parser, Request, Core, Dispatcher, Logger, NicSimulator, run_nic_sim

Logger.debug = True

class LinuxSPRequest(Request):
    """Custom request class"""
    def __init__(self, *args):
        super(LinuxSPRequest, self).__init__(*args)
        # indicate which core this request should be sent to
        self.core = random.randint(0, NicSimulator.num_cores-1)

    def __lt__(self, other):
        """Highest priority element is the one with the smallest priority value"""
        return self.priority < other.priority

class LinuxSPCore(Core):
    """Core which processes requests until a higher priority request arrives, checking every XXX ns"""
    def __init__(self, *args):
        super(LinuxSPCore, self).__init__(*args)
        # override queue attribute with a priority queue
        self.queue = simpy.PriorityStore(self.env)

        # current priority of the request being processed on the core
        self.cur_priority = 0xffffffff

        # timer interrupts fire at a constant rate
        self.timer_interrupt = self.env.event()
        self.env.process(self.do_timer_interrupt())

    @staticmethod
    def init_params():
        LinuxSPCore.context_switch = NicSimulator.config['context_switch'].next()
        LinuxSPCore.timer_interrupt = NicSimulator.config['timer_interrupt'].next()

    def do_timer_interrupt(self):
        yield self.env.timeout(LinuxSPCore.timer_interrupt)
        self.logger.log('Timer interrupt fired!')
        self.timer_interrupt.succeed()
        self.timer_interrupt = self.env.event()
        # reschedule timer interrupt
        if not NicSimulator.complete:
            self.env.process(self.do_timer_interrupt())

    def service_request(self, msg):
        self.logger.log('Starting request processing at core {}:\n\t"{}"'.format(self.ID, str(msg)))
        try:
            yield self.env.timeout(msg.service_time)
            self.logger.log('Completed request processing at core {}:\n\t"{}"'.format(self.ID, str(msg)))
        except simpy.Interrupt as i:
            self.logger.log('Request processing interrupted at core {}:\n\t"{}"'.format(self.ID, str(msg)))

    def process_msg(self, msg):
        # Continue processing request until either:
        #  (1) A timer interrupt fires
        #  (2) The request service time is complete
        processing = self.env.process(self.service_request(msg))
        start = self.env.now
        yield processing | self.timer_interrupt
        if not processing.triggered:
            # Timer interrupt occurred
            processing.interrupt('Processing timer interrupt!')
            # update msg service time
            msg.service_time -= self.env.now - start
            if len(self.queue.items) > 0 and self.queue.items[0].priority < self.cur_priority:
                # Need to switch to start processing higher priority request
                # Put msg back in the queue for later processing
                self.logger.log('There is a higher priority msg to process!')
                self.queue.put(msg)
            else:
                # Continue processing current msg
                yield self.env.process(self.process_msg(msg))
        else:
            # Request processing completed
            NicSimulator.completion_times[msg.priority].append(self.env.now - msg.start_time)
            NicSimulator.request_cnt += 1
            NicSimulator.check_done(self.env.now)

    def start(self):
        while not NicSimulator.complete:
            # Receive request from queue
            msg = yield self.queue.get()
            if self.cur_priority != msg.priority:
                # need to perform a context switch
                yield self.env.timeout(LinuxSPCore.context_switch)
            self.cur_priority = msg.priority
            yield self.env.process(self.process_msg(msg))

class LinuxSPDispatcher(Dispatcher):
    """Send requests to the core indicated in the message"""
    def start(self):
        while not NicSimulator.complete:
            # wait for a msg to arrive
            msg = yield self.queue.get()
            assert msg.core < len(self.cores), "msg for an unconnected core: {}".format(msg.core)
            core = self.cores[msg.core]
            self.logger.log('Dispatching msg to core {}:\n\t"{}"'.format(core.ID, str(msg)))
            # put the request in the core's queue
            core.queue.put(msg)

def main():
    args = cmd_parser.parse_args()
    # Run the simulation
    run_nic_sim(args, LinuxSPCore, LinuxSPDispatcher, LinuxSPRequest)

if __name__ == '__main__':
    main()
