import os
import json
import time
import socket


class Calculator(object):

    def __init__(self, addr):
        self.addr = addr
        self.is_free = True
        self.task_received = None
        self.timeout = False
        self.data = None
        self._client = None

    def clear_data(self):
        self.client = None
        self.task_received = None

    @property
    def client(self):
        return self._client

    @client.setter
    def client(self, value):
        if value is None:
            self._client = None
            self.data = None
            self.is_free = True
        else:
            self._client = value
            self.is_free = False


class Dispatcher(object):

    calcs = set()
    queue = []

    def __init__(self):
        with open(os.path.join('config', 'dispatcher.json')) as f:
            for attr, value in json.load(f).iteritems():
                # Setting following attrs: self.port, self.timeout
                setattr(self, attr, value)
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setblocking(0)
        self.socket.bind(('', self.port))
        print 'waiting on port: %s' % self.port

    def register_calculator(self, addr):
        """Register new calculator in dispatcher registry."""
        is_registered = self.get_calculator_by_addr(addr) or None
        if is_registered is not None:
            print 'Already registered calculator'
        else:
            self.calcs.add(Calculator(addr))
            print 'Registered new calculator on %s:%s' % (addr[0], addr[1])

    def get_calculator_by_addr(self, addr):
        """Returns Calculator object by it's address or None."""
        try:
            return next(c for c in self.calcs if c.addr == addr)
        except StopIteration:
            return

    def get_free_calculator(self):
        """Returns Calculator object which is free and not timed out or None.
        """
        try:
            return next(c for c in self.calcs if c.is_free and not c.timeout)
        except StopIteration:
            return

    def get_num_of_unfinished_requests(self):
        """Returns the number of requests which were passed to calculators but
        response from calculators wasn't received.
        """
        return len([c for c in self.calcs if not c.is_free])

    def get_timed_out_calcs(self):
        """Returns list of timed out calculators."""
        cur_time = time.time()
        busy_calcs = [c for c in self.calcs if not c.is_free]
        return [c for c in busy_calcs if cur_time - c.task_received > self.timeout]

    def handle_calculator_echo(self, data, addr):
        """Handle calculators echo messages like introducing and *i'm alive*
        signals.
        """
        if data.startswith('CALCULATOR:'):
            self.register_calculator(addr)
        elif data == 'CALCULATOR IS ALIVE':
            calc = self.get_calculator_by_addr(addr)
            # Check if we recieved echo from timed out calculator and if so -
            # reset it state.
            if calc is not None and calc.timeout:
                print 'Look at it! %s is no longer broken!' % addr[1]
                calc.timeout = False
                calc.clear_data()
            elif calc is not None:
                print 'Got alive notification from calculator on port %s' % addr[1]

    def handle_client_response(self, calculator, data):
        """Returns result from calculator to client."""
        print 'Task finished, response to client!'
        data = data + ' for client %s' % calculator.client[1]
        self.socket.sendto(data, calculator.client)
        calculator.clear_data()  # Calculations finished, calculator is free.

    def handle_client_request(self, data, addr):
        """Handle clients requests by passing them to available calculators."""
        calculator = self.get_free_calculator()
        if calculator is not None:
            calculator.client = addr
            calculator.data = data
            calculator.task_received = time.time()
            self.socket.sendto(calculator.data, calculator.addr)
        else:
            print 'task placed in queue'
            self.queue.append((data, addr))

    def handle_queue(self):
        """Check queue for tasks occurences if so - proccess them."""
        if not self.queue:
            return
        calculator = self.get_free_calculator()
        if calculator is not None:
            task = self.queue.pop(0)  # tuple: (data, client_addr)
            print 'Task from queue'
            self.handle_client_request(*task)

    def handle_timed_out_calcs(self):
        """Check for timed out calculators and pass task from timed out
        calculator to available one or put task in queue.'
        """
        if not self.calcs:
            return
        timed_out = self.get_timed_out_calcs()
        if timed_out:
            for c in timed_out:
                print 'Got broken calculator! %s' % c.addr[1]
                c.timeout = True
                self.handle_client_request(c.data, c.client)
                c.clear_data()

    def print_stat(self):
        print '\n'
        print 'Requests in queue: %s' % len(self.queue)
        print 'Number of unfinished requests: %s' % self.get_num_of_unfinished_requests()
        print '\n'

    def run(self):
        try:
            while True:
                time.sleep(0.05) # Have mercy on CPU.
                try:
                    data, addr = self.socket.recvfrom(1024)
                    if data:
                        if data.startswith('CALCULATOR'):
                            self.handle_calculator_echo(data, addr)
                        elif data.startswith('TASK:'):
                            print 'Task received!'
                            self.handle_client_request(data, addr)
                        elif data.startswith('TASK FINISHED:'):
                            calculator = self.get_calculator_by_addr(addr)
                            self.handle_client_response(calculator, data)

                except socket.error:
                    self.handle_timed_out_calcs()
                    self.handle_queue()
        except KeyboardInterrupt:
            self.print_stat()
            raise


if __name__ == '__main__':
    Dispatcher().run()



