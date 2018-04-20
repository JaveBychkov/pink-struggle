import os
import random
import time
import json
import socket


class Calculator(object):

    def __init__(self, port=None):

        with open(os.path.join('config', 'calculator.json')) as f:
            for attr, value in json.load(f).iteritems():
                # Setting following attrs: self.dispatcher_host,
                # self.dispatcher_port, self.port, self.sleep_min,
                # self.sleep_max, self.notify_interval, self.break_chance,
                # self.break_time
                setattr(self, attr, value)

        self.port = self.port if port is None else port
        self.disp_addr = (self.dispatcher_host, self.dispatcher_port)
    
        self.last_notification = None

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setblocking(0)
        self.socket.bind(('', self.port))

        self.hello_dispatcher()

    def is_broken(self):
        """Returns True if calculator is broken, False otherwise."""
        rnd = random.randint(1, 100)
        return self.break_chance >= rnd

    def notify_dispatcher(self):
        """Sends *i'm alive* signal to dispatcher."""
        self.socket.sendto('CALCULATOR IS ALIVE', self.disp_addr)
        self.last_notification = time.time()

    def hello_dispatcher(self):
        """Sends *hello* message to dispatcher to register itself as calculator.
        """
        message = 'CALCULATOR: Hello!'
        self.socket.sendto(message, self.disp_addr)
        self.last_notification = time.time()

    def run(self):
        print 'Ready to recieve messages at %s' % self.port
        while True:
            time.sleep(0.05) # Have mercy on CPU.
            try:
                data, _ = self.socket.recvfrom(1024)
                if data:
                    _, _, task_number = data.partition('#')
                    print 'Task #%s received' % task_number

                    # Emalute complex computations.
                    sleep_time = random.randrange(
                        self.sleep_min, self.sleep_max
                    )
                    time.sleep(sleep_time)
                    # If broken: don't notify dispatcher about finished task.
                    if self.is_broken():
                        print 'Ooops... Broken'
                        time.sleep(self.break_time)
                    else:
                        self.socket.sendto(
                            'TASK FINISHED: #%s in %s' % (task_number, sleep_time),
                            self.disp_addr)
                        self.last_notification = time.time()
            except socket.error:
                if time.time() - self.last_notification > self.notify_interval:
                    self.notify_dispatcher()



if __name__ == '__main__':
    from optparse import OptionParser
    # More easier way to run multiple calculators.
    parser = OptionParser()
    parser.add_option('-p', '--port', dest='port', type='int', default=None)
    options, _ = parser.parse_args()
    Calculator(options.port).run()

