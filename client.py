import os
import socket
import time
import json


class Task(object):
    
    tasks = 0

    def __init__(self):
        Task.tasks += 1  # Very naive.
        self.number = Task.tasks
        self.sent_at = time.time()
        self.done_at = None
        self.time_spent = None

    def __repr__(self):
        return 'TASK: some task #%s' % self.number


class Client(object):
    
    tasks_sent = []

    def __init__(self):
        with open(os.path.join('config', 'client.json')) as f:
            for attr, value in json.load(f).iteritems():
                # Setting following attrs: self.dispatcher_host,
                # self.dispatcher_port, self.frequency, self.max_tasks
                setattr(self, attr, value)

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setblocking(0)
        self.disp_addr = (self.dispatcher_host, self.dispatcher_port)
        self.last_task = None

    def finish_task(self, data):
        _, _, number_time = data.partition('#')
        number, _ = number_time.split(' in ')
        task = [t for t in self.tasks_sent if t.number == int(number)][0]
        task.done_at = time.time()
        task.time_spent = task.done_at - task.sent_at

    def get_finished_tasks(self):
        return [t for t in self.tasks_sent if t.done_at is not None]

    def num_of_finished_tasks(self):
        return len(self.get_finished_tasks())

    def num_of_unfinished_tasks(self):
        return len(self.tasks_sent) - self.num_of_finished_tasks()

    def max_time_spent(self):
        return max(t.time_spent for t in self.get_finished_tasks())

    def min_time_spent(self):
        return min(t.time_spent for t in self.get_finished_tasks())

    def average_time_spent(self):
        tasks = self.get_finished_tasks()
        return sum(t.time_spent for t in tasks) / len(tasks)

    def print_stat(self):
        print '\n'
        print 'Number of finished tasks: %s' % self.num_of_finished_tasks()
        print 'Number of unfinished tasks: %s' % self.num_of_unfinished_tasks()
        print 'Max response time for a single task: %s' % self.max_time_spent()
        print 'Min response time for a single task: %s' % self.min_time_spent()
        print 'Average response time: %s' % self.average_time_spent()
        print '\n'

    def run(self):
        try:
            while True:
                time.sleep(0.05) # Have mercy on CPU.
                try:
                    data, _ = self.socket.recvfrom(1024)
                    if data:
                        print 'Response from dispatcher: %s' % data
                        self.finish_task(data)
                    # Comment out next 2 lines to remove task limit. 1/2.
                    if self.num_of_finished_tasks() == self.max_tasks:
                        break
                except socket.error:
                    if self.last_task is None or time.time() - self.last_task > self.frequency:
                        # Comment out next 2 lines to remove task limit. 2/2.
                        if len(self.tasks_sent) == self.max_tasks:
                            continue
                        task = Task()
                        self.socket.sendto(str(task), self.disp_addr)
                        self.tasks_sent.append(task)
                        self.last_task = time.time()
        except KeyboardInterrupt:
            self.print_stat()
            raise
        self.print_stat()


if __name__ == '__main__':
    Client().run()