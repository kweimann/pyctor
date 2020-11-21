import asyncio
import random
from dataclasses import dataclass
from datetime import datetime

from pyctor import ActorSystem, Actor


@dataclass
class SendWork:
    work: 'any'


@dataclass
class WorkDone:
    work: 'any'
    result: 'any'


@dataclass
class WorkFailed:
    work: 'any'
    exc: Exception


class Worker(Actor):
    """Worker does work and reports back to the master."""
    def __init__(self, master, do_work_func):
        super().__init__()
        self.master = master
        self.do_work_func = do_work_func

    async def receive(self, sender, message):
        assert sender == self.master
        if isinstance(message, SendWork):
            try:
                result = await self.do_work_func(message.work)
                response = WorkDone(message.work, result)
            except Exception as e:
                response = WorkFailed(message.work, e)
            self.tell(self.master, response)


class Master(Actor):
    """Master receives work from other actors, distributes the work
    across its workers and sends the results back."""
    def __init__(self, do_work_func, num_workers):
        super().__init__()
        self.do_work_func = do_work_func
        self.num_workers = num_workers
        self.workers = {}
        self.work_queue = []

    async def started(self):
        for i in range(self.num_workers):
            self.create_worker(name=f'worker-{i+1}')

    async def receive(self, sender, message):
        if isinstance(message, SendWork):
            # Enqueue the work.
            self.enqueue_work(sender, message)
            self.dequeue_work()
        elif isinstance(message, (WorkDone, WorkFailed)):
            # Inform the work sender about the result.
            work_sender, work_message = self.free_worker(sender)
            assert message.work == work_message.work
            self.tell(work_sender, message)
            self.dequeue_work()

    async def stopped(self):
        for worker in self.workers:
            self.system.stop(worker)

    def create_worker(self, name=None):
        worker = self.system.create(
            Worker(self.actor_ref, self.do_work_func), name=name)
        self.watch(worker)
        self.workers[worker] = None

    def enqueue_work(self, work_sender, work_message):
        self.work_queue.append((work_sender, work_message))

    def dequeue_work(self):
        while self.work_queue:
            free_worker = self.get_next_free_worker()
            if free_worker is None:
                break
            work = self.work_queue.pop(0)
            self.assign_work(free_worker, work)

    def assign_work(self, worker, work):
        assert worker in self.workers
        current_work = self.workers[worker]
        assert current_work is None
        work_sender, work_message = work
        self.tell(worker, work_message)
        self.workers[worker] = work

    def free_worker(self, worker):
        assert worker in self.workers
        work = self.workers[worker]
        assert work is not None
        self.workers[worker] = None
        return work

    def get_next_free_worker(self):
        for worker, work in self.workers.items():
            if work is None:
                return worker


# Below you can find an example actor that uses the master-worker pattern implemented above.


elements_to_count = 100
avg_work_time = 1.
p_fail = 0.1
parallelism = 10


async def count(_):
    delay = random.uniform(0, 2 * avg_work_time)
    await asyncio.sleep(delay)
    if random.random() < p_fail:
        raise Exception('count failed')
    return 1


class Counter(Actor):
    def __init__(self):
        super().__init__()
        self.counted = 0

    async def started(self):
        self.master = self.system.create(
            Master(count, num_workers=parallelism), name='master')
        print(f'[{datetime.now()}] {self.name}: Start the count!')
        for i in range(elements_to_count):
            self.tell(self.master, SendWork(i))

    async def receive(self, sender, message):
        if isinstance(message, WorkDone):
            self.counted += 1
            if self.counted == elements_to_count:
                print(f'[{datetime.now()}] {self.name}: Finished!')
                self.system.shutdown()
        elif isinstance(message, WorkFailed):
            self.tell(self.master, SendWork(message.work))


async def main():
    system = ActorSystem()
    system.create(Counter(), name='counter')
    await system.stopped()


if __name__ == '__main__':
    """
    We can estimate the average runtime of the counter using the following equation:
    
        avg_runtime = ((1 + p_fail) * elements_to_count * avg_work_time) / parallelism
    
    Plugging in the values above we get:
    
        avg_runtime = ((1 + 0.1) * 100 * 1.) / 10 = 11 seconds
    
    Note that we ignore the computational overhead of executing the code.
    """
    asyncio.run(main())
