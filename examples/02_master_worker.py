import asyncio
import random
from dataclasses import dataclass
from datetime import datetime

from pyctor import ActorSystem, Actor, ActorRef, Terminated


@dataclass
class WorkerReady:
    pass


@dataclass
class SendWork:
    work: 'any'
    send_to: ActorRef = None


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

    def __init__(self, master):
        super().__init__()
        self.master = master

    async def started(self):
        self.tell(self.master, WorkerReady())

    async def receive(self, sender, message):
        if isinstance(message, SendWork):
            asyncio.create_task(self.do_work_report_back(message.work))
        elif isinstance(message, (WorkDone, WorkFailed)):
            self.tell(self.master, message)
            self.tell(self.master, WorkerReady())

    async def do_work_report_back(self, work):
        try:
            result = await self.do_work(work)
            self.tell(self, WorkDone(work, result))
        except Exception as e:
            self.tell(self, WorkFailed(work, e))

    async def do_work(self, work):
        raise NotImplementedError


class Master(Actor):
    """Master receives work from other actors, distributes the work
    across its workers and sends the results back."""

    def __init__(self, worker_init, num_workers):
        super().__init__()
        self.worker_init = worker_init
        self.num_workers = num_workers
        self.ready_workers = set()
        self.busy_workers = {}
        self.work_queue = []

    async def started(self):
        for i in range(self.num_workers):
            self.create(
                self.worker_init(self.actor_ref),
                name=f'Worker-{i+1}')

    async def receive(self, sender, message):
        if isinstance(message, WorkerReady):
            assert sender not in self.busy_workers, sender
            if self.work_queue:
                work_sender, work = self.work_queue.pop(0)
                self.tell(sender, SendWork(work))
                self.busy_workers[sender] = (work_sender, work)
            else:
                self.ready_workers.add(sender)
        if isinstance(message, SendWork):
            work_sender = message.send_to or sender
            if self.work_queue or not self.ready_workers:
                self.work_queue.append((work_sender, message.work))
            elif self.ready_workers:
                worker = self.ready_workers.pop()
                self.tell(worker, message)
                self.busy_workers[worker] = (work_sender, message.work)
        elif isinstance(message, (WorkDone, WorkFailed)):
            assert sender in self.busy_workers
            work_sender, work = self.busy_workers.pop(sender)
            assert message.work == work
            self.tell(work_sender, message)
        elif isinstance(message, Terminated):
            if sender in self.ready_workers:
                self.ready_workers.remove(sender)
            elif sender in self.busy_workers:
                work_sender, work = self.busy_workers.pop(sender)
                self.tell(self, SendWork(work, send_to=work_sender))


# Below you can find an example actor that uses the master-worker pattern implemented above.


elements_to_count = 1000
avg_work_time = 0.1
p_fail = 0.1
parallelism = 10


class CountWorker(Worker):
    async def do_work(self, _):
        delay = random.uniform(0, 2 * avg_work_time)
        await asyncio.sleep(delay)  # simulate processing
        if random.random() < p_fail:
            raise Exception('count failed')
        return 1


class Counter(Actor):
    def __init__(self):
        super().__init__()
        self.counted = 0
        self.failed = 0

    async def started(self):
        self.master = self.create(
            Master(CountWorker, num_workers=parallelism),
            name='Master')
        print(f'[{datetime.now()}] {self.name}: Start the count!')
        for i in range(elements_to_count):
            self.tell(self.master, SendWork(i))

    async def receive(self, sender, message):
        if isinstance(message, WorkDone):
            self.counted += 1
            if self.counted == elements_to_count:
                print(f'[{datetime.now()}] {self.name}: Finished! '
                      f'Fail rate: {self.failed / self.counted:.2%}')
                self.system.shutdown()
        elif isinstance(message, WorkFailed):
            self.failed += 1
            self.tell(self.master, SendWork(message.work))


async def main():
    system = ActorSystem()
    system.create(Counter(), name='Counter')
    await system.stopped()

if __name__ == '__main__':
    avg_runtime = ((1 + p_fail) * elements_to_count * avg_work_time) / parallelism
    print(f'Expected runtime: {avg_runtime} seconds')
    asyncio.run(main())
