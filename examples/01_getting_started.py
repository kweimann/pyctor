import asyncio
import random
from dataclasses import dataclass
from datetime import datetime

from pyctor import ActorSystem, Actor, ActorRef, Terminated


@dataclass
class Play:
    student: ActorRef  # play with this student


class Student(Actor):  # subclass of Actor
    # Every Actor must implement `receive` to receive messages.
    async def receive(self, sender, message):
        if isinstance(message, Play):
            # Student was told to play.
            # Send a message to the other student which will start the match.
            self.tell(message.student, 'ping!', delay=1)
        elif message == 'bye!':
            # The other student has stopped so this student stops as well.
            self.stop()
        elif random.random() < 0.5:
            # With a probability of 0.5 leave the playground and notify the other student.
            self.tell(sender, 'bye!')
            self.stop()
        elif message == 'ping!':
            # Respond with "pong" after 1 second.
            self.tell(sender, 'pong!', delay=1)
        elif message == 'pong!':
            # Respond with "ping" after 1 second.
            self.tell(sender, 'ping!', delay=1)
        print(f'[{datetime.now()}] {sender.name} to {self.name}: {message}')


class Teacher(Actor):
    def __init__(self):
        super().__init__()
        self.students = 0

    async def started(self):
        # Create 2 students: Mike and Sarah.
        mike = self.system.create(Student(), name='Mike')
        sarah = self.system.create(Student(), name='Sarah')
        # Get notified when students stop.
        self.watch(mike)
        self.watch(sarah)
        # Tell Mike to play with Sarah.
        self.tell(mike, Play(sarah))
        # Initialize the number of students to 2.
        self.students = 2
        print(f'[{datetime.now()}] {self.name}: it\'s playtime!')

    async def receive(self, sender, message):
        if isinstance(message, Terminated):
            # Student has stopped.
            self.students -= 1
        if self.students == 0:
            # All students have stopped so shut down.
            await self.system.shutdown()

    async def stopped(self):
        print(f'[{datetime.now()}] {self.name}: time to go back.')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        # Create the actor system.
        system = ActorSystem()
        # Create the teacher.
        system.create(Teacher(), name='Teacher')
        # Run until system is shutdown.
        loop.run_until_complete(system.stopped())
    finally:
        loop.close()
