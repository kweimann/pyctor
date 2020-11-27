import asyncio
import random
from dataclasses import dataclass
from datetime import datetime

from pyctor import ActorSystem, Actor, ActorRef, Terminated


@dataclass
class Play:
    student: ActorRef  # play with this student


class Student(Actor):  # subclass of Actor
    """Students exchange ping-pong messages until one of them randomly decides to stop."""

    # Every Actor must implement `receive` to receive messages.
    async def receive(self, sender, message):
        if isinstance(message, Play):
            # Student was told to play.
            # Send a message to the other student which will start the match.
            self.tell(message.student, 'ping!')
        elif message == 'bye!':
            # The other student has stopped so this student stops as well.
            self.stop()
        elif random.random() < 0.25:
            # With a probability of 0.25 leave the playground and notify the other student.
            self.tell(sender, 'bye!')
            self.stop()
        elif message == 'ping!':
            # Respond with "pong" after 1 second.
            self.schedule_tell(sender, 'pong!', delay=1)
        elif message == 'pong!':
            # Respond with "ping" after 1 second.
            self.schedule_tell(sender, 'ping!', delay=1)
        print(f'[{datetime.now()}] {sender.name} to {self.name}: {message}')


class Teacher(Actor):
    """Teacher creates the two students and watches them.
    If students stop playing, the teacher shuts down the entire system."""

    def __init__(self):
        super().__init__()
        self.students = 0

    async def started(self):
        # Create 2 students: Mike and Sarah. Children are watched by default.
        mike = self.create(Student(), name='Mike')
        sarah = self.create(Student(), name='Sarah')
        self.students = 2
        # Tell Mike to play with Sarah.
        self.tell(mike, Play(sarah))
        print(f'[{datetime.now()}] {self}: it\'s playtime!')

    async def receive(self, sender, message):
        if isinstance(message, Terminated):
            self.students -= 1
            print(f'[{datetime.now()}] {message.actor.name} has stopped playing')
        if self.students == 0:
            # All students have stopped so shut down.
            self.system.shutdown()

    async def stopped(self):
        print(f'[{datetime.now()}] {self.name}: time to go back.')


async def main():
    # Create the actor system.
    system = ActorSystem()
    # Create the teacher.
    system.create(Teacher(), name='Teacher')
    # Run until system is shutdown.
    await system.stopped()

if __name__ == '__main__':
    asyncio.run(main())
