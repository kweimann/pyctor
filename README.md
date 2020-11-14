# 🎭 pyctor

Minimalistic implementation of an actor concurrency model powered by asyncio.

---

* Installation: `pip install pyctor` (requires Python `>= 3.7.3`)
* [API](#api)

## Example

Imagine a teacher and two students, Sarah and Mike, going to a playground to play a round of ping-pong. The teacher throws the ball to Mike, and so the match between the two kids begins. Should one of them decide they've had enough and leave the playground, the other one will follow. Meanwhile, the teacher watches the students and leaves with them once they're done. Cut.

Let's start with the implementation of a student i.e. Sarah and Mike. The students exchange ping-pong messages until one of them randomly decides to stop:

```python
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
```

Upon creation, the teacher creates the two students and asks the system to send a `Terminated` message if a student stops. If no students are left, the teacher shuts down the entire system:

```python
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
```

Finally, the script first creates the actor system and the teacher, then waits for the system to shut down.

```python
import asyncio
import random
from dataclasses import dataclass
from datetime import datetime

from pyctor import ActorSystem, Actor, ActorRef, Terminated

# [Play code omitted]

# [Student code omitted]

# [Teacher code omitted]

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
```

An output of the script could be:

```
[2020-06-21 12:53:41.598117] Teacher: it's playtime!
[2020-06-21 12:53:41.598117] Teacher to Mike: Play(student=Sarah)
[2020-06-21 12:53:42.598700] Mike to Sarah: ping!
[2020-06-21 12:53:43.600073] Sarah to Mike: pong!
[2020-06-21 12:53:44.601047] Mike to Sarah: ping!
[2020-06-21 12:53:44.602062] Sarah to Mike: bye!
[2020-06-21 12:53:44.603047] Teacher: time to go back.
```

## API

Quick overview of the API:

* `pyctor.ActorSystem`
  * `create(actor, name=None)` Start an actor.
  * `tell(actor, message, *, delay=None, period=None)` Deliver a message to an actor.
  * `stop(actor)` Stop an actor.
  * `shutdown(timeout=None)` Initialize a shutdown of the actor system.
  * `stopped()` Await system shutdown.

* `pyctor.Actor`
  * `receive(sender, message)` Must be implemented by the class inheriting from Actor. It is used by the system to deliver the messages.
  * `tell(actor, message, *, delay=None, period=None)` Deliver a message to another actor.
  * `watch(actor)` Watch another actor and receive a `Terminated(actor)` message when the watched actor is terminated.
  * `unwatch(actor)` Stop watching another actor.
  * `stop()` Stop the actor.
  * `started()` Called by the system before the actor starts to receive messages.
  * `stopped()` Called by the system before stopping the actor. The actor will not receive messages anymore.

* system messages:
  * `PoisonPill()` Kills an actor.
  * `Terminated(actor)` Sent to all watchers when the actor dies.
  * `DeadLetter(actor, message)` Sent back to the sender if the actor was unable to receive the message.