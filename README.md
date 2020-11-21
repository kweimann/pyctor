# 🎭 pyctor

Minimalistic implementation of the actor concurrency model powered by asyncio.

---

* Installation: `pip install pyctor` (requires Python `>= 3.7.3`)
* [API](#api)

## Example

Imagine a teacher and two students, Sarah and Mike, at a playground ready to play a round of ping-pong. The teacher throws the ball to Mike and the match between the two kids begins. Should one of them decide they've had enough and leave the playground, the other one will follow. Meanwhile, the teacher watches the students and leaves with them once they've finished playing.

Let's start with the implementation of a student, i.e., Sarah and Mike. The students exchange ping-pong messages until one of them randomly decides to stop:

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
            self.system.shutdown()

    async def stopped(self):
        print(f'[{datetime.now()}] {self.name}: time to go back.')
```

Finally, we implement the top-level entry point `main()` function that creates the actor system, the teacher, and waits for the system to shut down.

```python
import asyncio
import random
from dataclasses import dataclass
from datetime import datetime

from pyctor import ActorSystem, Actor, ActorRef, Terminated

# [Play code omitted]

# [Student code omitted]

# [Teacher code omitted]

async def main():
    # Create the actor system.
    system = ActorSystem()
    # Create the teacher.
    system.create(Teacher(), name='Teacher')
    # Run until system is shutdown.
    await system.stopped()

if __name__ == '__main__':
    asyncio.run(main())
```

An output of this script could be:

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
  * `tell(actor, message)` Deliver a message to an actor.
  * `schedule_tell(actor, message, *, delay=None, period=None)` Schedule a message to be delivered to an actor at some time.
  * `stop(actor)` Stop an actor.
  * `shutdown(timeout=None)` Initialize a shutdown of the actor system.
  * `stopped()` Await system shutdown.

* `pyctor.Actor`
  * `receive(sender, message)` Must be implemented by the class inheriting from Actor. It is used by the system to deliver the messages.
  * `tell(actor, message)` Deliver a message to another actor.
  * `schedule_tell(actor, message, *, delay=None, period=None)` Schedule a message to be delivered to another actor at some time.
  * `watch(actor)` Watch another actor and receive a `Terminated(actor)` message when the watched actor stops.
  * `unwatch(actor)` Stop watching another actor.
  * `stop()` Stop the actor.
  * `started()` Called by the system before the actor starts to receive messages.
  * `restarted(sender, message, error)` Called by the system if receiving the message caused an error. Actor will continue to receive messages.
  * `stopped()` Called by the system before stopping the actor. The actor will not receive messages anymore.

* System messages:
  * `PoisonPill()` kills an actor.
  * `Terminated(actor)` is sent to all watchers when the actor stops.
  * `DeadLetter(actor, message)` is sent back to the sender if the actor was unable to receive the message.