# 🎭 pyctor

Minimalistic implementation of the actor concurrency model powered by asyncio.

##

* Installation: 
  * `pip install pyctor` (requires Python `>= 3.7.3`)
* [Examples](examples)
* [API](#api)

## Overview

Pyctor is a lightweight implementation of the actor model for concurrent systems. Actors should only modify their own private state. They can influence other actors in the system through messaging. In Pyctor, we define actors by subclassing the `Actor` class:

```python
class MyActor(pyctor.Actor):
    async def receive(self, sender, message):
        print(f'received message: {message}')
```

Every actor must override the `receive` method. Inside it, the actor responds to the received messages, e.g., by modifying the internal state, sending new messages or creating new actors.

All actors live in a single ecosystem called `ActorSystem`. The actor system controls the lifecycle of every actor and handles message passing between actors. Actors may override lifecycle methods (e.g., `started`, `restarted` or `stopped`) that will be called by the system at specific points throughout actor's life. In the main function below, which serves as an entry point, we create the actor system and the first actor:

```python
async def main():
    system = pyctor.ActorSystem()  # create actor system
    my_actor = system.create(MyActor())  # create first actor
    system.tell(my_actor, 'hello')  # send message to the actor
    await system.stopped()  # await system shutdown
```

The `create` method starts an actor and returns a reference for that actor. Actors may create child actors, forming a hierarchy of actors in the system. Actors should not be modified directly. Instead, they should communicate through messages. Messages are sent with the `tell` method which expects a reference of the actor that shall receive the message. The actor system guarantees that the messages are received in the same order they were sent in.

We run the actor system by passing the main function to the `asyncio.run` method:

```python
if __name__ == '__main__':
    asyncio.run(main())
```

You can find further examples in the [examples](examples) directory. 

## API

* `pyctor.ActorSystem`
  * `create(actor, name=None)`<br/>
  Start an actor.
  * `tell(actor, message)`<br/>
  Deliver a message to an actor.
  * `schedule_tell(actor, message, *, delay=None, period=None)`<br/>
  Schedule a message to be delivered to an actor at some time. The message may also be delivered periodically.
  * `stop(actor)`<br/>
  Stop an actor.
  * `shutdown(timeout=None)`<br/>
  Initialize a shutdown of the actor system.
  * `stopped()`<br/>
  Await system shutdown.

* `pyctor.Actor`
  * `receive(sender, message)`<br/>
  This method is called by the system every time a message is received by the actor. It must be implemented by the class inheriting from Actor.
  * `create(actor, name=None)`<br/>
  Start a child actor. The child actor will be watched.
  * `tell(actor, message)`<br/>
  Deliver a message to another actor.
  * `schedule_tell(actor, message, *, delay=None, period=None)`<br/>
  Schedule a message to be delivered to another actor at some time. The message may also be delivered periodically.
  * `watch(actor)`<br/>
  Watch another actor and receive a `Terminated(actor)` message when the watched actor stops.
  * `unwatch(actor)`<br/>
  Stop watching another actor.
  * `stop()`<br/>
  Stop the actor.

  Lifecycle callbacks:
  
  * `started()`<br/>
  Called by the system before the actor starts to receive messages.
  * `restarted(sender, message, error)`<br/>
  Called by the system if receiving the message caused an error. Actor will continue to receive messages.
  * `stopped()`<br/>
  Called by the system before stopping the actor. The actor will not receive messages anymore.

* System messages:
  * `PoisonPill()`<br/>
  Stop an actor.
  * `Terminated(actor)`<br/>
  Notify that the actor has stopped.
  * `DeadLetter(actor, message)`<br/>
  Notify the sender that the actor did not receive the message.