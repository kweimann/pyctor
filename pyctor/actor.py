__all__ = ('ActorRef', 'Actor', 'ActorSystem', 'PoisonPill', 'DeadLetter', 'Terminated')

import asyncio
import logging
import uuid
from dataclasses import dataclass
from typing import Union


@dataclass(repr=False, eq=False, frozen=True)
class ActorRef:  # serializable reference which represents an Actor
    actor_id: str
    name: str
    def __eq__(self, other): return isinstance(other, ActorRef) and self.actor_id == other.actor_id
    def __hash__(self): return hash(self.actor_id)
    def __repr__(self): return self.name
    def __str__(self): return self.name


class Actor:  # inheritable Actor class
    # noinspection PyTypeChecker
    def __init__(self):
        self.actor_ref: ActorRef = None
        self.system: ActorSystem = None

    async def receive(
            self,
            sender: ActorRef,
            message: 'any'
    ) -> None:
        """Called every time a message is delivered.

        Args:
            sender: Actor that sent the message.
            message: Received message.

        Returns:
            None
        """
        raise NotImplementedError

    # noinspection PyProtectedMember
    def tell(
            self,
            actor: Union['Actor', ActorRef],
            message: 'any'
    ) -> None:
        """Deliver a message to an actor.

        Args:
            actor: Actor that will receive a message from the system.
            message: Message to another actor.

        Returns:
            None
        """
        self.system._tell(
            actor=actor,
            message=message,
            sender=self.actor_ref)

    # noinspection PyProtectedMember
    def schedule_tell(
            self,
            actor: Union['Actor', ActorRef],
            message: 'any',
            *,
            delay: Union[None, int, float] = None,
            period: Union[None, int, float] = None
    ) -> asyncio.Task:
        """Schedule a message to be delivered to an actor at some time.

        Args:
            actor: Actor that will receive a message from the system.
            message: Message to another actor.
            delay: Message will be delivered after a delay.
            period: Resend message periodically.

        Returns:
            Delivery task that can be cancelled.
        """
        return self.system._schedule_tell(
            actor=actor,
            message=message,
            sender=self.actor_ref,
            delay=delay,
            period=period)

    # noinspection PyProtectedMember
    def watch(
            self,
            actor: ActorRef
    ) -> None:
        """Watch another actor to get messaged when they are terminated.

        When the watched actor dies, this actor will receive a Terminated(actor) message.

        Args:
            actor: Actor to watch.

        Returns:
            None
        """
        self.system._watch(
            actor=self.actor_ref,
            other=actor)

    # noinspection PyProtectedMember
    def unwatch(
            self,
            actor: ActorRef
    ) -> None:
        """Stop watching another actor.

        This function can be called even if the actor is not watched anymore.

        Args:
            actor: Watched actor.

        Returns:
            None
        """
        self.system._unwatch(
            actor=self.actor_ref,
            other=actor)

    def stop(self) -> None:
        """Asynchronously stop this actor.

        Returns:
            None
        """
        self.system.stop(actor=self.actor_ref)

    async def started(self) -> None:
        """Called by the system to let this actor know that they will start receiving messages now.

        Returns:
            None
        """
        pass

    # noinspection PyMethodMayBeStatic
    async def restarted(
            self,
            sender: ActorRef,
            message: 'any',
            error: Exception
    ) -> None:
        """Called by the system if receiving the message caused an error.

        Args:
            sender: Actor that sent the message.
            message: Received message.
            error: Raised exception.

        Returns:
            None
        """
        logging.exception('%s failed to receive message %s from %s',
                          self, message, sender, exc_info=error)

    async def stopped(self) -> None:
        """Called by the system to let this actor know that they will not receive messages anymore.

        Returns:
            None
        """
        pass

    @property
    def name(self) -> str:
        return self.actor_ref.name

    def __str__(self): return self.name
    def __repr__(self): return self.name


class ActorSystem:  # ActorSystem controls all the actors
    def __init__(self):
        self._actors = {}
        self._stopped = asyncio.Event()

    def create(
            self,
            actor: Actor,
            name: str = None
    ) -> ActorRef:
        """Initialize and asynchronously start an actor.

        Args:
            actor: Actor instance to start.
            name: Name of the actor.

        Returns:
            Actor reference.

        Examples:
            Start an actor by passing the actor instance to this function.

            >> class MyActor(Actor):
            ...    async def receive(self, sender, message): pass

            >> my_actor = ActorSystem().create(MyActor())
        """
        if not isinstance(actor, Actor):
            raise ValueError(f'Not an actor: {actor}')
        actor_id = uuid4()
        if not name:
            name = f'{type(actor).__name__}-{actor_id}'
        actor_ref = ActorRef(actor_id, name)
        actor.actor_ref = actor_ref
        actor.system = self
        actor_ctx = _ActorContext()
        actor_ctx.lifecycle = asyncio.get_event_loop().create_task(
            self._actor_lifecycle_loop(actor, actor_ref, actor_ctx))
        self._actors[actor_ref] = actor_ctx
        return actor_ref

    def tell(
            self,
            actor: Union[Actor, ActorRef],
            message: 'any'
    ) -> None:
        """Deliver a message to an actor.

        Args:
            actor: Actor that will receive a message from the system.
            message: Message to another actor.

        Returns:
            None
        """
        self._tell(
            actor=actor,
            message=message)

    def schedule_tell(
            self,
            actor: Union[Actor, ActorRef],
            message: 'any',
            *,
            delay: Union[None, int, float] = None,
            period: Union[None, int, float] = None
    ) -> asyncio.Task:
        """Schedule a message to be delivered to an actor at some time.

        Args:
            actor: Actor that will receive a message from the system.
            message: Message to another actor.
            delay: Message will be delivered after a delay.
            period: Resend message periodically.

        Returns:
            Delivery task that can be cancelled.
        """
        return self._schedule_tell(
            actor=actor,
            message=message,
            delay=delay,
            period=period)

    def stop(
            self,
            actor: Union[Actor, ActorRef]
    ) -> None:
        """Asynchronously stop an actor.

        Args:
            actor: Actor to stop.

        Returns:
            None
        """
        self._tell(
            actor=actor,
            message=PoisonPill())

    def shutdown(
            self,
            timeout: Union[None, int, float] = None
    ) -> None:
        """Initialize a shutdown of the actor system.

        Args:
            timeout: If the shutdown should take longer than `timeout`
            then the actors which are still running will be forcibly terminated.

        Returns:
            None
        """
        asyncio.create_task(self._shutdown(timeout=timeout))

    async def stopped(self) -> None:
        """Await system shutdown.

        Returns:
            Awaitable until the system is fully shutdown.
        """
        await self._stopped.wait()

    def _tell(
            self,
            actor: Union[Actor, ActorRef],
            message: 'any',
            *,
            sender: Union[None, Actor, ActorRef] = None,  # sender is None if the system sends the message
    ) -> None:
        actor = self._validate_actor_ref(actor)
        if sender:
            sender = self._validate_actor_ref(sender)
        if actor in self._actors:
            actor_ctx = self._actors[actor]
            actor_ctx.letterbox.put_nowait((sender, message))
        elif sender:
            deadletter = DeadLetter(actor=actor, message=message)
            self._tell(sender, deadletter)

    def _schedule_tell(
            self,
            actor: Union[Actor, ActorRef],
            message: 'any',
            *,
            sender: Union[None, Actor, ActorRef] = None,  # sender is None if the system sends the message
            delay: Union[None, int, float] = None,
            period: Union[None, int, float] = None
    ) -> asyncio.Task:
        if not delay:
            if not period:
                raise ValueError('Cannot schedule message without delay and period')
            self._tell(actor, message, sender=sender)
            delay = period
        ts = asyncio.get_event_loop().time() + delay
        return asyncio.create_task(
            self._schedule_tell_loop(actor, message, sender=sender, ts=ts, period=period))

    async def _schedule_tell_loop(
            self,
            actor: Union[Actor, ActorRef],
            message: 'any',
            *,
            sender: Union[None, Actor, ActorRef] = None,  # sender is None if the system sends the message
            ts: Union[None, int, float] = None,
            period: Union[None, int, float] = None
    ) -> None:
        actor = self._validate_actor_ref(actor)
        delay = (ts - asyncio.get_event_loop().time()) if ts else 0
        while True:
            if delay > 0:
                await asyncio.sleep(delay)
            self._tell(actor, message, sender=sender)
            if not period or actor not in self._actors:
                break
            delay = period

    def _watch(
            self,
            actor: Union[Actor, ActorRef],
            other: Union[Actor, ActorRef]
    ) -> None:
        actor = self._validate_actor_ref(actor)
        if actor not in self._actors:
            raise ValueError(f'Actor does not exist: {actor}')
        other = self._validate_actor_ref(other)
        if other not in self._actors:
            raise ValueError(f'Actor does not exist: {other}')
        if actor == other:
            raise ValueError(f'Actor cannot watch themselves: {actor}')
        actor_ctx = self._actors[actor]
        other_ctx = self._actors[other]
        actor_ctx.watching.append(other)
        other_ctx.watched_by.append(actor)

    def _unwatch(
            self,
            actor: Union[Actor, ActorRef],
            other: Union[Actor, ActorRef]
    ) -> None:
        actor = self._validate_actor_ref(actor)
        if actor not in self._actors:
            raise ValueError(f'Actor does not exist: {actor}')
        other = self._validate_actor_ref(other)
        if other not in self._actors:
            return  # other actor has been terminated so unwatch is obsolete
        if actor == other:
            raise ValueError(f'Actor cannot unwatch themselves: {actor}')
        actor_ctx = self._actors[actor]
        other_ctx = self._actors[other]
        if other in actor_ctx.watching:
            actor_ctx.watching.remove(other)
            other_ctx.watched_by.remove(actor)

    async def _shutdown(
            self,
            timeout: Union[None, int, float] = None
    ) -> None:
        if self._actors:
            lifecycle_tasks = []
            for actor_ref, actor_ctx in self._actors.items():
                self.stop(actor_ref)
                lifecycle_tasks.append(actor_ctx.lifecycle)
            done, pending = await asyncio.wait(lifecycle_tasks, timeout=timeout)
            for lifecycle_task in pending:
                lifecycle_task.cancel()
        self._stopped.set()

    async def _actor_lifecycle_loop(
            self,
            actor: Actor,
            actor_ref: ActorRef,
            actor_ctx: '_ActorContext'
    ) -> None:
        try:
            await actor.started()
            while True:
                sender, message = await actor_ctx.letterbox.get()
                if isinstance(message, PoisonPill):
                    break
                try:
                    await actor.receive(sender, message)
                except Exception as e:
                    await actor.restarted(sender, message, e)
            await actor.stopped()
        finally:
            # notify others that actor has been terminated
            for other in actor_ctx.watched_by:
                self._tell(other, Terminated(actor_ref))
                other_ctx = self._actors[other]
                other_ctx.watching.remove(actor_ref)
            # unwatch other actors
            for other in actor_ctx.watching:
                other_ctx = self._actors[other]
                other_ctx.watched_by.remove(actor_ref)
            # remove the actor
            del self._actors[actor_ref]

    @staticmethod
    def _validate_actor_ref(
            actor: Union[Actor, ActorRef]
    ) -> ActorRef:
        if isinstance(actor, Actor):
            actor = actor.actor_ref
        if not isinstance(actor, ActorRef):
            raise ValueError(f'Not an actor: {actor}')
        return actor


class _ActorContext:  # context class that holds the internal state of an actor
    def __init__(self):
        self.letterbox = asyncio.Queue()  # unbounded queue
        self.lifecycle = None  # main task controlling actor's lifecycle
        self.watching = []  # this actor is watching other actors in the list
        self.watched_by = []  # this actor is watched by other actors in the list


# Messages


@dataclass(frozen=True)
class PoisonPill:  # sent to an actor to stop them
    pass


@dataclass(frozen=True)
class DeadLetter:  # sent back to the sender if `actor` cannot receive the `message` because they have been terminated
    actor: ActorRef
    message: 'any'


@dataclass(frozen=True)
class Terminated:  # sent to a watcher when `actor` has been terminated
    actor: ActorRef


# Utilities


def uuid4() -> str: return uuid.uuid4().hex
