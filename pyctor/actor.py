__all__ = ('ActorRef', 'Actor', 'ActorSystem', 'PoisonPill', 'DeadLetter', 'Terminated')

import asyncio
import logging
import uuid
from dataclasses import dataclass
from typing import Union, List, Dict, Set


@dataclass(repr=False, eq=False, frozen=True)
class ActorRef:  # serializable reference which represents an Actor
    actor_id: str
    path: str
    name: str
    def __eq__(self, other): return isinstance(other, ActorRef) and self.actor_id == other.actor_id
    def __hash__(self): return hash(self.actor_id)
    def __repr__(self): return self.path
    def __str__(self): return self.path


class Actor:  # inheritable Actor class
    # noinspection PyTypeChecker
    def __init__(self):
        self._context: '_ActorContext' = None

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
    def create(
            self,
            actor: 'Actor',
            name: str = None
    ) -> ActorRef:
        """Initialize and asynchronously start a child actor. The child actor will be watched.

        Args:
            actor: Actor instance to start.
            name: Name of the actor.

        Returns:
            Actor reference.
        """
        child_actor_ref = self.system._create(
            actor=actor,
            parent=self.actor_ref,
            name=name)
        self.watch(child_actor_ref)
        return child_actor_ref

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

        This method can be called even if the actor is not watched anymore.

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
                          self.actor_ref, message, sender, exc_info=error)

    async def stopped(self) -> None:
        """Called by the system to let this actor know that they will not receive messages anymore.
        When this method is called, the actor's children have already been terminated.

        Returns:
            None
        """
        pass

    @property
    def actor_ref(self) -> ActorRef:
        return self._context.actor_ref

    @property
    def system(self) -> 'ActorSystem':
        return self._context.system

    @property
    def parent(self) -> ActorRef:
        return self._context.parent

    @property
    def name(self) -> str:
        return self.actor_ref.name

    @property
    def path(self) -> str:
        return self.actor_ref.path

    def __str__(self): return self.path
    def __repr__(self): return self.path


class ActorSystem:  # ActorSystem controls all the actors
    def __init__(self):
        self._actors: Dict[ActorRef, '_ActorContext'] = {}
        self._is_stopped = asyncio.Event()
        self.children: List[ActorRef] = []

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
            Start the actor by passing its instance to this method.

            >> class MyActor(Actor):
            ...    async def receive(self, sender, message): pass

            >> my_actor = ActorSystem().create(MyActor())
        """
        return self._create(
            actor=actor,
            parent=None,
            name=name)

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
            message=message,
            sender=None)

    def schedule_tell(
            self,
            actor: Union[Actor, ActorRef],
            message: 'any',
            *,
            delay: Union[None, int, float] = None,
            period: Union[None, int, float] = None
    ) -> asyncio.Task:
        """Schedule a message to be delivered to an actor at some time.
        The message may also be delivered periodically.

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
            sender=None,
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
            message=PoisonPill(),
            sender=None)

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

    def stopped(self):
        """Await system shutdown.

        Returns:
            Awaitable until the system is fully shutdown.
        """
        return self._is_stopped.wait()

    def _create(
            self,
            actor: Actor,
            *,
            parent: Union[None, Actor, ActorRef],  # parent is None if system is the parent
            name: str = None
    ) -> ActorRef:
        if not isinstance(actor, Actor):
            raise ValueError(f'Not an actor: {actor}')
        if parent:
            parent = self._validate_actor_ref(parent)
            parent_ctx = self._actors[parent]
            child_idx = len(parent_ctx.children) + 1
        else:
            child_idx = len(self.children) + 1
        if not name:
            name = f'{type(actor).__name__}-{child_idx}'
        if parent:
            path = f'{parent.path}/{name}'
        else:
            path = name
        actor_id = uuid4()
        actor_ref = ActorRef(actor_id=actor_id, path=path, name=name)
        actor_ctx = _ActorContext(self, actor_ref, parent)
        actor_ctx.lifecycle = asyncio.get_event_loop().create_task(
            self._actor_lifecycle_loop(actor, actor_ref, actor_ctx))
        actor._context = actor_ctx
        self._actors[actor_ref] = actor_ctx
        if parent:
            # parent_ctx is assigned above
            # noinspection PyUnboundLocalVariable
            parent_ctx.children.append(actor_ref)
        else:
            self.children.append(actor_ref)
        return actor_ref

    def _tell(
            self,
            actor: Union[Actor, ActorRef],
            message: 'any',
            *,
            sender: Union[None, Actor, ActorRef],  # sender is None if system sends the message
    ) -> None:
        actor = self._validate_actor_ref(actor)
        if sender:
            sender = self._validate_actor_ref(sender)
            if sender not in self._actors:
                raise ValueError(f'Sender does not exist: {sender}')
        if actor in self._actors:
            actor_ctx = self._actors[actor]
            actor_ctx.letterbox.put_nowait((sender, message))
        elif sender:
            deadletter = DeadLetter(actor=actor, message=message)
            self._tell(sender, deadletter, sender=None)
        else:
            logging.warning('Failed to deliver message %s to %s', message, actor)

    def _schedule_tell(
            self,
            actor: Union[Actor, ActorRef],
            message: 'any',
            *,
            sender: Union[None, Actor, ActorRef],  # sender is None if the system sends the message
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
            sender: Union[None, Actor, ActorRef],  # sender is None if system sends the message
            ts: Union[None, int, float] = None,
            period: Union[None, int, float] = None
    ) -> None:
        actor = self._validate_actor_ref(actor)
        delay = (ts - asyncio.get_event_loop().time()) if ts else 0
        while True:
            if delay > 0:
                await asyncio.sleep(delay)
            self._tell(actor, message, sender=sender)
            if not period:
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
        actor_ctx.watching.add(other)
        other_ctx.watched_by.add(actor)

    def _unwatch(
            self,
            actor: Union[Actor, ActorRef],
            other: Union[Actor, ActorRef]
    ) -> None:
        actor = self._validate_actor_ref(actor)
        if actor not in self._actors:
            raise ValueError(f'Actor does not exist: {actor}')
        other = self._validate_actor_ref(other)
        if actor == other:
            raise ValueError(f'Actor cannot unwatch themselves: {actor}')
        actor_ctx = self._actors[actor]
        if other in actor_ctx.watching:
            actor_ctx.watching.remove(other)
        if other in self._actors:
            other_ctx = self._actors[other]
            if actor in other_ctx.watched_by:
                other_ctx.watched_by.remove(actor)

    async def _shutdown(
            self,
            timeout: Union[None, int, float] = None
    ) -> None:
        if self._actors:
            for actor_ref in self.children:  # children propagate stop messages
                self.stop(actor_ref)
            lifecycle_tasks = [actor_ctx.lifecycle for actor_ctx in self._actors.values()]
            done, pending = await asyncio.wait(lifecycle_tasks, timeout=timeout)
            for lifecycle_task in pending:
                lifecycle_task.cancel()
        self._is_stopped.set()

    async def _actor_lifecycle_loop(
            self,
            actor: Actor,
            actor_ref: ActorRef,
            actor_ctx: '_ActorContext'
    ) -> None:
        # start actor
        try:
            await actor.started()
            actor_ctx.receiving_messages = True
        except Exception as e:
            logging.exception('Exception raised while awaiting start of %s', actor_ref, exc_info=e)
        # receive messages
        while actor_ctx.receiving_messages:
            sender, message = await actor_ctx.letterbox.get()
            if isinstance(message, PoisonPill):
                break
            try:
                await actor.receive(sender, message)
            except Exception as e:
                try:
                    await actor.restarted(sender, message, e)
                except Exception as e:
                    logging.exception('Exception raised while awaiting restart of %s', actor_ref, exc_info=e)
        actor_ctx.receiving_messages = False
        # stop children
        children_stopping = []
        for child in actor_ctx.children:
            child_ctx = self._actors[child]
            children_stopping.append(child_ctx.is_stopped.wait())
            self.stop(child)
        if children_stopping:
            await asyncio.wait(children_stopping)
        # stop actor
        try:
            await actor.stopped()
        except Exception as e:
            logging.exception('Exception raised while awaiting stop of %s', actor_ref, exc_info=e)
        # notify others that actor has been terminated
        for other in actor_ctx.watched_by:
            self._tell(other, Terminated(actor_ref), sender=None)
            other_ctx = self._actors[other]
            other_ctx.watching.remove(actor_ref)
        # unwatch other actors
        for other in actor_ctx.watching:
            other_ctx = self._actors[other]
            other_ctx.watched_by.remove(actor_ref)
        # update parent's children
        if actor_ctx.parent:
            parent_ctx = self._actors[actor_ctx.parent]
            parent_ctx.children.remove(actor_ref)
        else:
            self.children.remove(actor_ref)
        # flush actor's letterbox and send deadletters
        while not actor_ctx.letterbox.empty():
            sender, message = actor_ctx.letterbox.get_nowait()
            if sender and sender != actor_ref:
                deadletter = DeadLetter(actor_ref, message)
                self._tell(sender, deadletter, sender=None)
            # messages to this actor are ignored, because the actor is not receiving messages anymore
        # remove the actor
        actor_ctx.is_stopped.set()
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
    def __init__(
            self,
            system: ActorSystem,
            actor_ref: ActorRef,
            parent: ActorRef
    ):
        self.system: ActorSystem = system
        self.actor_ref: ActorRef = actor_ref
        self.parent: ActorRef = parent
        self.letterbox = asyncio.Queue()  # unbounded queue
        self.lifecycle = None  # main task controlling actor's lifecycle
        # watching, watched_by and children are not exposed to the actor
        #  because they are updated before the actor receives a Terminated message.
        #  This leads to an inconsistent state if the actor has not received multiple Terminated messages yet.
        self.watching: Set[ActorRef] = set()  # this actor is watching other actors in the list
        self.watched_by: Set[ActorRef] = set()  # this actor is watched by other actors in the list
        self.children: List[ActorRef] = []  # actor's children
        self.is_stopped = asyncio.Event()
        self.receiving_messages = False


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
