import asyncio
import uuid
from dataclasses import dataclass
from typing import Union, Optional


@dataclass(repr=False, eq=False, frozen=True)
class ActorRef:  # serializable reference which represents an Actor
    actor_id: str
    name: str
    def __eq__(self, other): return isinstance(other, ActorRef) and self.actor_id == other.actor_id
    def __hash__(self): return hash(self.actor_id)
    def __repr__(self): return self.name
    def __str__(self): return self.name


class Actor:  # inheritable Actor class
    def __init__(self):
        self.actor_ref = None
        self.system = None

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
            message: 'any',
            *,
            delay: Union[None, int, float] = None,
            period: Union[None, int, float] = None
    ) -> asyncio.Task:
        """Asynchronously deliver a message to another actor.

        Args:
            actor: Anther actor that will receive a message from this actor.
            message: Message to another actor.
            delay: Send message after a delay.
            period: Resend message periodically.

        Returns:
            Delivery task that can be cancelled if the message is delayed or periodical.
        """
        return self.system._tell(
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
    def __init__(self, *, loop: Optional[asyncio.AbstractEventLoop] = None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop
        self._actors = {}
        self._stopped = asyncio.Event(loop=loop)

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
        actor_ctx.lifecycle = self._loop.create_task(
            self.__start_lifecycle(actor, actor_ref, actor_ctx))
        self._actors[actor_ref] = actor_ctx
        return actor_ref

    def tell(
            self,
            actor: Union[Actor, ActorRef],
            message: 'any',
            *,
            delay: Union[None, int, float] = None,
            period: Union[None, int, float] = None
    ) -> asyncio.Task:
        """Asynchronously deliver a message to an actor.

        Args:
            actor: Actor that will receive a message from the system.
            message: Message to another actor.
            delay: Send message after a delay.
            period: Resend message periodically.

        Returns:
            Delivery task that can be cancelled if the message is delayed or periodical.
        """
        return self._tell(
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

    async def shutdown(
            self,
            timeout: Union[None, int, float] = None
    ) -> None:
        """Initialize a shutdown of the actor system.

        Args:
            timeout: If the shutdown should take longer than `timeout`
            then the actors which are still running will be forcibly terminated.

        Returns:
            Awaitable until the system is fully shutdown.
        """
        self._loop.create_task(self._shutdown(timeout=timeout))
        await self.stopped()

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
            delay=None,
            period=None
    ) -> asyncio.Task:
        actor = self._validate_actor_ref(actor)
        if sender:
            sender = self._validate_actor_ref(sender)
        ts = (self._loop.time() + delay) if delay else None
        return self._loop.create_task(
            self.__deliver(actor, message, sender=sender, ts=ts, period=period))

    def _watch(
            self,
            actor: Union[Actor, ActorRef],
            other: Union[Actor, ActorRef]
    ) -> None:
        actor = self._validate_actor_ref(actor)
        other = self._validate_actor_ref(other)
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
        other = self._validate_actor_ref(other, must_exist=False)
        if other not in self._actors:
            return  # other actor has been terminated so unwatch is obsolete
        actor = self._validate_actor_ref(actor)
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
        lifecycle_tasks = []
        for actor_ref, actor_ctx in self._actors.items():
            self.stop(actor_ref)
            lifecycle_tasks.append(actor_ctx.lifecycle)
        done, pending = await asyncio.wait(lifecycle_tasks, timeout=timeout)
        for lifecycle_task in pending:
            lifecycle_task.cancel()
        self._stopped.set()

    def _validate_actor_ref(
            self,
            actor: Union[Actor, ActorRef],
            must_exist: bool = True
    ) -> ActorRef:
        if isinstance(actor, Actor):
            actor = actor.actor_ref
        if not isinstance(actor, ActorRef):
            raise ValueError(f'Not an actor: {actor}')
        if must_exist and actor not in self._actors:
            raise ValueError(f'Actor does not exist: {actor}')
        return actor

    async def __deliver(
            self,
            actor: ActorRef,
            message: 'any',
            *,
            sender: Optional[ActorRef] = None,
            ts: Union[None, int, float] = None,
            period: Union[None, int, float] = None
    ) -> None:
        delay = (ts - self._loop.time()) if ts else 0
        while True:
            if delay > 0:
                await asyncio.sleep(delay)
            if actor not in self._actors:
                # actor has been stopped, so attempt to send a deadletter to the sender
                if sender in self._actors:
                    deadletter = DeadLetter(actor=actor, message=message)
                    self._tell(sender, deadletter)
                break
            actor_ctx = self._actors[actor]
            asyncio.create_task(actor_ctx.letterbox.put((sender, message)))
            if not period:
                break
            delay = period

    async def __start_lifecycle(
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
                asyncio.create_task(actor.receive(sender, message))
            await actor.stopped()
        finally:
            # notify others that actor has been terminated
            for other in actor_ctx.watched_by:
                self.tell(other, Terminated(actor_ref))
                other_ctx = self._actors[other]
                other_ctx.watching.remove(actor_ref)
            # unwatch other actors
            for other in actor_ctx.watching:
                other_ctx = self._actors[other]
                other_ctx.watched_by.remove(actor_ref)
            # remove the actor
            del self._actors[actor_ref]


class _ActorContext:  # context class that holds the internal state of an actor
    def __init__(self):
        self.letterbox = asyncio.Queue(maxsize=1)  # bounded queue
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
