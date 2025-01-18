import asyncio
import logging

from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class WaitingManager:
    start_time: float = 1.0
    max_time: float = 60.0
    factor: float = 2.0
    current_time: float = field(init=False)
    _has_waited: bool = field(default=False, init=False)

    def __post_init__(self):
        self.current_time = self.start_time

    async def wait(self):
        wait_time = self.get_next_time()
        logger.debug(f'Wait for ... {wait_time}')
        await asyncio.sleep(wait_time)
        self._has_waited = True

    def get_next_time(self) -> float:
        next_time = self.current_time
        self.current_time = min(self.current_time * self.factor, self.max_time)
        return next_time

    def reset(self):
        if self._has_waited:
            self.current_time = self.start_time
            self._has_waited = False
