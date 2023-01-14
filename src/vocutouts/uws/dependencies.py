"""FastAPI dependencies for the UWS service.

The UWS FastAPI support is initialized by the parent application via this
dependency's ``initialize`` method.  It then returns a `UWSFactory` on
request to individual route handlers, which in turn can create other needed
objects.
"""

from typing import Generic, Optional, TypeVar

from fastapi import Depends
from pydantic import BaseModel
from safir.dependencies.db_session import db_session_dependency
from safir.dependencies.logger import logger_dependency
from sqlalchemy.ext.asyncio import async_scoped_session
from structlog.stdlib import BoundLogger

from .config import UWSConfig
from .policy import UWSPolicy
from .service import JobService
from .storage import FrontendJobStore

T = TypeVar("T", bound=BaseModel)

__all__ = [
    "UWSDependency",
    "UWSFactory",
    "uws_dependency",
]


class UWSFactory(Generic[T]):
    """Build UWS components."""

    def __init__(
        self,
        *,
        config: UWSConfig,
        policy: UWSPolicy,
        session: async_scoped_session,
        param_type: type[T],
        logger: BoundLogger,
    ) -> None:
        self._config = config
        self._policy = policy
        self._session = session
        self._param_type = param_type
        self._logger = logger

    def create_job_service(self) -> JobService[T]:
        """Create a new UWS job metadata service."""
        storage = FrontendJobStore(self._session, self._param_type)
        return JobService(
            config=self._config,
            policy=self._policy,
            storage=storage,
            logger=self._logger,
        )


class UWSDependency:
    """Initializes UWS and provides a UWS factory as a dependency."""

    def __init__(self) -> None:
        self._config: Optional[UWSConfig] = None
        self._policy: Optional[UWSPolicy] = None
        self._param_type: Optional[type[BaseModel]] = None

    async def __call__(
        self,
        session: async_scoped_session = Depends(db_session_dependency),
        logger: BoundLogger = Depends(logger_dependency),
    ) -> UWSFactory:
        if not self._config or not self._policy or not self._param_type:
            raise RuntimeError("UWSDependency not initialized")
        return UWSFactory(
            config=self._config,
            policy=self._policy,
            session=session,
            param_type=self._param_type,
            logger=logger,
        )

    async def aclose(self) -> None:
        """Shut down the UWS subsystem."""
        await db_session_dependency.aclose()

    async def initialize(
        self,
        *,
        config: UWSConfig,
        policy: UWSPolicy,
        param_type: type[BaseModel],
        logger: BoundLogger,
    ) -> None:
        """Initialize the UWS subsystem.

        Parameters
        ----------
        config
            The UWS configuration.
        policy
            The UWS policy layer.
        param_type
            The type of the job parameters.
        logger
            Logger to use during database initialization.  This is not saved;
            subsequent invocations as a dependency will create a new logger
            from the triggering request.
        """
        self._config = config
        self._policy = policy
        self._param_type = param_type
        await db_session_dependency.initialize(
            config.database_url,
            config.database_password,
            isolation_level="REPEATABLE READ",
        )

    def override_policy(self, policy: UWSPolicy) -> None:
        """Change the actor used in subsequent invocations.

        This method is probably only useful for the test suite.

        Parameters
        ----------
        actor
            The new policy.
        """
        self._policy = policy


uws_dependency = UWSDependency()
