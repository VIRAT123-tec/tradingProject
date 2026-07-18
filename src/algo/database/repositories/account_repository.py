"""AccountRepository: persistence for the Account aggregate.

Pure persistence -- no decisions about which account a strategy should trade
under (that belongs to accounts/account_manager.py), and no credential
handling (Account never stores secrets; see the model's own docstring).
"""

from __future__ import annotations

from sqlalchemy import select

from algo.common.enums import BrokerName
from algo.database.models.account import Account
from algo.database.repositories.base import BaseRepository


class AccountRepository(BaseRepository[Account]):
    """Persistence operations for Account rows."""

    model = Account

    def get_by_broker_client_id(
        self, broker: BrokerName, broker_client_id: str
    ) -> Account | None:
        """Look up an account by its (broker, broker_client_id) natural key."""
        stmt = select(Account).where(
            Account.broker == broker, Account.broker_client_id == broker_client_id
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def get_or_create(
        self, *, broker: BrokerName, broker_client_id: str | None, display_name: str
    ) -> tuple[Account, bool]:
        """Idempotently ensure an Account row exists, for the composition
        root (dependency_container.py) to resolve each configured account name
        to a durable id once at startup.

        Looked up by (broker, broker_client_id) when ``broker_client_id`` is
        set -- exactly the natural key the table's unique constraint enforces.
        SIMULATION accounts have no broker-side identity
        (``broker_client_id`` is ``None``), for which that constraint does not
        apply (Postgres treats every ``NULL`` as distinct from every other, so
        it cannot prevent two (SIMULATION, NULL) rows) -- for that case lookup
        falls back to (broker, display_name) instead. That fallback is
        sufficient for this platform's actual usage (a handful of named
        accounts resolved once at single-process startup, not a concurrent
        account-creation path) without claiming a DB-level guarantee the
        schema does not actually provide for this one case.
        """

        def lookup() -> Account | None:
            if broker_client_id is not None:
                return self.get_by_broker_client_id(broker, broker_client_id)
            stmt = select(Account).where(
                Account.broker == broker, Account.display_name == display_name
            )
            return self.session.execute(stmt).scalar_one_or_none()

        def factory() -> Account:
            return Account(
                broker=broker, broker_client_id=broker_client_id, display_name=display_name
            )

        return self._get_or_create(lookup=lookup, factory=factory)

    def list_active(self) -> list[Account]:
        """List every account currently enabled for trading."""
        stmt = select(Account).where(Account.is_active.is_(True))
        return list(self.session.execute(stmt).scalars())
