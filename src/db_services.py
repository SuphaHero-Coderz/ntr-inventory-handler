import src.database as _database
from src.database import engine
from src.models import Inventory, InventoryTransaction
from sqlmodel import Session, select

DEFAULT_TOKEN_AMOUNT = 100

"""
DATABASE ZONE
"""


def create_database() -> None:
    """
    Initializes the database engine
    """
    _database.init_db()


"""
INVENTORY ZONE
"""


def populate_inventory() -> None:
    """
    Populates the inventory table with tokens
    """
    with Session(engine) as session:
        lonely_inventory = Inventory(amount=DEFAULT_TOKEN_AMOUNT)
        session.add(lonely_inventory)
        session.commit()


def get_num_tokens() -> int:
    """
    Gets the number of tokens

    Returns:
        int: the number of tokens (duh)
    """
    with Session(engine) as session:
        query = select(Inventory)
        inventory = session.exec(query).one()

        return inventory.amount


def add_tokens(num_tokens: int) -> None:
    """
    Adds `num_tokens` more tokens to inventory

    Args:
        num_tokens (int): number of tokens to add
    """
    with Session(engine) as session:
        query = select(Inventory)
        inventory = session.exec(query).one()

        inventory.amount += num_tokens

        session.add(inventory)
        session.commit()
        session.refresh(inventory)


def deduct_tokens(num_tokens: int) -> None:
    """
    Deducts `num_tokens` tokens from inventory

    Args:
        num_tokens (int): number of tokens to deduct
    """
    with Session(engine) as session:
        query = select(Inventory)
        inventory = session.exec(query).one()

        inventory.amount -= num_tokens

        session.add(inventory)
        session.commit()
        session.refresh(inventory)


def create_transaction(
    user_id: int, order_id: int, num_tokens: int
) -> InventoryTransaction:
    with Session(engine) as session:
        transaction: InventoryTransaction = InventoryTransaction(
            user_id=user_id, order_id=order_id, amount=num_tokens
        )
        session.add(transaction)
        session.commit()

        return transaction


def get_transaction(user_id: int, order_id: int) -> InventoryTransaction:
    with Session(engine) as session:
        query = select(InventoryTransaction).where(
            InventoryTransaction.user_id == user_id,
            InventoryTransaction.order_id == order_id,
        )

        try:
            transaction = session.exec(query).one()
        except:
            transaction = None

        return transaction
