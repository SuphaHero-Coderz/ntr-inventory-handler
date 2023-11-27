import src.database as _database
from src.database import engine
from src.models import Inventory
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


def deduct_tokens(num_tokens: int) -> None:
    with Session(engine) as session:
        query = select(Inventory)
        inventory = session.exec(query).one()

        inventory.amount -= num_tokens

        session.add(inventory)
        session.commit()
        session.refresh(inventory)
