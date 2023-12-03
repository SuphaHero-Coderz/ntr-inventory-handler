from sqlmodel import SQLModel, Field
from typing import Optional


class Inventory(SQLModel, table=True):
    __tablename__: str = "inventory"
    id: Optional[int] = Field(default=None, primary_key=True)
    amount: int


class InventoryTransaction(SQLModel, table=True):
    __tablename__: str = "transactions"
    id: Optional[int] = Field(default=None, primary_key=True)
    order_id: int
    user_id: int
    amount: int
