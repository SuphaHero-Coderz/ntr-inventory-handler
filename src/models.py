from sqlmodel import SQLModel, Field
from typing import Optional


class Inventory(SQLModel, table=True):
    __tablename__: str = "inventory"
    id: Optional[int] = Field(default=None, primary_key=True)
    amount: int
