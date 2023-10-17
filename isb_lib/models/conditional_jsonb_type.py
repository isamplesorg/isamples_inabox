from sqlalchemy import TypeDecorator, JSON
from sqlalchemy.dialects.postgresql import JSONB


class ConditionalJSONB(TypeDecorator):
    """Type that will use JSONB when connected to postgresql, and JSON otherwise.
    """

    impl = JSON

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB())
        else:
            return dialect.type_descriptor(JSON())
