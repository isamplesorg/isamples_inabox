from sqlalchemy import TypeDecorator, JSON
from sqlalchemy.dialects.postgresql import JSONB


class ConditionalJSONB(TypeDecorator):
    """Type that will use JSONB when connected to postgresql, and JSON otherwise.
    Modeled after example at: https://docs.sqlalchemy.org/en/20/core/custom_types.html#sqlalchemy.types.TypeDecorator.load_dialect_impl
    """

    impl = JSON

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB())
        else:
            return dialect.type_descriptor(JSON())
