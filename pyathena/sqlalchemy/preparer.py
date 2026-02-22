from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.sql.compiler import ILLEGAL_INITIAL_CHARACTERS, IdentifierPreparer

from pyathena.sqlalchemy.constants import DDL_RESERVED_WORDS, SELECT_STATEMENT_RESERVED_WORDS

if TYPE_CHECKING:
    from sqlalchemy import Dialect


class AthenaDMLIdentifierPreparer(IdentifierPreparer):
    """Identifier preparer for Athena DML (SELECT, INSERT, etc.) statements.

    This preparer handles quoting and escaping of identifiers in DML statements.
    It uses double quotes for identifiers and recognizes Athena's SELECT
    statement reserved words to determine when quoting is necessary.

    Athena's DML syntax follows Presto/Trino conventions, which differ from
    DDL syntax (which uses Hive conventions with backticks).

    See Also:
        :class:`AthenaDDLIdentifierPreparer`: Preparer for DDL statements.
        AWS Athena Reserved Words:
        https://docs.aws.amazon.com/athena/latest/ug/reserved-words.html
    """

    reserved_words: set[str] = SELECT_STATEMENT_RESERVED_WORDS


class AthenaDDLIdentifierPreparer(IdentifierPreparer):
    """Identifier preparer for Athena DDL (CREATE, ALTER, DROP) statements.

    This preparer handles quoting and escaping of identifiers in DDL statements.
    It uses backticks for identifiers (Hive convention) rather than double
    quotes (Presto/Trino convention used in DML).

    Key differences from DML preparer:
    - Uses backtick (`) as the quote character
    - Recognizes DDL-specific reserved words
    - Treats underscore (_) as an illegal initial character

    See Also:
        :class:`AthenaDMLIdentifierPreparer`: Preparer for DML statements.
        AWS Athena DDL Reserved Words:
        https://docs.aws.amazon.com/athena/latest/ug/reserved-words.html
    """

    reserved_words = DDL_RESERVED_WORDS
    illegal_initial_characters = ILLEGAL_INITIAL_CHARACTERS.union("_")

    def __init__(
        self,
        dialect: Dialect,
        initial_quote: str = "`",
        final_quote: str | None = None,
        escape_quote: str = "`",
        quote_case_sensitive_collations: bool = True,
        omit_schema: bool = False,
    ):
        super().__init__(
            dialect=dialect,
            initial_quote=initial_quote,
            final_quote=final_quote,
            escape_quote=escape_quote,
            quote_case_sensitive_collations=quote_case_sensitive_collations,
            omit_schema=omit_schema,
        )
