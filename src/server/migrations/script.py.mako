<%!
import re
%>"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

revision: str = ${repr(up_revision)}
down_revision: Union[str, Sequence[str], None] = ${repr(down_revision)}
branch_labels: Union[str, Sequence[str], None] = ${repr(branch_labels)}
depends_on: Union[str, Sequence[str], None] = ${repr(depends_on)}


def upgrade(engine_name: str) -> None:
    globals()[f"upgrade_{engine_name}"]()


def downgrade(engine_name: str) -> None:
    globals()[f"downgrade_{engine_name}"]()

<%
databases = config.get_main_option("databases")
%>
% for database in re.split(r",\s*", databases):

def upgrade_${database}() -> None:
    ${context.get(f"{database}_upgrades", "pass")}


def downgrade_${database}() -> None:
    ${context.get(f"{database}_downgrades", "pass")}

% endfor
