from __future__ import annotations

from .ai_account_profiles_repository import (
    AIAccountProfilesRepositoryMixin as _AIAccountProfilesRepositoryMixin,
)
from .ai_comment_generation_repository import (
    AICommentGenerationRepositoryMixin as _AICommentGenerationRepositoryMixin,
)
from .ai_comments_repository import AICommentsRepositoryMixin as _AICommentsRepositoryMixin
from .ai_dialogues_repository import AIDialoguesRepositoryMixin as _AIDialoguesRepositoryMixin
from .db_accounts import AccountDatabaseMixin as _AccountDatabaseMixin
from .db_analytics import AnalyticsDatabaseMixin as _AnalyticsDatabaseMixin
from .db_channels import ChannelDatabaseMixin as _ChannelDatabaseMixin
from .db_configuration import ConfigurationDatabaseMixin as _ConfigurationDatabaseMixin
from .db_core import DatabaseCore as _DatabaseCore
from .db_membership_jobs import MembershipJobDatabaseMixin as _MembershipJobDatabaseMixin
from .db_reaction_jobs import ReactionJobDatabaseMixin as _ReactionJobDatabaseMixin

# Compatibility facade: public Database imports and historically patchable
# module symbols (for example ``laika_bot.db.inspect``) remain available.
from .db_shared import *  # noqa: F401,F403
from .db_view_jobs import ViewJobDatabaseMixin as _ViewJobDatabaseMixin


class Database(
    _AIDialoguesRepositoryMixin,
    _AICommentGenerationRepositoryMixin,
    _AIAccountProfilesRepositoryMixin,
    _AICommentsRepositoryMixin,
    _ConfigurationDatabaseMixin,
    _AccountDatabaseMixin,
    _ChannelDatabaseMixin,
    _MembershipJobDatabaseMixin,
    _ReactionJobDatabaseMixin,
    _ViewJobDatabaseMixin,
    _AnalyticsDatabaseMixin,
    _DatabaseCore,
):
    """Composed persistence facade preserving the original Database API."""

    pass
