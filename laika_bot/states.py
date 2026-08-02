from aiogram.fsm.state import State, StatesGroup


class AccountAuth(StatesGroup):
    phone = State()
    email = State()
    email_confirm = State()
    code = State()
    password = State()


class AccountEmailEdit(StatesGroup):
    address = State()
    note = State()


class AddChannel(StatesGroup):
    link = State()


class SetReactions(StatesGroup):
    reactions = State()


class SetGroupReactions(StatesGroup):
    reactions = State()


class SetChannelReactions(StatesGroup):
    reactions = State()


class SetDelay(StatesGroup):
    minimum = State()
    maximum = State()


class SetMembershipDelay(StatesGroup):
    minimum = State()
    maximum = State()


class SetPostReactionLimit(StatesGroup):
    value = State()


class SetPostTypePercentage(StatesGroup):
    value = State()


class SetPromotionPeriod(StatesGroup):
    value = State()


class SetManualViewAmount(StatesGroup):
    value = State()


class SetChannelReactionWindow(StatesGroup):
    value = State()


class ConfigurationRestore(StatesGroup):
    backup_file = State()
    confirmation = State()


class ManagementSearch(StatesGroup):
    accounts = State()
    targets = State()
