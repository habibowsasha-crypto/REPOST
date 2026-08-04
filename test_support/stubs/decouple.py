import os

def config(name, default=None, cast=None):
    if name in os.environ:
        value = os.environ[name]
    elif default is not None:
        value = default
    else:
        raise UndefinedValueError(name)
    if cast is not None:
        return cast(value)
    return value

class UndefinedValueError(Exception):
    pass
