class _DecoratorEvent:
    def __init__(self,*a,**kw): pass
    def __call__(self, func): return func
class NewMessage(_DecoratorEvent):
    class Event: pass
class CallbackQuery(_DecoratorEvent):
    class Event: pass
