import asyncio

class _DecoratorEvent:
    def __init__(self,*a,**kw): pass
    def __call__(self, func): return func

class events:
    class NewMessage(_DecoratorEvent):
        class Event: pass
    class CallbackQuery(_DecoratorEvent):
        class Event: pass

class Button:
    @staticmethod
    def inline(text, data=None): return ('inline', text, data)
    @staticmethod
    def text(text, **kwargs): return ('text', text, kwargs)

class TelegramClient:
    def __init__(self,*a,**kw):
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
        self.session = type('S',(),{'save':lambda self: 'stub-session'})()
    def on(self,*a,**kw):
        def deco(f): return f
        return deco
    def is_connected(self): return True
    async def connect(self): return None
    async def disconnect(self): return None
    async def is_user_authorized(self): return True
