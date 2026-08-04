class RPCError(Exception): pass
class FloodWaitError(RPCError):
    def __init__(self,*a,seconds=60,**kw): super().__init__(*a); self.seconds=seconds
class PeerFloodError(RPCError): pass
class UserIsBlockedError(RPCError): pass
class UserPrivacyRestrictedError(RPCError): pass
class ChatWriteForbiddenError(RPCError): pass
class InputUserDeactivatedError(RPCError): pass
class UserBannedInChannelError(RPCError): pass
class PhoneCodeExpiredError(RPCError): pass
class PhoneCodeInvalidError(RPCError): pass
class PhoneNumberInvalidError(RPCError): pass
class SessionPasswordNeededError(RPCError): pass
class PeerIdInvalidError(RPCError): pass
