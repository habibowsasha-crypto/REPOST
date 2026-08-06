class StringSession:
    def __init__(self, value=None): self.value=value
    def save(self): return self.value or 'stub-session'
