# Offline test adapters

These minimal modules are used only by `pytest` when Telethon or
`python-decouple` are not installed in the audit environment. Production code
never adds this directory to `sys.path`; a real deployment must install
`requirements.txt`.
