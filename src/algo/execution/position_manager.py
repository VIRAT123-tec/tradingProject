"""Answers "what's open right now" purely from the database. Must never rely on
in-memory state — this is the module a crash-and-restart recovery path depends on.

TODO: implement position queries backed entirely by database/repositories/.
"""
