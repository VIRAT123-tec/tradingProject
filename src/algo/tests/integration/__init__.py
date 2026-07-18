"""Integration and end-to-end tests: real collaborators wired together
(DependencyContainer, SimulationBroker, Strategy1, RiskCore,
ReconciliationEngine, PlatformScheduler, Application), as opposed to each
module's own unit test suite, which deliberately isolates its subject behind
fakes of everything around it. See conftest.py's module docstring for the
full rationale and the fixtures shared across this package.
"""
