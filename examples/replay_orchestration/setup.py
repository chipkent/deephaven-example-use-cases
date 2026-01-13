from setuptools import setup

setup(
    name="deephaven-replay-orchestration",
    version="0.1.0",
    description="Generic framework for orchestrating Deephaven Enterprise replay persistent queries",
    python_requires=">=3.10",
    install_requires=[
        "deephaven-coreplus-client",
        "pyyaml>=6.0",
    ],
    py_modules=['replay_orchestrator'],
    entry_points={
        "console_scripts": [
            "replay-orchestrator=replay_orchestrator:main",
        ],
    },
)
