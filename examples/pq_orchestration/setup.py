from setuptools import setup

setup(
    name="deephaven-pq-orchestration",
    version="0.1.0",
    description="Generic framework for orchestrating Deephaven Enterprise persistent queries",
    python_requires=">=3.10",
    install_requires=[
        "deephaven-coreplus-client",
        "pyyaml>=6.0",
    ],
    py_modules=['pq_orchestrator'],
    entry_points={
        "console_scripts": [
            "pq-orchestrator=pq_orchestrator:main",
        ],
    },
)
