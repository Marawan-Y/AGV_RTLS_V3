# 37. setup.py
"""Setup script for AGV RTLS Dashboard."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="agv-rtls-dashboard",
    version="2.0.0",
    author="AGV RTLS Team",
    author_email="agv-rtls@company.com",
    description="Real-time Location System Dashboard for AGV Fleet Management",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/company/agv-rtls-dashboard",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Manufacturing",
        "Topic :: Industrial Automation :: AGV",
        "License :: Proprietary",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "agv-dashboard=src.dashboard.app:main",
            "agv-ingest=src.ingestion.mqtt_consumer:main",
            "agv-api=src.api.fastapi_app:run_api",
            "agv-calibrate=scripts.calibrate_transform:main",
            "agv-simulate=scripts.simulator:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.yaml", "*.yml", "*.json", "*.sql", "*.png", "*.geojson"],
    },
)