"""Guard: every active pipeline must ship golden fixtures.

The golden test in ``test_staging.py`` only runs for pipelines that
ship the fixture trio (input, table config, expected output) - a
pipeline without them would be silently skipped, never failed. This
test closes that gap: it discovers every ACTIVE pipeline - a source
registered in ``STAGING_SOURCES`` (run by the dynamic staging DAGs)
or a directory shipping its own module (``pipelines/<name>/<name>.py``,
exclusive treatment) - and fails if any of the three fixture files is
missing, so a new pipeline cannot reach CI without golden coverage.
"""
import os

import pytest
from staging_pipeline import STAGING_SOURCES

from tests.pipelines.fixtures import PIPELINES_ROOT

REQUIRED_FIXTURES = (
    "input_{name}.json",
    "staging_{name}.json",
    "output_staging_{name}.json",
)


def discover_active_pipelines() -> list[str]:
    """List every active pipeline under pipelines/.

    A pipeline is active when it is registered in ``STAGING_SOURCES``
    (declarative source run by the dynamic staging DAGs) or when its
    directory holds the module named after it
    (`pipelines/<name>/<name>.py`) - exclusive-treatment pipelines.

    Returns:
        Sorted pipeline names.
    """
    exclusive = {
        name
        for name in os.listdir(PIPELINES_ROOT)
        if os.path.isfile(os.path.join(PIPELINES_ROOT, name, f"{name}.py"))
    }
    return sorted(set(STAGING_SOURCES) | exclusive)


def test_at_least_one_active_pipeline_exists():
    assert discover_active_pipelines(), f"no active pipeline found under {PIPELINES_ROOT}"


@pytest.mark.parametrize("name", discover_active_pipelines())
def test_active_pipeline_ships_golden_fixtures(name):
    missing = [
        fixture.format(name=name)
        for fixture in REQUIRED_FIXTURES
        if not os.path.isfile(os.path.join(PIPELINES_ROOT, name, fixture.format(name=name)))
    ]

    assert not missing, (
        f"active pipeline '{name}' misses golden fixtures: {', '.join(missing)} "
        "- the golden test in test_staging.py would silently skip it"
    )
