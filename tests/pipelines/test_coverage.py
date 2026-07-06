"""Guard: every active pipeline must ship golden fixtures."""
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
