import io
import json
import os
from pathlib import Path
from random import random, randint
from textwrap import dedent
from typing import List, Tuple

import numpy as np
import pandas as pd
import pytest
from ecl.util.util import BoolVector
from numpy.testing import assert_almost_equal, assert_array_equal
from res.enkf import ErtRunContext
from res.enkf.enkf_main import EnKFMain
from res.enkf.res_config import ResConfig

from ert_shared.libres_facade import LibresFacade
from ert_shared.storage import extraction


@pytest.mark.parametrize(
    "x_axis, expected",
    [
        ([1, 2, 3, 4], ["1", "2", "3", "4"]),
        (["a", "b", "c"], ["a", "b", "c"]),
        (
            [pd.Timestamp(x, unit="d") for x in range(4)],
            [
                "1970-01-01T00:00:00",
                "1970-01-02T00:00:00",
                "1970-01-03T00:00:00",
                "1970-01-04T00:00:00",
            ],
        ),
    ],
)
def test_prepare_x_axis(x_axis, expected):
    assert expected == extraction._prepare_x_axis(x_axis)


class ErtConfigBuilder:
    def __init__(self):
        self.ensemble_size = 1
        self._priors = {}
        self._obs = []
        self.job_script = None

    def add_general_observation(self, observation_name, response_name, data):
        """Add GENERAL_OBSERVATION

        The `data` parameter is a pandas DataFrame. This is to be a two-column
        frame where the first column are the values and the second column are
        the errors. The index-column of this frame are the observation's indices
        which link the observations to the responses.

        """
        self._obs.append((observation_name, response_name, data.copy()))

    def add_prior(self, name, entry):
        assert name not in self._priors
        self._priors[name] = entry
        return self

    def build(self, path=None):
        if path is None:
            path = Path.cwd()

        self._build_ert(path)
        self._build_job(path)
        self._build_observations(path)
        self._build_priors(path)

        config = ResConfig(str(path / "test.ert"))
        enkfmain = EnKFMain(config)

        # The C code doesn't do resource counting correctly, so we need to hook
        # ResConfig to EnKFMain because otherwise ResConfig will be deleted and
        # EnKFMain will use a dangling pointer.
        enkfmain.__config = config

        return LibresFacade(enkfmain)

    def _build_ert(self, path):
        f = (path / "test.ert").open("w")

        # Default
        f.write(
            "JOBNAME poly_%d\n"
            "QUEUE_SYSTEM LOCAL\n"
            "QUEUE_OPTION LOCAL MAX_RUNNING 50\n"
            f"NUM_REALIZATIONS {self.ensemble_size}\n"
        )

    def _build_job(self, path):
        f = (path / "test.ert").open("a")
        f.write("INSTALL_JOB job JOB\n" "SIMULATION_JOB job\n")

        if self.job_script is None:
            (path / "JOB").write_text("EXECUTABLE /usr/bin/true\n")
        else:
            (path / "JOB").write_text(f"EXECUTABLE {path}/script\n")
            (path / "script").write_text(self.job_script)

    def _build_observations(self, path):
        """
        Creates a TIME_MAP and OBS_CONFIG entry in the ERT config. The TIME_MAP is
        required for ERT to load the OBS_CONFIG.

        Creates an 'obs_config.txt' file into which the generate observations are written.
        """
        if not self._obs:
            return

        (path / "time_map").write_text("1/10/2006\n")
        with (path / "test.ert").open("a") as f:
            f.write("OBS_CONFIG obs_config.txt\n")
            f.write("TIME_MAP time_map\n")
            f.write(
                "GEN_DATA RES RESULT_FILE:poly_%d.out REPORT_STEPS:0 INPUT_FORMAT:ASCII\n"
            )

        with (path / "obs_config.txt").open("w") as f:
            for obs_name, resp_name, data in self._obs:
                indices = ",".join(str(index) for index in data.index.tolist())

                f.write(
                    f"GENERAL_OBSERVATION {obs_name} {{\n"
                    f"    DATA       = {resp_name};\n"
                    f"    INDEX_LIST = {indices};\n"
                    f"    RESTART    = 0;\n"
                    f"    OBS_FILE   = {obs_name}.txt;\n"
                    "};\n"
                )
                with (path / f"{obs_name}.txt").open("w") as fobs:
                    data.to_csv(fobs, sep=" ", header=False, index=False)

    def _build_priors(self, path):
        if not self._priors:
            return

        with (path / "test.ert").open("a") as f:
            f.write("GEN_KW COEFFS coeffs.json.in coeffs.json coeffs_priors\n")
        with (path / "coeffs.json.in").open("w") as f:
            f.write("{\n")
            f.write(",\n".join(f'  "{name}": <{name}>' for name in self._priors))
            f.write("\n}\n")
        with (path / "coeffs_priors").open("w") as f:
            for name, entry in self._priors.items():
                f.write(f"{name} {entry}\n")


@pytest.fixture(autouse=True)
def _chdir_tmp_path(monkeypatch, tmp_path):
    """
    All tests in this file must be run in a clean directory
    """
    monkeypatch.chdir(tmp_path)


def test_empty_ensemble(client):
    ert = ErtConfigBuilder().build()
    extraction.post_ensemble_data(ert, -1)

    id = client.fetch_experiment()

    # Name is "default"
    for ens in client.get(f"/experiments/{id}/ensembles").json():
        assert (
            client.get(f"/ensembles/{ens['id']}/metadata").json()["name"] == "default"
        )

    # No priors exist
    assert client.get(f"/experiments/{id}/priors").json() == {}


def test_empty_ensemble_with_name(client):
    name = _rand_name()

    # Create case with given name
    ert = ErtConfigBuilder().build()
    ert.select_or_create_new_case(name)

    # Post initial ensemble
    extraction.post_ensemble_data(ert, -1)

    # Compare results
    id = client.fetch_experiment()
    for ens in client.get(f"/experiments/{id}/ensembles").json():
        assert client.get(f"/ensembles/{ens['id']}/metadata").json()["name"] == name


def test_priors(client):
    priors = _make_priors()

    # Add priors to ERT config
    builder = ErtConfigBuilder()
    for name, conf, _ in priors:
        builder.add_prior(name, conf)
    ert = builder.build()

    # Start ERT
    _create_runpath(ert)

    # Post initial ensemble
    extraction.post_ensemble_data(ert, -1)

    # Compare results
    id = client.fetch_experiment()
    actual_priors = client.get(f"/experiments/{id}/priors").json()
    assert len(priors) == len(actual_priors)
    for name, _, resp in priors:
        assert actual_priors[f"COEFFS:{name}"] == resp


def test_parameters(client):
    priors = _make_priors()

    # Add priors to ERT config
    builder = ErtConfigBuilder()
    builder.ensemble_size = 10  # randint(5, 20)
    for name, conf, _ in priors:
        builder.add_prior(name, conf)
    ert = builder.build()

    # Start ERT
    _create_runpath(ert)

    # Post initial ensemble
    extraction.post_ensemble_data(ert, -1)

    # Get ensemble_id
    experiment_id = client.fetch_experiment()
    ensembles = client.get(f"/experiments/{experiment_id}/ensembles").json()
    ensemble_id = ensembles[0]["id"]

    # Compare parameters (+ 2 due to the two log10_ coeffs)
    parameters = client.get(f"/ensembles/{ensemble_id}/parameters").json()
    assert len(parameters) == len(priors) + 2
    for name, _, prior in priors:
        assert f"COEFFS:{name}" in parameters
        if prior["function"] in ("lognormal", "loguniform"):
            assert f"LOG10_COEFFS:{name}" in parameters

    # Compare records (+ 2 due to the two log10_ coeffs)
    records = client.get(f"/ensembles/{ensemble_id}/records").json()
    assert len(records) == len(priors) + 2
    for name, _, prior in priors:
        assert f"COEFFS:{name}" in records
        if prior["function"] in ("lognormal", "loguniform"):
            assert f"LOG10_COEFFS:{name}" in records

    parameters_df = _get_parameters()
    assert len(parameters_df) == builder.ensemble_size
    for col in parameters_df:
        record_data = client.get(
            f"/ensembles/{ensemble_id}/records/COEFFS:{col}",
            headers={"accept": "application/x-dataframe"},
        ).content
        stream = io.BytesIO(record_data)
        df = pd.read_csv(stream, index_col=0, float_precision="round_trip")

        # ERT produces a low-quality version
        assert_almost_equal(parameters_df[col].values, df.values.flatten(), decimal=4)


def test_observations(client):
    data = pd.DataFrame([[1, 0.1], [2, 0.2], [3, 0.3], [4, 0.4]], index=[2, 4, 6, 8])

    builder = ErtConfigBuilder()
    builder.add_general_observation("OBS", "RES", data)
    ert = builder.build()

    # Post ensemble
    extraction.post_ensemble_data(ert, builder.ensemble_size)

    # Experiment should have 1 observation
    experiment_id = client.fetch_experiment()
    observations = client.get(f"/experiments/{experiment_id}/observations").json()
    assert len(observations) == 1

    # Validate data
    obs = observations[0]
    assert obs["name"] == "OBS"
    assert obs["values"] == data[0].tolist()
    assert obs["errors"] == data[1].tolist()
    assert obs["x_axis"] == data.index.astype(str).tolist()
    assert obs["transformation"] is None


def test_post_ensemble_results(client):
    data = pd.DataFrame([[1, 0.1], [2, 0.2], [3, 0.3], [4, 0.4]], index=[2, 4, 6, 8])
    response_name = "RES"

    # Add priors to ERT config
    builder = ErtConfigBuilder()
    builder.ensemble_size = 2
    builder.add_general_observation("OBS", response_name, data)

    data = [0.0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9]
    df = pd.DataFrame(data)

    # Create job
    script = dedent(
        f"""\
    #!/usr/bin/python3

    if __name__ == "__main__":
        output = {str(data)}
        with open("poly_0.out", "w") as f:
            f.write("\\n".join(map(str, output)))
    """
    )
    builder.job_script = script

    ert = builder.build()

    # Create runpath and run ERT
    run_context = _create_runpath(ert)
    _evaluate_ensemble(ert, run_context)

    # Post initial ensemble
    ensemble_id = extraction.post_ensemble_data(ert, builder.ensemble_size)

    # Post ensemble results
    extraction.post_ensemble_results(ert, ensemble_id)

    # Retrieve response data
    data = client.get(f"/ensembles/{ensemble_id}/responses/{response_name}/data")
    stream = io.BytesIO(data.content)
    response_df = pd.read_csv(stream, index_col=0, float_precision="round_trip")
    for realization in range(0, builder.ensemble_size):
        assert_array_equal(response_df.loc[realization].values, df.values.flatten())


def test_post_update_data(client):
    data = pd.DataFrame(np.random.rand(4, 2), index=[2, 4, 6, 8])

    builder = ErtConfigBuilder()
    builder.add_general_observation("OBS", "RES", data)
    ert = builder.build()

    # Post two ensembles
    parent_ensemble_id = extraction.post_ensemble_data(ert, builder.ensemble_size)
    update_id = extraction.post_update_data(ert, parent_ensemble_id, "boruvka")
    child_ensemble_id = extraction.post_ensemble_data(
        ert, builder.ensemble_size, update_id
    )

    # Experiment should have two ensembles
    experiment_id = client.fetch_experiment()
    ensembles = client.get(f"/experiments/{experiment_id}/ensembles").json()
    assert len(ensembles) == 2

    # Parent ensemble should have a child
    assert ensembles[0]["child_ensemble_ids"] == [child_ensemble_id]
    assert ensembles[0]["parent_ensemble_id"] is None

    # Child ensemble should have a parent
    assert ensembles[1]["child_ensemble_ids"] == []
    assert ensembles[1]["parent_ensemble_id"] == parent_ensemble_id


def _make_priors() -> List[Tuple[str, str, dict]]:
    def normal():
        # trans_normal @ libres/enkf/trans_func.cpp
        #
        # Well defined for all real values of a and b
        a, b = random(), random()
        return (f"NORMAL {a} {b}", dict(function="normal", mean=a, std=b))

    def lognormal():
        # trans_lognormal @ libres/enkf/trans_func.cpp
        #
        # Well defined for all real values of a and b
        a, b = random(), random()
        return (
            f"LOGNORMAL {a} {b}",
            {"function": "lognormal", "mean": a, "std": b},
        )

    def truncnormal():
        # trans_truncated_normal @ libres/enkf/trans_func.cpp
        #
        # Well defined for all real values of a, b, c and d
        a, b, c, d = [random() for _ in range(4)]
        return (
            f"TRUNCATED_NORMAL {a} {b} {c} {d}",
            {
                "function": "ert_truncnormal",
                "mean": a,
                "std": b,
                "min": c,
                "max": d,
            },
        )

    def uniform():
        # trans_unif @ libres/enkf/trans_func.cpp
        #
        # Well defined for all real values of a and b
        a, b = random(), random()
        return (f"UNIFORM {a} {b}", {"function": "uniform", "min": a, "max": b})

    def loguniform():
        # trans_logunif @ libres/enkf/trans_func.cpp
        #
        # Well defined for strictly positive a, b due to log()
        a, b = random() + 1, random() + 1  # +1 to avoid zero
        return (
            f"LOGUNIF {a} {b}",
            {"function": "loguniform", "min": a, "max": b},
        )

    def const():
        a = random()
        return (f"CONST {a}", {"function": "const", "value": a})

    def duniform():
        # trans_dunif @ libres/enkf/trans_func.cpp
        #
        # Well defined for all real values of b and c, integer values >= 2 of
        # bins (due to division by [bins - 1])
        bins = randint(2, 100)
        b, c = random(), random()
        return (
            f"DUNIF {bins} {b} {c}",
            {"function": "ert_duniform", "bins": bins, "min": b, "max": c},
        )

    def erf():
        # trans_errf @ libres/enkf/trans_func.cpp
        #
        # Well defined for all real values of a, b, c, non-zero real values of d
        # (width) due to division by zero.
        a, b, c, d = [random() + 1 for _ in range(4)]  # +1 to all to avoid zero
        return (
            f"ERRF {a} {b} {c} {d}",
            {"function": "ert_erf", "min": a, "max": b, "skewness": c, "width": d},
        )

    def derf():
        # trans_derrf @ libres/enkf/trans_func.cpp
        #
        # Well defined for all real values of a, b, c, non-zero real values of d
        # (width) due to division by zero, integer values >= 2 of bins due to
        # division by (bins - 1)
        bins = randint(2, 100)
        a, b, c, d = [random() + 1 for _ in range(4)]  # +1 to all to avoid zero
        return (
            f"DERRF {bins} {a} {b} {c} {d}",
            {
                "function": "ert_derf",
                "bins": bins,
                "min": a,
                "max": b,
                "skewness": c,
                "width": d,
            },
        )

    return [
        (_rand_name(), *p())
        for p in (
            normal,
            lognormal,
            truncnormal,
            uniform,
            loguniform,
            const,
            duniform,
            erf,
            derf,
        )
    ]


def _rand_name():
    import random, string

    return "".join(random.choice(string.ascii_lowercase) for _ in range(8))


def _create_runpath(ert: LibresFacade, iteration: int = 0) -> ErtRunContext:
    """
    Instantiate an ERT runpath. This will create the parameter coefficients.
    """
    enkf_main = ert._enkf_main
    result_fs = ert.get_current_fs()

    model_config = enkf_main.getModelConfig()
    runpath_fmt = model_config.getRunpathFormat()
    jobname_fmt = model_config.getJobnameFormat()
    subst_list = enkf_main.getDataKW()

    run_context = ErtRunContext.ensemble_experiment(
        result_fs,
        BoolVector(default_value=True, initial_size=ert.get_ensemble_size()),
        runpath_fmt,
        jobname_fmt,
        subst_list,
        iteration,
    )

    enkf_main.getEnkfSimulationRunner().createRunPath(run_context)
    return run_context


def _evaluate_ensemble(ert: LibresFacade, run_context: ErtRunContext):
    """
    Launch ensemble experiment with the created config
    """
    queue_config = ert.get_queue_config()
    _job_queue = queue_config.create_job_queue()

    ert._enkf_main.getEnkfSimulationRunner().runEnsembleExperiment(
        _job_queue, run_context
    )


def _get_parameters() -> pd.DataFrame:
    params_json = [
        json.loads(path.read_text())
        for path in sorted(Path.cwd().glob("simulations/realization*/coeffs.json"))
    ]

    return pd.DataFrame(params_json)
