import ert3

import flaky
import numpy as np
import pytest
import scipy


def approx(x, eps=0.2):
    return pytest.approx(x, abs=eps, rel=eps)


@flaky.flaky(max_runs=3, min_passes=2)
@pytest.mark.parametrize(
    ("size", "mean", "std"),
    (
        (30000, 0, 1),
        (30000, 10, 10),
    ),
)
def test_gaussian_distribution(size, mean, std):
    gauss = ert3.stats.Gaussian(mean, std, size=size)

    assert gauss.mean == mean
    assert gauss.std == std

    prev_samples = set()
    for _ in range(10):
        sample = gauss.sample()

        assert len(sample.data) == size
        assert sorted(gauss.index) == sorted(range(size))

        assert tuple(sample.data) not in prev_samples
        prev_samples.add(tuple(sample.data))

        assert np.array(sample.data).mean() == approx(mean)
        assert np.array(sample.data).std() == approx(std)


@flaky.flaky(max_runs=3, min_passes=2)
@pytest.mark.parametrize(
    ("index", "mean", "std"),
    (
        (("a", "b", "c"), 0, 1),
        (tuple("a" * i for i in range(1, 10)), 2, 5),
    ),
)
def test_gaussian_distribution_index(index, mean, std):
    gauss = ert3.stats.Gaussian(mean, std, index=index)

    assert gauss.mean == mean
    assert gauss.std == std

    samples = {idx: [] for idx in index}
    for i in range(2000):
        sample = gauss.sample()
        assert sorted(gauss.index) == sorted(sample.index)
        assert sorted(sample.index) == sorted(index)

        for key in index:
            samples[key].append(sample.data[key])

    for key in index:
        s = np.array(samples[key])
        assert s.mean() == approx(mean)
        assert s.std() == approx(std)


def test_gaussian_distribution_invalid():
    err_msg_neither = "Cannot create distribution with neither size nor index"
    with pytest.raises(ValueError, match=err_msg_neither):
        ert3.stats.Gaussian(0, 1)

    err_msg_both = "Cannot create distribution with both size and index"
    with pytest.raises(ValueError, match=err_msg_both):
        ert3.stats.Gaussian(0, 1, size=10, index=list(range(10)))


@flaky.flaky(max_runs=3, min_passes=2)
@pytest.mark.parametrize(
    ("size", "lower_bound", "upper_bound"),
    (
        (10000, 0, 1),
        (20000, 10, 20),
    ),
)
def test_uniform_distribution(size, lower_bound, upper_bound):
    uniform = ert3.stats.Uniform(lower_bound, upper_bound, size=size)

    assert uniform.lower_bound == lower_bound
    assert uniform.upper_bound == upper_bound

    prev_samples = set()
    for _ in range(10):
        sample = uniform.sample()

        assert len(sample.data) == size

        assert tuple(sample.data) not in prev_samples
        prev_samples.add(tuple(sample.data))

        assert np.array(sample.data).min() == approx(lower_bound)
        assert np.array(sample.data).mean() == approx((lower_bound + upper_bound) / 2)
        assert np.array(sample.data).max() == approx(upper_bound)


@flaky.flaky(max_runs=3, min_passes=2)
@pytest.mark.parametrize(
    ("index", "lower_bound", "upper_bound"),
    (
        (("a", "b", "c"), 0, 1),
        (tuple("a" * i for i in range(1, 10)), 2, 5),
    ),
)
def test_uniform_distribution_index(index, lower_bound, upper_bound):
    uniform = ert3.stats.Uniform(lower_bound, upper_bound, index=index)

    assert uniform.lower_bound == lower_bound
    assert uniform.upper_bound == upper_bound

    samples = {idx: [] for idx in index}
    for i in range(1000):
        sample = uniform.sample()
        assert sorted(sample.index) == sorted(index)

        for key in index:
            samples[key].append(sample.data[key])

    for key in index:
        s = np.array(samples[key])
        assert s.min() == approx(lower_bound)
        assert s.mean() == approx((lower_bound + upper_bound) / 2)
        assert s.max() == approx(upper_bound)


def test_uniform_distribution_invalid():
    err_msg_neither = "Cannot create distribution with neither size nor index"
    with pytest.raises(ValueError, match=err_msg_neither):
        ert3.stats.Uniform(0, 1)

    err_msg_both = "Cannot create distribution with both size and index"
    with pytest.raises(ValueError, match=err_msg_both):
        ert3.stats.Uniform(0, 1, size=10, index=list(range(10)))


@pytest.mark.parametrize(
    ("mean", "std", "q", "size", "index"),
    (
        (0, 1, 0.005, 3, None),
        (0, 1, 0.995, 1, None),
        (2, 5, 0.1, 5, None),
        (1, 2, 0.7, 10, None),
        (0, 1, 0.005, None, ("a", "b", "c")),
        (0, 1, 0.995, None, tuple(index_len * "x" for index_len in range(1, 10))),
        (2, 5, 0.1, None, ("x", "y")),
        (1, 2, 0.7, None, ("single_key",)),
    ),
)
def test_gaussian_ppf(mean, std, q, size, index):
    if size is not None:
        gauss = ert3.stats.Gaussian(mean, std, size=size)
    else:
        gauss = ert3.stats.Gaussian(mean, std, index=index)

    expected_value = scipy.stats.norm.ppf(q, loc=mean, scale=std)
    ppf_result = gauss.ppf(q)
    assert len(gauss.index) == len(ppf_result.data)
    assert sorted(gauss.index) == sorted(ppf_result.index)
    for idx in gauss.index:
        assert ppf_result.data[idx] == pytest.approx(expected_value)


@pytest.mark.parametrize(
    ("lower", "upper", "q", "size", "index"),
    (
        (0, 2, 0.5, 1, None),
        (2, 5, 0.1, 5, None),
        (1, 2, 0.7, 10, None),
        (0, 1, 0.005, None, ("a", "b", "c")),
        (0, 1, 0.995, None, tuple(index_len * "x" for index_len in range(1, 10))),
        (2, 5, 0.1, None, ("x", "y")),
        (1, 2, 0.7, None, ("single_key",)),
    ),
)
def test_uniform_ppf(lower, upper, q, size, index):
    if size is not None:
        dist = ert3.stats.Uniform(lower, upper, size=size)
    else:
        dist = ert3.stats.Uniform(lower, upper, index=index)

    expected_value = scipy.stats.uniform.ppf(q, loc=lower, scale=upper - lower)
    ppf_result = dist.ppf(q)
    assert len(dist.index) == len(ppf_result.data)
    assert sorted(dist.index) == sorted(ppf_result.index)
    for idx in dist.index:
        assert ppf_result.data[idx] == pytest.approx(expected_value)
