import copy
import datetime
import typing
from collections import defaultdict
from typing import Dict, Iterable, Optional, Any

import pyrsistent
from dateutil.parser import parse
from pydantic import BaseModel

from ert_shared.ensemble_evaluator.entity import identifiers as ids
from ert_shared.ensemble_evaluator.entity.tool import (
    get_job_id,
    get_real_id,
    get_stage_id,
    get_step_id,
    recursive_update,
)
from ert_shared.status.entity import state


class UnsupportedOperationException(ValueError):
    pass


_FM_TYPE_EVENT_TO_STATUS = {
    ids.EVTYPE_FM_STAGE_WAITING: state.STAGE_STATE_WAITING,
    ids.EVTYPE_FM_STAGE_PENDING: state.STAGE_STATE_PENDING,
    ids.EVTYPE_FM_STAGE_RUNNING: state.STAGE_STATE_PENDING,
    ids.EVTYPE_FM_STAGE_FAILURE: state.STAGE_STATE_FAILURE,
    ids.EVTYPE_FM_STAGE_SUCCESS: state.STAGE_STATE_SUCCESS,
    ids.EVTYPE_FM_STAGE_UNKNOWN: state.STAGE_STATE_UNKNOWN,
    ids.EVTYPE_FM_STEP_START: state.STEP_STATE_START,
    ids.EVTYPE_FM_STEP_FAILURE: state.STEP_STATE_FAILURE,
    ids.EVTYPE_FM_STEP_SUCCESS: state.STEP_STATE_SUCCESS,
    ids.EVTYPE_FM_JOB_START: state.JOB_STATE_START,
    ids.EVTYPE_FM_JOB_RUNNING: state.JOB_STATE_RUNNING,
    ids.EVTYPE_FM_JOB_SUCCESS: state.JOB_STATE_FINISHED,
    ids.EVTYPE_FM_JOB_FAILURE: state.JOB_STATE_FAILURE,
}

_ENSEMBLE_TYPE_EVENT_TO_STATUS = {
    ids.EVTYPE_ENSEMBLE_STARTED: state.ENSEMBLE_STATE_STARTED,
    ids.EVTYPE_ENSEMBLE_STOPPED: state.ENSEMBLE_STATE_STOPPED,
    ids.EVTYPE_ENSEMBLE_CANCELLED: state.ENSEMBLE_STATE_CANCELLED,
    ids.EVTYPE_ENSEMBLE_FAILED: state.ENSEMBLE_STATE_FAILED,
}


def convert_iso8601_to_datetime(timestamp):
    if isinstance(timestamp, datetime.datetime):
        return timestamp

    return parse(timestamp)


class PartialSnapshot:
    def __init__(self, snapshot):
        """Create a PartialSnapshot. If no snapshot is provided, the object is
        a immutable POD, and any attempt at mutating it will raise an
        UnsupportedOperationException."""
        self._data = pyrsistent.m()
        self._snapshot = copy.copy(snapshot) if snapshot else None

    def update_status(self, status):
        self._apply_update(SnapshotDict(status=status))

    def update_real(
        self,
        real_id,
        real,
    ):
        self._apply_update(SnapshotDict(reals={real_id: real}))

    def update_stage(
        self,
        real_id,
        stage_id,
        stage,
    ):
        self._apply_update(
            SnapshotDict(reals={real_id: Realization(stages={stage_id: stage})})
        )
        if self._snapshot.get_real(real_id).status != state.REALIZATION_STATE_FAILED:
            if stage.status in [
                state.REALIZATION_STATE_FAILED,
                state.REALIZATION_STATE_PENDING,
                state.REALIZATION_STATE_RUNNING,
            ]:
                self.update_real(real_id, Realization(status=stage.status))
            elif (
                stage.status == state.REALIZATION_STATE_FINISHED
                and self._snapshot.all_stages_finished(real_id)
            ):
                self.update_real(real_id, Realization(status=stage.status))

    def _apply_update(self, update):
        if self._snapshot is None:
            raise UnsupportedOperationException(
                f"trying to mutate {self.__class__} without providing a snapshot is not supported"
            )
        dictionary = update.dict(
            exclude_unset=True, exclude_none=True, exclude_defaults=True
        )
        self._data = recursive_update(self._data, dictionary, check_key=False)
        self._snapshot.merge(dictionary)

    def update_step(self, real_id, stage_id, step_id, step):
        self._apply_update(
            SnapshotDict(
                reals={
                    real_id: Realization(
                        stages={stage_id: Stage(steps={step_id: step})}
                    )
                }
            )
        )
        if (
            self._snapshot.get_stage(real_id, stage_id).status
            != state.REALIZATION_STATE_FAILED
        ):
            if step.status in [
                state.REALIZATION_STATE_FAILED,
                state.REALIZATION_STATE_PENDING,
                state.REALIZATION_STATE_RUNNING,
            ]:
                self.update_stage(real_id, stage_id, Stage(status=step.status))
            elif (
                step.status == state.STEP_STATE_SUCCESS
                and self._snapshot.all_steps_finished(real_id, stage_id)
            ):
                self.update_stage(real_id, stage_id, Stage(status=step.status))

    def update_job(
        self,
        real_id,
        stage_id,
        step_id,
        job_id,
        job,
    ):

        self._apply_update(
            SnapshotDict(
                reals={
                    real_id: Realization(
                        stages={
                            stage_id: Stage(steps={step_id: Step(jobs={job_id: job})})
                        }
                    )
                }
            )
        )

    def to_dict(self):
        return pyrsistent.thaw(self._data)

    def data(self):
        return self._data

    def from_cloudevent(self, event):
        e_type = event["type"]
        e_source = event["source"]
        status = _FM_TYPE_EVENT_TO_STATUS.get(e_type)
        timestamp = event["time"]

        if e_type in ids.EVGROUP_FM_STAGE:
            start_time = None
            end_time = None
            if e_type == ids.EVTYPE_FM_STAGE_RUNNING:
                start_time = convert_iso8601_to_datetime(timestamp)
            elif e_type in {ids.EVTYPE_FM_STAGE_FAILURE, ids.EVTYPE_FM_STAGE_SUCCESS}:
                end_time = convert_iso8601_to_datetime(timestamp)

            self.update_stage(
                get_real_id(e_source),
                get_stage_id(e_source),
                stage=Stage(
                    status=status,
                    start_time=start_time,
                    end_time=end_time,
                ),
            )
        elif e_type in ids.EVGROUP_FM_STEP:
            start_time = None
            end_time = None
            if e_type == ids.EVTYPE_FM_STEP_START:
                start_time = convert_iso8601_to_datetime(timestamp)
            elif e_type in {ids.EVTYPE_FM_STEP_SUCCESS, ids.EVTYPE_FM_STEP_FAILURE}:
                end_time = convert_iso8601_to_datetime(timestamp)

            self.update_step(
                get_real_id(e_source),
                get_stage_id(e_source),
                get_step_id(e_source),
                step=Step(
                    status=status,
                    start_time=start_time,
                    end_time=end_time,
                ),
            )
            if e_type == ids.EVTYPE_FM_STEP_START and event.data:
                for job_id, job in event.data.get(ids.JOBS, {}).items():
                    self.update_job(
                        get_real_id(e_source),
                        get_stage_id(e_source),
                        get_step_id(e_source),
                        job_id,
                        # XXX: can we remove this?
                        job=Job(
                            stdout=job[ids.STDOUT],
                            stderr=job[ids.STDERR],
                        ),
                    )

        elif e_type in ids.EVGROUP_FM_JOB:
            start_time = None
            end_time = None
            if e_type == ids.EVTYPE_FM_JOB_START:
                start_time = convert_iso8601_to_datetime(timestamp)
            elif e_type in {ids.EVTYPE_FM_JOB_SUCCESS, ids.EVTYPE_FM_JOB_FAILURE}:
                end_time = convert_iso8601_to_datetime(timestamp)

            self.update_job(
                get_real_id(e_source),
                get_stage_id(e_source),
                get_step_id(e_source),
                get_job_id(e_source),
                job=Job(
                    status=status,
                    start_time=start_time,
                    end_time=end_time,
                    data=event.data if e_type == ids.EVTYPE_FM_JOB_RUNNING else None,
                    error=event.data.get(ids.ERROR_MSG)
                    if e_type == ids.EVTYPE_FM_JOB_FAILURE
                    else None,
                ),
            )
        elif e_type in ids.EVGROUP_ENSEMBLE:
            self.update_status(_ENSEMBLE_TYPE_EVENT_TO_STATUS[e_type])
        elif e_type == ids.EVTYPE_EE_SNAPSHOT_UPDATE:
            self._data = recursive_update(self._data, event.data, check_key=False)
        else:
            raise ValueError("Unknown type: {}".format(e_type))
        return self


class Snapshot:
    def __init__(self, input_dict):
        self._data = pyrsistent.freeze(input_dict)

    def merge_event(self, event):
        self._data = recursive_update(self._data, event.data())

    def merge(self, update):
        self._data = recursive_update(self._data, update)

    def to_dict(self):
        return pyrsistent.thaw(self._data)

    def get_status(self):
        return self._data[ids.STATUS]

    def get_reals(self):
        return SnapshotDict(**self._data).reals

    def get_real(self, real_id):
        if real_id not in self._data[ids.REALS]:
            raise ValueError(f"No realization with id {real_id}")
        return Realization(**self._data[ids.REALS][real_id])

    def get_stage(self, real_id, stage_id):
        real = self.get_real(real_id)
        stages = real.stages
        if stage_id not in stages:
            raise ValueError(f"No stage with id {stage_id} in {real_id}")
        return stages[stage_id]

    def get_step(self, real_id, stage_id, step_id):
        stage = self.get_stage(real_id, stage_id)
        steps = stage.steps
        if step_id not in steps:
            raise ValueError(f"No step with id {step_id} in {stage_id}")
        return steps[step_id]

    def get_job(self, real_id, stage_id, step_id, job_id):
        step = self.get_step(real_id, stage_id, step_id)
        jobs = step.jobs
        if job_id not in jobs:
            raise ValueError(f"No job with id {job_id} in {step_id}")
        return jobs[job_id]

    def all_stages_finished(self, real_id):
        real = self.get_real(real_id)
        return all(
            stage.status == state.STAGE_STATE_SUCCESS for stage in real.stages.values()
        )

    def all_steps_finished(self, real_id, stage_id):
        stage = self.get_stage(real_id, stage_id)
        return all(
            step.status == state.STEP_STATE_SUCCESS for step in stage.steps.values()
        )

    def get_successful_realizations(self):
        return len(
            [
                real
                for real in self._data[ids.REALS].values()
                if real[ids.STATUS] == state.REALIZATION_STATE_FINISHED
            ]
        )

    def aggregate_real_states(self) -> typing.Dict[str, int]:
        states = defaultdict(int)
        for real in self._data[ids.REALS].values():
            states[real[ids.STATUS]] += 1
        return states


class JobDetails(BaseModel):
    job_id: str
    name: str


class ForwardModel(BaseModel):
    step_definitions: Dict[str, Dict[str, Iterable[JobDetails]]]


class Job(BaseModel):
    status: Optional[str]
    start_time: Optional[datetime.datetime]
    end_time: Optional[datetime.datetime]
    data: Optional[Dict]
    name: Optional[str]
    error: Optional[str]
    stdout: Optional[str]
    stderr: Optional[str]


class Step(BaseModel):
    status: Optional[str]
    start_time: Optional[datetime.datetime]
    end_time: Optional[datetime.datetime]
    jobs: Optional[Dict[str, Job]] = {}


class Stage(BaseModel):
    status: Optional[str]
    start_time: Optional[datetime.datetime]
    end_time: Optional[datetime.datetime]
    steps: Optional[Dict[str, Step]] = {}


class Realization(BaseModel):
    status: Optional[str]
    active: Optional[bool]
    start_time: Optional[datetime.datetime]
    end_time: Optional[datetime.datetime]
    stages: Optional[Dict[str, Stage]] = {}


class SnapshotDict(BaseModel):
    status: Optional[str]
    reals: Optional[Dict[str, Realization]] = {}
    forward_model: Optional[ForwardModel]
    metadata: Optional[Dict[str, Any]]


class SnapshotBuilder(BaseModel):
    stages: Dict[str, Stage] = {}
    forward_model: ForwardModel = ForwardModel(step_definitions={})
    metadata: Dict[str, Any] = {}

    def build(self, real_ids, status, start_time=None, end_time=None):
        top = SnapshotDict(
            status=status, forward_model=self.forward_model, metadata=self.metadata
        )
        for r_id in real_ids:
            top.reals[r_id] = Realization(
                active=True,
                stages=self.stages,
                start_time=start_time,
                end_time=end_time,
                status=status,
            )
        return Snapshot(top.dict())

    def add_stage(self, stage_id, status, start_time=None, end_time=None):
        self.stages[stage_id] = Stage(
            status=status,
            start_time=start_time,
            end_time=end_time,
        )
        return self

    def add_step(self, stage_id, step_id, status, start_time=None, end_time=None):
        stage = self.stages[stage_id]
        stage.steps[step_id] = Step(
            status=status, start_time=start_time, end_time=end_time
        )
        return self

    def add_job(
        self,
        stage_id,
        step_id,
        job_id,
        name,
        status,
        data,
        start_time=None,
        end_time=None,
        stdout=None,
        stderr=None,
    ):
        stage = self.stages[stage_id]
        step = stage.steps[step_id]
        step.jobs[job_id] = Job(
            status=status,
            data=data,
            start_time=start_time,
            end_time=end_time,
            name=name,
            stdout=stdout,
            stderr=stderr,
        )
        if stage_id not in self.forward_model.step_definitions:
            self.forward_model.step_definitions[stage_id] = {}
        if step_id not in self.forward_model.step_definitions[stage_id]:
            self.forward_model.step_definitions[stage_id][step_id] = []
        self.forward_model.step_definitions[stage_id][step_id].append(
            JobDetails(job_id=job_id, name=name)
        )
        return self

    def add_metadata(self, key, value):
        self.metadata[key] = value
        return self
