import uuid
import time
from typing import Optional
from pydantic import BaseModel

from src.resources.clients.analysis_client import AnalysisClient
from src.resources.clients.storage_client import StorageClient
from src.resources.database.entity import Database


class NextflowRunEntity:
    def __init__(self,
                 analysis_id: str,
                 keycloak_token: str,
                 pipeline_name: Optional[str] = None,
                 run_args: Optional[list[str]] = None,
                 run_id: Optional[str] = None,
                 time_created: Optional[float] = None) -> None:
        self.analysis_id = analysis_id
        self.pipeline_name = pipeline_name
        self.run_args = run_args
        self.keycloak_token = keycloak_token
        self.run_id = f"nf-run-{str(uuid.uuid4())}" if run_id is None else run_id
        self.time_created: float = time.time() if time_created is None else time_created

    @classmethod
    def from_database(cls, run_id: str, database: Database) -> 'NextflowRunEntity':
        nf_run = database.get_nf_run_by_run_id(run_id)
        return cls(analysis_id=nf_run.analysis_id,
                   keycloak_token=nf_run.keycloak_token,
                   run_id=nf_run.run_id,
                   time_created=nf_run.time_created)

    def start(self, database: Database, input_location: str, output_location: str) -> None:
        if None not in [self.pipeline_name, self.run_args]:
            database.create_nf_run(self.run_id,
                                   self.analysis_id,
                                   self.keycloak_token,
                                   self.time_created)
            storage_client = StorageClient()
            # TODO: Retrieve and delete data from StorageClient [Step 3]
            # TODO: Execute Nextflow run command using input- and output_location [Step 4]

    def stop(self) -> None:
        # TODO: Stop Nextflow run, during cleanup [Step 10] or during manual interrupt
        pass

    def conclude(self, run_status: str, storage_location: str) -> None:
        storage_client = StorageClient()  # TODO: Initialize StorageClient
        analysis_client = AnalysisClient()  # TODO: Initialize AnalysisClient
        # TODO: Check run_status
        if run_status != 'success':
            # TODO: If successful, create result_storage with StorageClient using storage_location  [Step 7]
            pass
        # TODO: Inform analysis via AnalysisClient about conclusion (deliver result_storage id, if successful)  [Step 8]
        self.stop()
        # TODO: Cleanup resources (also in storage client) [Step 10]


class CreateNextflowRun(BaseModel):
    analysis_id: str = 'analysis_id'
    pipeline_name: str = 'pipeline_name'
    run_args: list[str] = []
    keycloak_token: str = 'keycloak_token'
    input_location: str = 'input_location'
    output_location: str = 'output_location'


class ConcludeNextflowRun(BaseModel):
    run_id: str = 'analysis_id'
    run_status: str = 'run_status'
    storage_location: str = 'storage_location'
