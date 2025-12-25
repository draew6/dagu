import json
from typing import Literal
import httpx
from pydantic import BaseModel
from .settings import DaguSettings


class Flow(BaseModel):
    file_name: str
    tags: list[str]


Status = Literal["not_started", "running", "failed", "cancelled", "success", "skipped"]

class Dagu:
    def __init__(self):
        settings = DaguSettings()
        self._client = httpx.AsyncClient(
            base_url=settings.dagu_base_url + "/api/v2",
            timeout=10.0,
            auth=(settings.dagu_username, settings.dagu_password),
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._client.aclose()

    async def enqueue_run(
        self,
        file_name: str,
        params: dict | None = None,
    ) -> str:
        """Enqueue a run."""
        endpoint = f"/dags/{file_name}/enqueue"
        api_response = await self._client.request(
            "POST", endpoint, json={"params": json.dumps(params)}
        )
        api_response.raise_for_status()
        dag_run_id = api_response.json()["dagRunId"]
        return dag_run_id

    async def _get_flow_detail(
            self,
            file_name: str
    ):
        """Get flow detail."""
        endpoint = f"/dags/{file_name}"
        api_response = await self._client.request("GET", endpoint)
        api_response.raise_for_status()
        return api_response.json()

    async def _get_last_dag_run_id_and_name(
            self,
            file_name: str
    ):
        """Get last dag run."""
        detail = await self._get_flow_detail(file_name)
        run_id = detail["latestDAGRun"]["dagRunId"]
        name = detail["latestDAGRun"]["name"]
        return run_id, name


    async def _update_step_status(
            self,
            name: str,
            dag_run_id: str,
            step_name: str,
            status: Status
    ):
        status_mapping = {
            "not_started": 0,
            "running": 1,
            "failed": 2,
            "cancelled": 3,
            "success": 4,
            "skipped": 5
        }

        """Update step status."""
        endpoint = f"/dag-runs/{name}/{dag_run_id}/steps/{step_name}/status"
        api_response = await self._client.request(
        "PATCH", endpoint, json={"status": status_mapping[status]}
        )
        api_response.raise_for_status()


    async def set_step_status(self, file_name: str, step_name: str, status: Status):
        """Set step status."""
        dag_run_id, name = await self._get_last_dag_run_id_and_name(file_name)
        await self._update_step_status(name, dag_run_id, step_name, status)

    async def _get_workflows(self):
        """Get list of workflows."""
        endpoint = "/dags"
        api_response = await self._client.request("GET", endpoint)

        api_response.raise_for_status()
        return api_response.json()["dags"]

    async def send_event(self, name: str, data: dict):
        """Send event."""
        workflows = await self._get_workflows()
        flows = [Flow(file_name=flow["fileName"], tags=flow["dag"]["tags"]) for flow in workflows]
        for flow in flows:
            if name in flow.tags:
                await self.enqueue_run(flow.file_name, params=data)

