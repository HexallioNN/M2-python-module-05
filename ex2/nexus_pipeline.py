from abc import ABC, abstractmethod
from typing import Any, List, Protocol, Union, Dict
import time


# -------------------------
# 1. Protocol (Duck Typing)
# -------------------------
class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


# -------------------------
# 2. Abstract Pipeline
# -------------------------
class ProcessingPipeline(ABC):
    def __init__(self) -> None:
        self.stages: List[ProcessingStage] = []
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())
        self.stats: Dict[str, Union[int, float]] = {
            "processed": 0,
            "success": 0,
            "failed": 0,
            "total_time": 0.0
        }

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        pass

    def run(self, data: Any) -> Any:
        start = time.perf_counter()
        self.stats["processed"] += 1

        try:
            for stage in self.stages:
                data = stage.process(data)
            self.stats["success"] += 1
            return data
        except Exception as e:
            self.stats["failed"] += 1
            return f"Recovery successful: backup output used after error \
-> {e}"
        finally:
            self.stats["total_time"] += time.perf_counter() - start

    def get_stats(self) -> Dict[str, Union[int, float]]:
        return self.stats


# -------------------------
# 3. Stages (NO inheritance)
# -------------------------
class InputStage:
    def process(self, data: Any) -> Dict:
        result: Dict = {}

        try:
            if isinstance(data, dict):
                result = data.copy()

            elif isinstance(data, str):
                parts = data.split(",")
                if len(parts) >= 3:
                    result["user"] = parts[0]
                    result["actions"] = parts[1:-1]
                    result["timestamp"] = parts[-1]
                else:
                    result = {"error": "Invalid CSV format"}
            else:
                result = {"error": "Unsupported data type"}
            result["valid"] = "error" not in result

        except Exception as e:
            result = {
                "valid": False,
                "error": str(e)
            }

        return result


class TransformStage:
    def process(self, data: Any) -> Dict:
        value = data.get("value")
        if value is not None:
            if value > 25:
                data["status"] = "High"
            else:
                data["status"] = "Normal"

        if "actions" in data:
            data["action_count"] = len(data["actions"])
        elif "action" in data:
            data["action_count"] = 1

        return data


class OutputStage:
    def process(self, data: Any) -> str:
        value = data.get("value")
        if value is not None:
            return (f"Processed {data.get('sensor')} reading: "
                    f"{data['value']}°{data['unit']} ({data['status']} range)")

        if "actions" in data:
            return (f"{data['user']} activity logged: "
                    f"{data['action_count']} actions processed")
        return ""


# -------------------------
# 4. Adapters (Inheritance)
# -------------------------
class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        return self.run(data)


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        ...


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        ...


# -------------------------
# 5. Nexus Manager
# -------------------------
class NexusManager:
    def __init__(self) -> None:
        self.pipelines: Dict[str, ProcessingPipeline] = {}

    def add_pipeline(self, pipeline_id: str, pipeline:
                     ProcessingPipeline) -> None:
        ...

    def process(self, pipeline_id: str, data: Any) -> Any:
        ...


if __name__ == "__main__":
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
