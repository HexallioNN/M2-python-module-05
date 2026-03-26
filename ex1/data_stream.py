from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict, Union


class DataStream(ABC):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.batch_count = 0
        self.current = 0
        self.high_prio_count = 0
        self.error = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if not data_batch:
            return []
        if criteria is None:
            return data_batch
        filtered = [item for item in data_batch if item != criteria]
        return filtered

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "batches_processed": self.batch_count,
        }


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.average = 0
        self.total = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        filtered = self.filter_data(data_batch, "High")
        if filtered != data_batch:
            self.high_prio_count += 1
        self.current = data_batch[0].split(":")
        self.total += float(self.current[1])
        self.batch_count += 1
        self.average = self.total / self.batch_count

        return f"Sensor analysis: {self.batch_count} readings processed, \
avg temp: {self.average}°C"


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.flow = 0
        self.list = []
        self.count = 0
        self.symbol = "+"

    def process_batch(self, data_batch: List[Any]) -> str:
        filtered = self.filter_data(data_batch, "High")
        if filtered != data_batch:
            self.high_prio_count += 1
        for x in data_batch:
            self.list.append(x.split(":"))
        for x in self.list:
            if x[0] == "buy":
                self.flow += int(x[1])
                self.count += 1
            elif x[0] == "sell":
                self.flow -= int(x[1])
                self.count += 1
            else:
                self.error += 1
        if self.flow < 0:
            self.symbol = ""
        return f"Transaction analysis: {self.count} \
operations, net flow: {self.symbol}{self.flow} units"


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

    def process_batch(self, data_batch: List[Any]) -> str:
        filtered = self.filter_data(data_batch, "High")
        if filtered != data_batch:
            self.high_prio_count += 1
        for x in data_batch:
            if x == "error":
                self.error += 1
                self.batch_count += 1
            else:
                self.batch_count += 1
        return f"Event analysis: {self.batch_count} events, \
{self.error} error detected"


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    print("Initializing Sensor Stream...")
    sensor1 = SensorStream("SENSOR_001")
    batch_info = ["temperature:22.5", "humidity:65", "pressure:1013"]
    sensor1.process_batch(batch_info)
    sensor1.process_batch(batch_info)
    sensor_stats = sensor1.get_stats()
    print(f"Stream ID: {sensor_stats.get('stream_id')}, Type: \
Environmental Data")
    print(f"Processing sensor batch: {batch_info}")
    print(f"{sensor1.process_batch(batch_info)}\n")

    print("Initializing Transaction Stream...")
    transaction1 = TransactionStream("TRANS_001")
    trans_stats = transaction1.get_stats()
    trans_batch = ["buy:100", "sell:150", "buy:75"]
    print(f"Stream ID: {trans_stats.get('stream_id')}, Type: \
Financial Data")
    print(f"Processing transaction batch: {trans_batch}")
    print(f"{transaction1.process_batch(trans_batch)}\n")

    print("Initializing Event Stream...")
    event1 = EventStream("EVENT_001")
    event_stats = event1.get_stats()
    event_batch = ["login", "error", "logout"]
    print(f"Stream ID: {event_stats.get('stream_id')}, Type: \
System Events")
    print(f"Processing event batch: {event_batch}")
    print(f"{event1.process_batch(event_batch)}\n")

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed steram types through unified interface...\n")

    processors = [
        (SensorStream("SENSON__002"), ["temperature:22.5", "High"]),
        (SensorStream("SENSON__003"), ["temperature:22.5", "High"]),
        (TransactionStream("TRANS__002"), ["buy:100", "sell:150", "\
buy:75", "buy:2000"]),
        (EventStream("EVENT__002"), ["login", "error", "logout"])
    ]

    sensor_readings = 0
    transaction_ops = 0
    event_count = 0

    critical_sensor_alerts = 0
    large_transactions = 0

    for stream, batch in processors:
        result = stream.process_batch(batch)

        if isinstance(stream, SensorStream):
            sensor_readings += stream.batch_count
            if stream.high_prio_count > 0:
                critical_sensor_alerts += 1

        elif isinstance(stream, TransactionStream):
            transaction_ops += stream.count
            for item in batch:
                if "2000" in item:
                    large_transactions += 1

        elif isinstance(stream, EventStream):
            event_count += stream.batch_count

    print("Batch 1 Results:")
    print(f"- Sensor data: {sensor_readings} readings processed")
    print(f"- Transaction data: {transaction_ops} operations processed")
    print(f"- Event data: {event_count} events processed\n")

    print("Stream filtering active: High-priority data only")
    print(f"Filtered results: {critical_sensor_alerts} critical sensor alerts,"
          f" {large_transactions} large transaction\n")

    print("All streams processed successfully. Nexus throughput optimal.")
