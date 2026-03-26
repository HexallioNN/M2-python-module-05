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

    @abstractmethod
    def get_metrics(self) -> Dict[str, int]:
        pass

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

    def get_metrics(self) -> Dict[str, int]:
        dict = {
            "sensor_readings": self.batch_count,
            "critical_alerts": self.high_prio_count
        }
        return dict


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

    def get_metrics(self) -> Dict[str, int]:
        return {
            "transaction_ops": self.count,
            "large_transactio\
ns": sum(1 for x in self.list if int(x[1]) >= 2000)
        }


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

    def get_metrics(self) -> Dict[str, int]:
        return {
            "event_count": self.batch_count,
            "errors": self.error
        }


class StreamProcessor:
    def __init__(self):
        self.processors = []

        self.sensor_readings = 0
        self.transaction_ops = 0
        self.event_count = 0

        self.critical_sensor_alerts = 0
        self.large_transactions = 0

    def add_stream(self, stream: DataStream, batch: List[Any]) -> None:
        self.processors.append((stream, batch))

    def process(self) -> None:
        print("=== Polymorphic Stream Processing ===")
        print("Processing mixed stream types through unified interface...\n")

        for stream, batch in self.processors:
            stream.process_batch(batch)

            if isinstance(stream, SensorStream):
                self.sensor_readings += stream.batch_count
                if stream.high_prio_count > 0:
                    self.critical_sensor_alerts += 1

            elif isinstance(stream, TransactionStream):
                self.transaction_ops += stream.count
                for item in batch:
                    if "2000" in item:
                        self.large_transactions += 1

            elif isinstance(stream, EventStream):
                self.event_count += stream.batch_count

        self.report()

    def report(self) -> None:
        print("Batch 1 Results:")
        print(f"- Sensor data: {self.sensor_readings} readings processed")
        print(f"- Transaction data: {self.transaction_ops} operations \
processed")
        print(f"- Event data: {self.event_count} events processed\n")

        print("Stream filtering active: High-priority data only")
        print(f"Filtered results: {self.critical_sensor_alerts} critical \
sensor alerts,"
              f" {self.large_transactions} large transaction\n")

        print("All streams processed successfully. Nexus throughput optimal.")


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

processor = StreamProcessor()

processor.add_stream(SensorStream("SENSON__002"), ["temperature:22.5", "High"])
processor.add_stream(SensorStream("SENSON__003"), ["temperature:22.5", "High"])
processor.add_stream(TransactionStream("TRANS__002"),
                     ["buy:100", "sell:150", "buy:75", "buy:2000"])
processor.add_stream(EventStream("EVENT__002"),
                     ["login", "error", "logout"])

processor.process()
