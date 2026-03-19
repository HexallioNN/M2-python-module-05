from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return (f"Output: {result}")


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, list):
            return False
        for x in data:
            if not isinstance(x, (int, float)):
                return False
        return True

    def process(self, data: Any) -> str:
        if not self.validate(data):
            raise ValueError("Numeric data not verified")
        count = 0
        total = 0
        for x in data:
            count += 1
            total = total + x
        avg = total / count
        result = f"Processed {count} numeric values, sum={total}, avg={avg}"
        return result


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        else:
            return False

    def process(self, data: Any) -> str:
        if not self.validate(data):
            raise ValueError("Text data not verified")
        length = len(data)
        words = len(data.split())
        result = f"Processed text: {length} characters, {words} words"
        return result


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        return isinstance(data, str) and ":" in data

    def process(self, data: Any) -> str:
        if not self.validate(data):
            raise ValueError("Log entry not verified")

        level_part, message_part = data.split(":", 1)

        self.level = level_part.strip().upper()
        message = message_part.strip()

        return f"{self.level} level detected: {message}"

    def format_output(self, result: str) -> str:
        if self.level == "ERROR":
            prefix = "ALERT"
        else:
            prefix = "INFO"

        return f"[{prefix}] {result}"


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    numeric = [1, 2, 3, 4, 5]
    string = "Hello Nexus World"
    log = "ERROR: Connection timeout"

    numeric_data = NumericProcessor()
    print("Initializing Numeric Processor...")
    print(f"Processing data: {numeric}")
    if numeric_data.validate(numeric):
        print("Validation: Numeric data verified")
    print(numeric_data.format_output(numeric_data.process(numeric)))
    print()

    text_data = TextProcessor()
    print("Initializing Text Processor...")
    print(f'Processing data: "{string}"')
    if text_data.validate(string):
        print("Validation: Text data verified")
    print(text_data.format_output(text_data.process(string)))
    print()

    log_data = LogProcessor()
    print("Initializing Log Processor...")
    print(f'Processing data: "{log}"')
    if log_data.validate(log):
        print("Validation: Log entry verified")
    print(log_data.format_output(log_data.process(log)))
    print()

    print("=== Polymorphic Processing Demo ===\n")
    print("Processing multiple data types through same interface...")

    processors = [
        (NumericProcessor(), [1, 2, 3]),
        (TextProcessor(), "Hello World!"),
        (LogProcessor(), "INFO: System ready")
    ]

    for i, (processor, data) in enumerate(processors, start=1):
        result = processor.process(data)
        output = processor.format_output(result)
        print(f"Result {i}: {output}")

    print("Foundation systems online. Nexus ready for advanced streams.")
