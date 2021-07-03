from dataclasses import dataclass
from typing import Union


@dataclass
class MetricRecord:
    min_value: Union[float, int]
    max_value: Union[float, int]
