from typing import Any
import json
import os


class Config:
    settings: dict[str, Any] = {}
    configpath = "config/config.json"
    localconfigpath = "config/config.local.json"

    def __init__(self) -> None:
        folder = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(folder, self.configpath)) as f:
            self.settings = json.load(f)

        try:
            with open(os.path.join(folder, self.localconfigpath)) as f:
                self.settings = {**self.settings, **json.load(f)}
        except FileNotFoundError:
            # local config is not required
            pass

    def __getitem__(self, key: Any) -> Any:
        return self.settings[key]
