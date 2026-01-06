"""Configuration management for ewb-paper."""
from dataclasses import dataclass
from pathlib import Path
import os
import sys

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


@dataclass
class Config:
    """Configuration for ewb-paper."""

    basepath: Path
    saved_data_path: Path
    parallel_backend: str = "loky"
    parallel_jobs: int = 32

    @classmethod
    def from_file(cls, config_path: Path | None = None) -> "Config":
        """Load configuration from TOML file.

        Args:
            config_path: Path to config.toml. If None, looks in current directory
                         and ~/.config/ewb-paper/config.toml

        Returns:
            Config object with loaded settings
        """
        if config_path is None:
            # Try current directory first
            if Path("config.toml").exists():
                config_path = Path("config.toml")
            # Then user config directory
            elif Path.home().joinpath(".config/ewb-paper/config.toml").exists():
                config_path = Path.home() / ".config/ewb-paper/config.toml"
            else:
                # No config file, use defaults
                return cls.default()

        with open(config_path, "rb") as f:
            config_data = tomllib.load(f)

        # Parse paths section
        basepath = Path(config_data.get("paths", {}).get("basepath",
                                                          Path.home() / "extreme-weather-bench-paper"))
        basepath = basepath.expanduser()

        # Parse parallel section
        parallel_config = config_data.get("parallel", {})
        parallel_backend = parallel_config.get("backend", "loky")
        parallel_jobs = parallel_config.get("n_jobs", 32)

        return cls(
            basepath=basepath,
            saved_data_path=basepath / "saved_data",
            parallel_backend=parallel_backend,
            parallel_jobs=parallel_jobs,
        )

    @classmethod
    def default(cls) -> "Config":
        """Create default configuration.

        Checks environment variable EWB_PAPER_BASEPATH, otherwise uses
        ~/extreme-weather-bench-paper
        """
        basepath_str = os.getenv("EWB_PAPER_BASEPATH")
        if basepath_str:
            basepath = Path(basepath_str).expanduser()
        else:
            basepath = Path.home() / "extreme-weather-bench-paper"

        return cls(
            basepath=basepath,
            saved_data_path=basepath / "saved_data",
        )

    def get_parallel_config(self) -> dict:
        """Return parallel configuration dict for joblib."""
        return {
            "backend": self.parallel_backend,
            "n_jobs": self.parallel_jobs,
        }
