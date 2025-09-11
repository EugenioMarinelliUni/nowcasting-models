import os
import yaml

def load_config(config_path: str = "config.yaml") -> dict:
    """
    Load the YAML configuration file as a Python dictionary.

    Args:
        config_path (str): Relative path to the YAML config file.

    Returns:
        dict: Parsed configuration.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    return config
