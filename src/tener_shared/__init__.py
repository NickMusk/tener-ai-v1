"""Shared runtime helpers for multi-instance deployment."""

from .instance_config import InstanceConfig, load_instance_config

__all__ = ["InstanceConfig", "load_instance_config"]
