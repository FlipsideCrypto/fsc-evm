"""
FSC-EVM Configuration Management System

This package handles centralized configuration for all EVM chain-specific projects.
It provides a consistent way to define, calculate, and access configuration parameters
across multiple blockchain projects.
"""

from analysis.config.config_manager import get_instance, get_config, get_children

__all__ = ['get_instance', 'get_config', 'get_children']