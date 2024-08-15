"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documenation builds.
"""

from documenteer.conf.guide import *  # noqa: F403,F401

autodoc_pydantic_model_show_config_summary = False
autodoc_pydantic_settings_show_config_summary = False
autodoc_pydantic_settings_show_json = False
autodoc_pydantic_model_show_json = False

exclude_patterns = ["changes/*"]
