# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
from datetime import date

import ctapipe_io_zfits

project = "ctapipe_io_zfits"
copyright = f"{date.today().year}, CTAO"
author = "Maximilian Linhoff"
version = ctapipe_io_zfits.__version__
release = version


# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx.ext.githubpages",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "numpydoc",
]

templates_path = []
exclude_patterns = []

# have all links automatically associated with the right domain.
default_role = "py:obj"

# intersphinx allows referencing other packages sphinx docs
intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "traitlets": ("https://traitlets.readthedocs.io/en/stable/", None),
    "ctapipe": ("https://ctapipe.readthedocs.io/en/v0.24.0/", None),
}

# -- Options for HTML output -------------------------------------------------
html_theme = "ctao"
html_theme_options = {
    "navigation_with_keys": False,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_logo = "_static/cta.png"

# fixes "no sub file found" for members inherited from traitlets
numpydoc_class_members_toctree = False


nitpick_ignore = [
    ("py:obj", "is_stream"),
    ("py:obj", "obs_ids"),
    ("py:obj", "simulation_config"),
    ("py:obj", "simulated_shower_distributions"),
    ("py:obj", "cross_validation_lock"),
    ("py:obj", "atmosphere_density_profile"),
]
nitpick_ignore_regex = [
    ("py:obj", r".*ProtozfitsDL0.*EventSource\..*"),
]
