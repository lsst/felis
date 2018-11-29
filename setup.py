from setuptools import setup, find_packages

requires = [
    'sqlalchemy>1.1',
    'pyyaml',
    'click'
]

doc_requires = [
    "sphinx",
    "sphinx_click",
    "sphinx_rtd_theme"
]

setup(
    name='felis',
    use_scm_version={'version_scheme': 'post-release'},
    setup_requires=['setuptools_scm'],
    packages=find_packages(),
    url='https://github.com/lsst-dm/felis',
    license='GPL',
    author='Brian Van Klaveren',
    author_email='bvan@slac.stanford.edu',
    description='A vocabulary for describing catalogs and '
                'acting on those descriptions',
    install_requires=requires,
    entry_points={
        'console_scripts': [
            'felis = felis:cli',
        ],
    },
    extras_require={'docs': doc_requires}
)
