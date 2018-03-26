from setuptools import setup

setup(
    name="StandardizedReleaseAndWasteInventories",
    version="0.1",
    author="Wesley Ingwersen",
    author_email="ingwersen.wesley@epa.gov",
    description="Standardized Release And Waste Inventories provides processed EPA release and emissions inventories "
                "in standard tabular format",
    license="GPL 3.0",
    keywords="USEPA data",
    url="http://www.github.com/usepa/lci-primer",
    packages=['StandardizedReleaseAndWasteInventories'],
    package_data={'StandardizedReleaseAndWasteInventories': ["data/*.*","output/*.*"]},
    install_requires=['numpy', 'pandas', 'requests'],
    long_description=open('README.md').read(),
    classifiers=[
        "Development Status :: Alpha",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "License :: GPL 3.0",
        "Programming Language :: Python :: 3.x",
        "Topic :: Utilities",
    ]
)
