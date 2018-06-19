import setuptools


setuptools.setup(
    name="Adwords Report Downloader",
    author="Adam Cunnington",
    author_email="adam.cunnington@wmglobal.com",
    license="MIT",
    packages=setuptools.find_packages(),
    package_data={"": ["googleads.yaml"]},
    install_requires=["click", "googleads"],
    entry_points={"console_scripts": ["adwordsreportdownloader = adwordsreportdownloader:main"]}
)
