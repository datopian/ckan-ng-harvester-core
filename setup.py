import setuptools

print(setuptools.find_packages())

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
     name='ckan-harvesters',
     version='0.122',
     license='MIT',
     author="Datopian",
     author_email="contact@datopian.com",
     description="Harvester Next Generation Core for CKAN",
     long_description=long_description,
     long_description_content_type="text/markdown",
     url="https://gitlab.com/datopian/ckan-ng-harvester-core",
     install_requires=[
        'python-slugify>=3.0.0',
        'requests>=2.20.0',
        'OWSLib>=0.18.0',
        'datapackage>=1.6.2',
        'jsonschema>=3.2.0',
        'rfc3987>=1.3.8',
        'validate_email>=1.3',
        'Jinja2>=2.10.1',
        'pathlib>=1.0.1',
        'importlib-resources>=1.0.2',
        'lxml>=4.4.1'
     ],
     include_package_data=True,
     packages=setuptools.find_packages(exclude=("tests", "tests_with_ckan")),
     keywords=['harvester', 'CKAN'],
     classifiers=[
         'Programming Language :: Python :: 3',
         'License :: OSI Approved :: MIT License',
         'Operating System :: OS Independent',
         'Intended Audience :: Developers',
     ],
     python_requires='>=3.6',
 )