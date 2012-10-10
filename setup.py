from setuptools import setup, find_packages

version = '0.0'

setup(name='TheCutOut',
      version=version,
      description="Data storage and synchronization system",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Ian Bicking',
      author_email='ian@ianbicking.org',
      url='http://thecutout.org',
      license='MIT',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
        'WebOb',
      ],
      )
