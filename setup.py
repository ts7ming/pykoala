from setuptools import setup, find_packages

setup(
    name='PyKoala',
    version='2.3.9',
    url='http://10.1.0.1:3000/Zoo/PyKoala',
    description='PyKoala makes programming easier',
    author='DataDev',
    author_email='report@qyt1902.com',
    packages=find_packages(),
    install_requires=[
        'importlib-metadata',
        'loguru',
        'numpy',
        'pandas',
        'PyMySQL',
        'requests',
        'socket.engine',
        'SQLAlchemy',
        'clickhouse_sqlalchemy',
        'xlrd==1.2.0',
        'XlsxWriter',
        'memory_profiler'
    ],
)
