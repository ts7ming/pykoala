from setuptools import setup, find_packages

setup(
    name='PyKoala',
    version='1.0.7',
    url='https://github.com/ts7ming/pykoala.git',
    description='PyKoala makes programming easier',
    author='7ming',
    author_email='qiming.ma@outlook.com',
    packages=find_packages(),
    install_requires=[
        'importlib-metadata',
        'numpy',
        'pandas',
        'PyMySQL',
        'requests',
        'socket.engine',
        'SQLAlchemy==1.4.49',
        'xlrd==1.2.0',
        'XlsxWriter'
    ],
)
