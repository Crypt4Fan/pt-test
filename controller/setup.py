from setuptools import setup


setup(
    name='controller',
    version='0.1',
    py_modules = ['controller'],
    python_requires = '>=3.7',
    entry_points = {
        'console_scripts': [
            'controller=controller:main',
        ],
    },
    install_requires = [
        'aiohttp==3.6.2',
        'aio-pika==4.8.0',
        'databases[postgresql]'
    ],
)