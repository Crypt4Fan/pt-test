from setuptools import setup


setup(
    name='worker',
    version='0.1',
    py_modules = ['worker'],
    python_requires = '>=3.7',
    entry_points = {
        'console_scripts': [
            'worker=worker:main',
        ],
    },
    install_requires = [
        'aiohttp==3.6.2',
        'aio-pika==4.8.0',
    ],
)