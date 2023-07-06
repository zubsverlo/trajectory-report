from setuptools import setup, find_packages


setup(
    name='trajectory_report',
    version='1.0',
    author='Arney',
    author_email='zub.sverlo@gmail.com',
    packages=find_packages(),
    install_requires=[
        'SQLAlchemy==2.0.12',
        'folium==0.12.1.post1',
        'mysql-connector-python==8.0.33',
        'numpy',
        'pandas==1.5.3',
        'scikit-mobility==1.3.1',
        'xlsxwriter==3.1.0',
        'aiohttp==3.8.4',
        'python-dotenv'
    ],
)
