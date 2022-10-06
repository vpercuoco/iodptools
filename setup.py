from setuptools import setup, find_packages

setup(
    name='iodptools',
    version='0.1.0',    
    description='A Python package containing common tools used with IODP data',
    url='https://github.com/vpercuoco/iodptools',
    author='Vincent Percuoco',
    author_email='percuoco@iodp.tamu.edu',
    license='MIT',
    packages=find_packages(),
    install_requires=['pandas',
                      'pandera',
                      'numpy',                     
                      ],
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',  
        'Programming Language :: Python :: 3.8',
    ],
    )
