import setuptools

with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setuptools.setup(
    name='pyctor',
    packages=['pyctor'],
    version='0.0.2',
    license='MIT',
    description='Minimalistic implementation of the actor concurrency model powered by asyncio.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Kuba Weimann',
    author_email='kuba.weimann@gmail.com',
    url='https://github.com/kweimann/pyctor',
    python_requires='>=3.7.3',
)
