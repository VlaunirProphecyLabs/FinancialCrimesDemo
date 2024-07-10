from setuptools import setup, find_packages
setup(
    name = 'pipe_financial_crimes',
    version = '1.0',
    packages = find_packages(include = ('pipe_financial_crimes*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.7'],
    entry_points = {
'console_scripts' : [
'main = pipe_financial_crimes.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
