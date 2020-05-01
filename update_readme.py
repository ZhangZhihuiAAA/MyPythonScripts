"""
Update README.md.
"""

import os
import sys
import importlib

repo_name = os.path.basename(os.getcwd())

files = [f for f in os.listdir('.') if os.path.isfile(f) and f.endswith('.py')]
modules = [f.split('.')[0] for f in files if f != 'update_readme.py']
modules.sort()

w = open('README.md', 'w')
print('# {}'.format(repo_name), file=w)
print('\n', file=w)
fmt_str = '<pre>{:30s}{}</pre>'
print(fmt_str.format('Script Name', 'Summary' ), file=w)
print('-' * 100, file=w)
for m in modules:
    m_imp = importlib.import_module(m)
    print(fmt_str.format(m + '.py', m_imp.__doc__), file=w)
print('-' * 100, file=w)
w.close()
