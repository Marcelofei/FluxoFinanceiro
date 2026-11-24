"""Load legacy app functions without starting Streamlit or opening production DB."""
import ast
from pathlib import Path


def app_functions(names, namespace):
    tree=ast.parse(Path('app.py').read_text())
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
    for n in nodes: n.decorator_list=[]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'app-functions','exec'),namespace)
    return namespace
