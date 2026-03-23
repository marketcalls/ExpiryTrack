"""Safe execution sandbox for user strategy code."""

from __future__ import annotations

import math
import re

from .strategy import Strategy

# Patterns that are not allowed in strategy code
BLOCKED_PATTERNS = [
    r'\bimport\b',
    r'__import__',
    r'\bexec\s*\(',
    r'\beval\s*\(',
    r'\bopen\s*\(',
    r'\bos\.',
    r'\bsys\.',
    r'\bsubprocess\b',
    r'__subclasses__',
    r'__globals__',
    r'\bgetattr\s*\(',
    r'\bsetattr\s*\(',
    r'\bdelattr\s*\(',
    r'\bcompile\s*\(',
    r'__builtins__',
    r'__class__',
    r'__bases__',
    r'__mro__',
]

# Safe builtins whitelist
SAFE_BUILTINS = {
    "__build_class__": __builtins__["__build_class__"] if isinstance(__builtins__, dict) else getattr(__builtins__, "__build_class__"),
    "__name__": "__main__",
    "abs": abs,
    "min": min,
    "max": max,
    "sum": sum,
    "len": len,
    "range": range,
    "round": round,
    "int": int,
    "float": float,
    "bool": bool,
    "str": str,
    "list": list,
    "dict": dict,
    "tuple": tuple,
    "enumerate": enumerate,
    "zip": zip,
    "sorted": sorted,
    "reversed": reversed,
    "any": any,
    "all": all,
    "isinstance": isinstance,
    "issubclass": issubclass,
    "object": object,
    "property": property,
    "staticmethod": staticmethod,
    "classmethod": classmethod,
    "super": super,
    "print": lambda *a, **kw: None,  # no-op print
    "True": True,
    "False": False,
    "None": None,
}


class SandboxError(Exception):
    """Raised when strategy code fails validation."""


def validate_code(code: str) -> list[str]:
    """Static analysis — returns list of violations found."""
    violations = []
    for pattern in BLOCKED_PATTERNS:
        matches = re.findall(pattern, code)
        if matches:
            violations.append(f"Blocked pattern found: {matches[0]}")
    return violations


def compile_strategy(code: str) -> Strategy:
    """Validate, compile, and instantiate a Strategy subclass from user code.

    Returns an instance of the user's Strategy subclass.
    Raises SandboxError if code is invalid or doesn't define a Strategy subclass.
    """
    # Step 1: Static validation
    violations = validate_code(code)
    if violations:
        raise SandboxError(
            "Code validation failed:\n" + "\n".join(f"  - {v}" for v in violations)
        )

    # Step 2: Prepare restricted globals
    restricted_globals: dict = {
        "__builtins__": SAFE_BUILTINS,
        "math": math,
        "Strategy": Strategy,
    }

    # Step 3: Execute in restricted namespace
    try:
        exec(compile(code, "<strategy>", "exec"), restricted_globals)  # noqa: S102
    except SyntaxError as e:
        raise SandboxError(f"Syntax error in strategy code: {e}") from e
    except Exception as e:
        raise SandboxError(f"Error executing strategy code: {e}") from e

    # Step 4: Find the user's Strategy subclass
    strategy_cls = None
    for name, obj in restricted_globals.items():
        if (
            name.startswith("_")
            or obj is Strategy
            or not isinstance(obj, type)
        ):
            continue
        if issubclass(obj, Strategy):
            strategy_cls = obj
            break

    if strategy_cls is None:
        raise SandboxError(
            "No Strategy subclass found. Your code must define a class "
            "that extends Strategy."
        )

    # Step 5: Instantiate
    try:
        instance = strategy_cls()
    except Exception as e:
        raise SandboxError(f"Error instantiating strategy: {e}") from e

    return instance
