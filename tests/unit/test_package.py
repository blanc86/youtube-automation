import ytauto


def test_package_exposes_version() -> None:
    assert ytauto.__version__ == "0.1.0"


def test_running_on_python_312_or_newer() -> None:
    import sys

    assert sys.version_info >= (3, 12), f"expected >=3.12, got {sys.version_info}"
