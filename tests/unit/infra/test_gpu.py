import shutil

from ytauto.infra.gpu import GpuInfo, detect, parse_nvidia_smi

# Captured verbatim from the target machine.
SMI_OUTPUT = "NVIDIA GeForce RTX 3050 Laptop GPU, 4096 MiB, 592.82\n"


def test_parses_name_vram_and_driver() -> None:
    info = parse_nvidia_smi(SMI_OUTPUT)
    assert info == GpuInfo(name="NVIDIA GeForce RTX 3050 Laptop GPU", vram_mb=4096, driver="592.82")


def test_parses_vram_without_a_unit_suffix() -> None:
    info = parse_nvidia_smi("NVIDIA A100, 40960, 550.54\n")
    assert info is not None
    assert info.vram_mb == 40960


def test_uses_the_first_gpu_when_several_are_listed() -> None:
    info = parse_nvidia_smi("GPU One, 4096 MiB, 592.82\nGPU Two, 8192 MiB, 592.82\n")
    assert info is not None
    assert info.name == "GPU One"


def test_empty_output_yields_none() -> None:
    assert parse_nvidia_smi("") is None
    assert parse_nvidia_smi("   \n  ") is None


def test_malformed_output_yields_none_rather_than_raising() -> None:
    assert parse_nvidia_smi("something went wrong") is None
    assert parse_nvidia_smi("GPU, not-a-number, 1.0") is None


def test_detect_agrees_with_whether_nvidia_smi_is_installed() -> None:
    """Smoke-test the real subprocess path with a non-vacuous assertion."""
    result = detect()

    if result is None:
        assert shutil.which("nvidia-smi") is None, (
            "detect() returned None even though nvidia-smi is on PATH"
        )
    else:
        assert result.vram_mb > 0
        assert result.driver, "driver string must not be empty"
        assert result.name, "GPU name must not be empty"
