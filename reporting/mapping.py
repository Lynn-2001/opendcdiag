"""
OpenDCDiag CPU Output → OCP Diagnostic Format Converter
Supports both YAML and JSON input.

Usage:
    python opendcdiag_to_ocp.py input.yaml output.jsonl
    python opendcdiag_to_ocp.py input.json output.jsonl
"""

import json
import argparse
from datetime import datetime, timezone
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None



def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ts_to_iso(ts) -> str:
    """Convert opendcdiag timestamp field (.now) to ISO-8601 string."""
    if ts is None:
        return now_iso()
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.isoformat()
    if isinstance(ts, str):
        return ts
    return now_iso()


def extract_timestamp(test: dict, field: str) -> str:
    """Extract .now from time-at-start or time-at-end, fall back to now_iso."""
    block = test.get(field, {}) or {}
    return ts_to_iso(block.get("now"))


def result_to_ocp(result: str) -> str:
    return {
        "pass":                   "PASS",
        "fail":                   "FAIL",
        "skip":                   "NOT_APPLICABLE",
        "timed out":              "FAIL",
        "interrupted":            "FAIL",
        "operating system error": "FAIL",
        "crash":                  "FAIL",
    }.get(result, "NOT_APPLICABLE")


def exit_to_ocp_status(exit_val: str) -> str:
    return {
        "pass":        "COMPLETE",
        "fail":        "COMPLETE",
        "invalid":     "ERROR",
        "interrupted": "SKIP",
    }.get(exit_val, "ERROR")


def log_level_to_ocp(level: str) -> str:
    return {
        "debug":   "DEBUG",
        "info":    "INFO",
        "warning": "WARNING",
        "error":   "ERROR",
        "skip":    "INFO",
    }.get(level, "INFO")


def hw_id(cpu: dict) -> str:
    return (f"cpu-pkg{cpu.get('package', 0)}"
            f"-core{cpu.get('core', 0)}"
            f"-logical{cpu.get('logical', 0)}")



def convert(data: dict) -> list[dict]:
    artifacts = []
    seq = 0

    def emit(payload: dict, timestamp: str = None):
        nonlocal seq
        artifacts.append({
            "sequenceNumber": seq,
            "timestamp": timestamp or now_iso(),
            **payload
        })
        seq += 1

    # Schema version
    emit({"schemaVersion": {"major": 2, "minor": 0}})

    # dutInfo 
    cpu_infos = data.get("cpu-info", [])
    hardware_infos = []
    for cpu in cpu_infos:
        hw = {
            "hardwareInfoId": hw_id(cpu),
            "name": (f"CPU Package {cpu.get('package', 0)} "
                     f"Core {cpu.get('core', 0)} "
                     f"(logical {cpu.get('logical', 0)})"),
            "location": (f"package={cpu.get('package','?')} "
                         f"numa={cpu.get('numa_node','?')} "
                         f"core={cpu.get('core','?')} "
                         f"thread={cpu.get('thread','?')}"),
        }
        if cpu.get("ppin"):
            hw["serialNumber"] = str(cpu["ppin"])
        if cpu.get("family") is not None:
            hw["version"] = (f"family={cpu.get('family')} "
                             f"model={hex(cpu.get('model', 0))} "
                             f"stepping={cpu.get('stepping')}")
        if cpu.get("microcode") is not None:
            hw["revision"] = (hex(cpu["microcode"])
                              if isinstance(cpu["microcode"], int)
                              else str(cpu["microcode"]))
        hardware_infos.append(hw)

    platform_infos = [{"info": data.get("runtime", "unknown")}]
    virt = data.get("virtualization-state") or {}
    if virt:
        parts = [f"{k}={v}" for k, v in virt.items() if v and v != "none"]
        if parts:
            platform_infos.append({"info": "virtualization: " + ", ".join(parts)})

    software_infos = [{
        "softwareInfoId": "opendcdiag",
        "name": "opendcdiag",
        "version": data.get("version", "unknown"),
        "softwareType": "APPLICATION",
    }]
    openssl = data.get("openssl")
    if openssl and isinstance(openssl, dict):
        software_infos.append({
            "softwareInfoId": "openssl",
            "name": "openssl",
            "version": openssl.get("version", "unknown"),
            "softwareType": "APPLICATION",
        })

    dut_info = {
        "dutInfoId": "dut0",
        "name": data.get("os", "unknown"),
        "platformInfos": platform_infos,
        "softwareInfos": software_infos,
        "hardwareInfos": hardware_infos,
    }

    # testRunStart 
    timing = data.get("timing", {})
    emit({
        "testRunArtifact": {
            "testRunStart": {
                "name": "opendcdiag-cpu",
                "version": data.get("version", "unknown"),
                "commandLine": data.get("command-line", ""),
                "parameters": {
                    "timeout_ms": timing.get("timeout"),
                    "duration_ms": timing.get("duration"),
                },
                "dutInfo": dut_info,
            }
        }
    })

    # One testStep per test 
    all_tests = data.get("tests", [])
    for step_idx, test in enumerate(all_tests):
        test_name = test.get("test", f"test_{step_idx}")
        step_id   = str(step_idx)
        result    = test.get("result", "skip")
        details   = test.get("details", {}) or {}
        state     = test.get("state", {}) or {}

        ts_start = extract_timestamp(test, "time-at-start")
        ts_end   = extract_timestamp(test, "time-at-end")

        t_start_block = test.get("time-at-start", {}) or {}
        t_end_block   = test.get("time-at-end", {})   or {}

        # testStepStart — timestamp = test's actual start time
        emit({
            "testStepArtifact": {
                "testStepId": step_id,
                "testStepStart": {"name": test_name},
            }
        }, timestamp=ts_start)

        # Structured metadata
        fail_info = test.get("fail") or {}
        emit({
            "testStepArtifact": {
                "testStepId": step_id,
                "extension": {
                    "name": "opendcdiag-metadata",
                    "content": {
                        "iteration":      state.get("iteration"),
                        "seed":           state.get("seed"),
                        "retry":          state.get("retry"),
                        "quality":        details.get("quality"),
                        "description":    details.get("description"),
                        "elapsed_start":  t_start_block.get("elapsed"),
                        "elapsed_end":    t_end_block.get("elapsed"),
                        "fail_seed":      fail_info.get("seed"),
                        "time_to_fail":   fail_info.get("time-to-fail"),
                    },
                },
            }
        }, timestamp=ts_start)

        # Runtime measurement — timestamp = test's end time
        emit({
            "testStepArtifact": {
                "testStepId": step_id,
                "measurement": {
                    "name": "test-runtime-ms",
                    "value": test.get("test-runtime", 0),
                    "unit": "ms",
                    "validators": [{"type": "GREATER_THAN_OR_EQUAL", "value": 0}],
                },
            }
        }, timestamp=ts_end)

        # Skip reason
        if result == "skip" and test.get("skip-reason"):
            emit({
                "testStepArtifact": {
                    "testStepId": step_id,
                    "log": {
                        "severity": "INFO",
                        "message": (f"SKIP [{test.get('skip-category', '?')}]: "
                                    f"{test['skip-reason']}"),
                    },
                }
            }, timestamp=ts_start)

        # Per-thread messages
        for thread_info in test.get("threads", []):
            thread_id  = thread_info.get("thread", "?")
            cpu_id     = thread_info.get("id") or {}
            prefix     = (f"[thread={thread_id} "
                          f"pkg={cpu_id.get('package','?')} "
                          f"core={cpu_id.get('core','?')}]")

            # thread-level extension: thread_id, loop_count, time_to_fail, freq_mhz
            thread_ext_content = {
                "thread_id":    thread_id,
                "loop_count":   thread_info.get("loop-count"),
                "time_to_fail": thread_info.get("time-to-fail"),
                "freq_mhz":     thread_info.get("freq_mhz"),
                "thread_state": thread_info.get("state"),
            }
            # only emit if at least one interesting field is non-null
            if any(v is not None for v in thread_ext_content.values()):
                thread_hw = hw_id(cpu_id) if cpu_id else None
                ext_content = {"hardwareInfoId": thread_hw, **thread_ext_content} if thread_hw else thread_ext_content
                emit({
                    "testStepArtifact": {
                        "testStepId": step_id,
                        "extension": {
                            "name": "opendcdiag-thread",
                            "content": ext_content,
                        },
                    }
                }, timestamp=ts_start)

            if thread_info.get("freq_mhz") is not None:
                meas = {
                    "name": "cpu-freq-mhz",
                    "value": thread_info["freq_mhz"],
                    "unit": "MHz",
                }
                if cpu_id:
                    meas["hardwareInfoId"] = hw_id(cpu_id)
                emit({"testStepArtifact": {"testStepId": step_id, "measurement": meas}},
                     timestamp=ts_start)

            for msg in (thread_info.get("messages") or []):
                level = msg.get("level", "info")
                text  = msg.get("text", "")

                if "data-miscompare" in msg:
                    mc = msg["data-miscompare"]
                    offset = mc.get("offset")
                    offset_str = f" offset={offset}" if offset is not None else ""
                    diag = {
                        "verdict": f"{test_name}-miscompare",
                        "type": "FAIL",
                        "message": (
                            f"{mc.get('description','')} | "
                            f"type={mc.get('type','')} "
                            f"expected={mc.get('expected','')} "
                            f"actual={mc.get('actual','')} "
                            f"mask={mc.get('mask','')}"
                            f"{offset_str} "
                            f"addr={mc.get('address','')}"
                        ),
                    }
                    if cpu_id:
                        diag["hardwareInfoId"] = hw_id(cpu_id)
                    emit({"testStepArtifact": {"testStepId": step_id, "diagnosis": diag}},
                         timestamp=ts_start)
                else:
                    emit({
                        "testStepArtifact": {
                            "testStepId": step_id,
                            "log": {
                                "severity": log_level_to_ocp(level),
                                "message": f"{prefix} {text}".strip(),
                            },
                        }
                    }, timestamp=ts_start)

        # stderr
        if test.get("stderr messages"):
            emit({
                "testStepArtifact": {
                    "testStepId": step_id,
                    "log": {
                        "severity": "WARNING",
                        "message": f"[stderr] {test['stderr messages']}",
                    },
                }
            }, timestamp=ts_end)

        # result-details (crash)
        rd = test.get("result-details") or {}
        if rd:
            emit({
                "testStepArtifact": {
                    "testStepId": step_id,
                    "log": {
                        "severity": "ERROR" if rd.get("crashed") else "INFO",
                        "message": (f"result-details: crashed={rd.get('crashed')} "
                                    f"core-dump={rd.get('core-dump')} "
                                    f"code={rd.get('code')} "
                                    f"reason={rd.get('reason','')}"),
                    },
                }
            }, timestamp=ts_end)

        # Overall diagnosis
        if result in ("pass", "fail"):
            message = details.get("description", test_name)
            if result == "fail":
                if fail_info.get("cpu-mask"):
                    message += f" | cpu-mask={fail_info['cpu-mask']}"
                if fail_info.get("seed"):
                    message += f" | fail_seed={fail_info['seed']}"
            emit({
                "testStepArtifact": {
                    "testStepId": step_id,
                    "diagnosis": {
                        "verdict": f"{test_name}-result",
                        "type": "PASS" if result == "pass" else "FAIL",
                        "message": message,
                    },
                }
            }, timestamp=ts_end)

        # testStepEnd
        emit({
            "testStepArtifact": {
                "testStepId": step_id,
                "testStepEnd": {
                    "status": "COMPLETE" if result not in ("skip", "interrupted") else "SKIP"
                },
            }
        }, timestamp=ts_end)

    # testRunEnd 
    exit_val = data.get("exit", "invalid")
    emit({
        "testRunArtifact": {
            "testRunEnd": {
                "status": exit_to_ocp_status(exit_val),
                "result": result_to_ocp(exit_val),
            }
        }
    })

    return artifacts


# Validation summary 

def validate(artifacts: list[dict], expected_tests: int):
    step_artifacts = [a for a in artifacts if "testStepArtifact" in a]
    step_ids = set(a["testStepArtifact"]["testStepId"] for a in step_artifacts)
    has_run_start = any("testRunArtifact" in a and "testRunStart" in a["testRunArtifact"]
                        for a in artifacts)
    has_run_end   = any("testRunArtifact" in a and "testRunEnd"   in a["testRunArtifact"]
                        for a in artifacts)

    print(f"Artifacts:       {len(artifacts)}")
    print(f"Run Start/End:   [{'PASS' if has_run_start else 'FAIL'}] / [{'PASS' if has_run_end else 'FAIL'}]")
    print(f"Steps Integrity: {len(step_ids)}/{expected_tests} {'(OK)' if len(step_ids) == expected_tests else '(MISMATCH)'}")
    print(f"Step Artifacts:  {len(step_artifacts)}")



def load_input(path: str) -> dict:
    suffix = Path(path).suffix.lower()
    with open(path, "r", encoding="utf-8") as f:
        if suffix in (".yaml", ".yml"):
            if yaml is None:
                raise RuntimeError("PyYAML not installed: pip install pyyaml")
            return yaml.safe_load(f)
        else:
            return json.load(f)


def main():
    parser = argparse.ArgumentParser(
        description="Convert OpenDCDiag CPU output (YAML or JSON) to OCP JSONL format"
    )
    parser.add_argument("input",  help="Input file (.yaml or .json)")
    parser.add_argument("output", help="Output OCP JSONL file")
    args = parser.parse_args()

    data = load_input(args.input)
    artifacts = convert(data)

    with open(args.output, "w", encoding="utf-8") as f:
        for a in artifacts:
            f.write(json.dumps(a, ensure_ascii=False, default=str) + "\n")

    expected = len(data.get("tests", []))
    print(f"[success]  Done {args.output}")
    validate(artifacts, expected)


if __name__ == "__main__":
    main()
