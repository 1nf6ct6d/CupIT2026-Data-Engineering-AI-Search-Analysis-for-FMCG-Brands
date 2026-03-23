import subprocess
import sys
from pathlib import Path


def run_step(step_name: str, module_name: str, project_root: Path):
    print(f"\n--- START: {step_name} ---")

    result = subprocess.run(
        [sys.executable, "-m", module_name],
        capture_output=True,
        text=True,
        cwd=project_root
    )

    if result.stdout:
        print(result.stdout)

    if result.returncode != 0:
        print(f"--- ERROR IN STEP: {step_name} ---")
        if result.stderr:
            print(result.stderr)
        raise RuntimeError(f"Pipeline failed on step: {step_name}")

    print(f"--- DONE: {step_name} ---")


def main():
    project_root = Path(__file__).resolve().parents[2]

    steps = [
        ("Parse RAW AI to STAGE", "src.parsing.parse_raw_ai_to_stage"),
        ("Load AI STAGE to DWH", "src.dwh_loading.load_ai_stage_to_dwh"),
        ("Detect AI Brand Mentions", "src.enrichment.detect_ai_brand_mentions"),
    ]

    for step_name, module_name in steps:
        run_step(step_name, module_name, project_root)

    print("\nAI pipeline completed successfully.")


if __name__ == "__main__":
    main()