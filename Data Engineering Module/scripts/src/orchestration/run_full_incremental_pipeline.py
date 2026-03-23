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
        ("Collect Search Results to JSON", "src.ingestion.collect_search_results_to_json"),
        ("Load Search JSON to RAW", "src.ingestion.load_raw_search_manual"),
        ("Parse Search RAW to STAGE", "src.parsing.parse_raw_to_stage"),
        ("Load Search STAGE to DWH", "src.dwh_loading.load_stage_to_dwh"),
        ("Detect Search Brand Mentions", "src.enrichment.detect_brand_mentions"),

        ("Generate AI Answers from Search Results", "src.ingestion.generate_ai_from_search_results"),
        ("Load AI JSON to RAW", "src.ingestion.load_raw_ai_manual"),
        ("Parse AI RAW to STAGE", "src.parsing.parse_raw_ai_to_stage"),
        ("Load AI STAGE to DWH", "src.dwh_loading.load_ai_stage_to_dwh"),
        ("Detect AI Brand Mentions", "src.enrichment.detect_ai_brand_mentions"),
    ]

    for step_name, module_name in steps:
        run_step(step_name, module_name, project_root)

    print("\nFULL INCREMENTAL PIPELINE COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    main()