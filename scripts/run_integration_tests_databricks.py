import argparse
import os
import subprocess
import sys


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run integration tests in Databricks without writing bytecode cache to /Workspace."
    )
    parser.add_argument(
        "--repo",
        default="/Workspace/Users/vishnusuresh87@gmail.com/spark-ecommerce-recommendation",
        help="Workspace repo path",
    )
    parser.add_argument(
        "--target",
        default="tests/test_integration_quality.py",
        help="Specific test file or node id to run",
    )
    parser.add_argument(
        "--markers",
        default="integration",
        help="Pytest marker expression",
    )

    args = parser.parse_args()

    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"

    cmd = [
        sys.executable,
        "-B",
        "-m",
        "pytest",
        "-m",
        args.markers,
        "-vv",
        args.target,
        "-p",
        "no:cacheprovider",
    ]

    print("Running:", " ".join(cmd))
    print("CWD:", args.repo)

    result = subprocess.run(cmd, cwd=args.repo, env=env)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
