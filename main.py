"""Local entrypoint. Docker image runs `python -u scanner.py` (see Dockerfile)."""

from scanner import FortressScanner

if __name__ == "__main__":
    FortressScanner().run_forever()
