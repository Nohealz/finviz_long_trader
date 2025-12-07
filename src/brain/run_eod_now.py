from __future__ import annotations

from dotenv import load_dotenv

from .brain_service import build_services


def main() -> None:
    load_dotenv()
    scheduler = build_services()
    if scheduler._eod_callback:
        scheduler._eod_callback()
    else:
        print("No EOD callback configured.")


if __name__ == "__main__":
    main()
