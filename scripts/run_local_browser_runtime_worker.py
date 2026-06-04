from __future__ import annotations

import asyncio

from src.platform.application_root import get_application_root


async def _main() -> None:
    worker = get_application_root().build_local_browser_runtime_worker()
    await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
