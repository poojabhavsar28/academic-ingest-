import os

import uvicorn

from .asyncio_compat import configure_windows_event_loop


def main() -> None:
    configure_windows_event_loop()
    uvicorn.run(
        "ingest.api:app",
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "8080")),
    )


if __name__ == "__main__":
    main()
