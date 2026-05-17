import argparse
import asyncio
import unittest

import bench_rps


class BenchRpsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.server = await asyncio.start_server(self.handle_client, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def asyncTearDown(self) -> None:
        self.server.close()
        await self.server.wait_closed()

    async def handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            while True:
                await reader.readexactly(bench_rps.PUSH_FRAME_SIZE)
                writer.write(b"\x00\x00\x00\x00\x00")
                await writer.drain()
        except asyncio.IncompleteReadError:
            writer.close()
            await writer.wait_closed()

    async def test_run_benchmark_completes_with_warmup(self) -> None:
        args = argparse.Namespace(
            host="127.0.0.1",
            port=self.port,
            connections=2,
            count=10,
            warmup=4,
            uid=1,
        )

        await asyncio.wait_for(bench_rps.run_benchmark(args), timeout=1)


if __name__ == "__main__":
    unittest.main()
