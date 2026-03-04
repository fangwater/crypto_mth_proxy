#!/usr/bin/env python3
import argparse
import struct
from pathlib import Path

import zmq


EVENT_MAP = {
    1: "inc",
    2: "trade",
    3: "tick",
}

SIDE_MAP = {
    0: "buy/bid",
    1: "sell/ask",
}

ORIGIN_MAP = {
    0: "open",
    1: "hedge",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Subscribe to one proxy IPC stream and print decoded messages."
    )
    parser.add_argument(
        "--ipc-dir",
        default="/tmp/mth_pubs",
        help="Base IPC directory (default: /tmp/mth_pubs)",
    )
    parser.add_argument(
        "--channel",
        default="binance-futures-binance-futures",
        help="Channel directory name, e.g. okex-futures-binance-futures",
    )
    parser.add_argument(
        "--symbol",
        default="BTCUSDT",
        help="Symbol IPC file name without extension (default: BTCUSDT)",
    )
    parser.add_argument(
        "--max-msg",
        type=int,
        default=0,
        help="Stop after N messages; 0 means run forever",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=1000,
        help="Polling timeout in ms (default: 1000)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    symbol = args.symbol.strip().upper()
    if not symbol:
        raise SystemExit("symbol is empty")

    endpoint_path = Path(args.ipc_dir) / args.channel / f"{symbol}.ipc"
    endpoint = f"ipc://{endpoint_path}"

    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.SUB)
    sock.setsockopt(zmq.SUBSCRIBE, b"")
    sock.connect(endpoint)

    print(f"[INFO] subscribe endpoint: {endpoint}")
    print("[INFO] waiting for messages ... (Ctrl+C to exit)")

    printed = 0
    skipped_zero_price = 0
    try:
        while True:
            if not sock.poll(args.timeout_ms):
                continue

            msg = sock.recv()
            if len(msg) != 28:
                continue

            event_type, side_id, is_snapshot, origin, ts_us, price, amount = struct.unpack(
                "<BBBBqdd", msg
            )

            # Filter noisy zero-price messages (typically tick events) at script layer.
            if price == 0.0:
                skipped_zero_price += 1
                continue

            event = EVENT_MAP.get(event_type, f"unknown({event_type})")
            side = SIDE_MAP.get(side_id, str(side_id))
            origin_text = ORIGIN_MAP.get(origin, str(origin))

            printed += 1
            print(
                f"[{printed}] event={event} ts_us={ts_us} side={side} "
                f"is_snapshot={is_snapshot} origin={origin_text} "
                f"price={price:.10f} amount={amount:.10f}"
            )

            if args.max_msg > 0 and printed >= args.max_msg:
                break
    except KeyboardInterrupt:
        pass
    finally:
        sock.close(0)

    print(
        f"[INFO] exit printed={printed} skipped_zero_price={skipped_zero_price}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
