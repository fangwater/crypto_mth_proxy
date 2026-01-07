#!/usr/bin/env python3
"""
对比 Binance WebSocket 直连 vs Kafka 数据

采集所有币种的 liquidation 数据 10 分钟，对比两边是否一致
"""
import asyncio
import websockets
import json
import time
import threading
import zlib
import struct
from datetime import datetime
from collections import defaultdict
from kafka import KafkaConsumer, TopicPartition
from python_socks.sync import Proxy as SyncProxy

# ============ 配置 ============
DURATION_SECONDS = 600  # 10分钟
KAFKA_BOOTSTRAP = '10.61.10.22:9092'
KAFKA_TOPIC = 'binance-futures_liquidation'
BUCKET_SECONDS = 3  # Binance 打印时间桶（秒）

# 代理配置 (设置为 None 则不使用代理)
PROXY_URL = "socks5://127.0.0.1:10808"  # 修改为你的代理地址

# ============ Kafka 解析函数 ============
def decode_varint(data, offset):
    value = 0
    shift = 0
    while offset < len(data):
        b = data[offset]
        offset += 1
        value |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
    return value, offset

def normalize_ts_ms(ts):
    if ts is None:
        return None
    # Heuristic: microseconds are 16 digits, milliseconds are 13 digits.
    if ts > 10_000_000_000_000:
        return ts // 1000
    return ts

def normalize_side(side):
    if not side:
        return None
    side = side.upper()
    if side in ('B', 'BUY'):
        return 'BUY'
    if side in ('S', 'SELL'):
        return 'SELL'
    return side

def parse_liquidation_info(data):
    """解析 LiquidationInfo"""
    offset = 0
    ts = None
    side = None
    price = None
    amount = None

    while offset < len(data):
        byte = data[offset]
        field_num = byte >> 3
        wire_type = byte & 0x07
        offset += 1

        if wire_type == 0:
            v, offset = decode_varint(data, offset)
            if field_num == 1:
                ts = v
        elif wire_type == 1:
            if offset + 8 <= len(data):
                v = struct.unpack('<d', data[offset:offset+8])[0]
                if field_num == 3:
                    price = v
                elif field_num == 4:
                    amount = v
            offset += 8
        elif wire_type == 2:
            length, offset = decode_varint(data, offset)
            if offset + length <= len(data):
                val = data[offset:offset+length]
                if field_num == 2:
                    side = val.decode('utf-8')
            offset += length
        else:
            break

    ts_ms = normalize_ts_ms(ts)
    side = normalize_side(side)
    if ts_ms is None:
        return None
    return {
        'ts_ms': ts_ms,
        'side': side,
        'price': price,
        'amount': amount
    }

def parse_liquidation_symbol_info(data):
    """解析 LiquidationSymbolInfo"""
    offset = 0
    symbol = None
    liqs = []

    while offset < len(data):
        byte = data[offset]
        field_num = byte >> 3
        wire_type = byte & 0x07
        offset += 1

        if wire_type == 2:
            length, offset = decode_varint(data, offset)
            if offset + length <= len(data):
                val = data[offset:offset+length]
                if field_num == 1:
                    symbol = val.decode('utf-8').upper()
                elif field_num == 2:
                    liq = parse_liquidation_info(val)
                    if liq:
                        liqs.append(liq)
            offset += length
        elif wire_type == 0:
            _, offset = decode_varint(data, offset)
        elif wire_type == 1:
            offset += 8
        else:
            break

    return symbol, liqs

def parse_kafka_message(data):
    """解析 LiquidationPeriodMessage，返回 liquidation 列表和条数"""
    offset = 0
    liquidations = []
    total_count = 0

    while offset < len(data):
        byte = data[offset]
        field_num = byte >> 3
        wire_type = byte & 0x07
        offset += 1

        if wire_type == 2:
            length, offset = decode_varint(data, offset)
            if offset + length <= len(data):
                val = data[offset:offset+length]
                if field_num == 5:
                    symbol, liqs = parse_liquidation_symbol_info(val)
                    total_count += len(liqs)
                    if symbol and is_usdt_perpetual(symbol):
                        for liq in liqs:
                            record = {
                                'symbol': symbol,
                                'ts_ms': liq['ts_ms'],
                                'side': liq['side'],
                                'price': liq['price'],
                                'amount': liq['amount'],
                            }
                            liquidations.append(record)
            offset += length
        elif wire_type == 0:
            _, offset = decode_varint(data, offset)
        elif wire_type == 1:
            offset += 8
        else:
            break

    return liquidations, total_count

def is_usdt_perpetual(symbol):
    """检查是否是USDT永续合约（过滤掉交割合约和非USDT交易对）"""
    if not symbol:
        return False
    symbol = symbol.upper()
    # 必须以USDT结尾
    if not symbol.endswith('USDT'):
        return False
    # 过滤掉交割合约（包含下划线，如 BTCUSDT_250328）
    if '_' in symbol:
        return False
    return True


# ============ 全局变量 ============
binance_records = []
kafka_records = []
kafka_running = True
start_time_ms = None
end_time_ms = None
binance_bucket_start_ms = None
binance_bucket_records = []

# ============ Kafka 消费线程 ============
def kafka_consumer_thread():
    global kafka_records, kafka_running, start_time_ms, end_time_ms
    
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='latest',
        enable_auto_commit=False,
        consumer_timeout_ms=1000
    )
    
    tp = TopicPartition(KAFKA_TOPIC, 0)
    consumer.assign([tp])
    
    # 定位到最新
    end = consumer.end_offsets([tp])
    consumer.seek(tp, end[tp])
    
    print(f"[Kafka] 开始消费，从 offset {end[tp]} 开始")
    
    while kafka_running:
        for msg in consumer:
            if not kafka_running:
                break
            
            # 只处理时间范围内的消息
            if start_time_ms and msg.timestamp < start_time_ms - 5000:
                continue
            if end_time_ms and msg.timestamp > end_time_ms + 10000:
                continue
            
            try:
                data = zlib.decompress(msg.value)
                liqs, total_count = parse_kafka_message(data)
                print(f"[Kafka] tp={msg.topic}-{msg.partition} liq_count={total_count}")
                for liq in liqs:
                    if start_time_ms and liq['ts_ms'] >= start_time_ms - 1000:
                        kafka_records.append(liq)
            except Exception as e:
                continue
    
    consumer.close()
    print(f"[Kafka] 消费结束，共 {len(kafka_records)} 条记录")

# ============ Binance WebSocket ============
async def collect_binance():
    global binance_records, start_time_ms, end_time_ms
    global binance_bucket_start_ms, binance_bucket_records

    # 连接所有 liquidation 流
    uri = "wss://fstream.binance.com/ws/!forceOrder@arr"

    print(f"[Binance] 连接 WebSocket: {uri}")
    if PROXY_URL:
        print(f"[Binance] 使用代理: {PROXY_URL}")
    print(f"[Binance] 采集 {DURATION_SECONDS} 秒...")

    try:
        # 如果配置了代理，通过代理连接
        if PROXY_URL:
            proxy = SyncProxy.from_url(PROXY_URL)
            # Use sync proxy to get a raw socket; websockets expects socket.socket.
            sock = await asyncio.to_thread(
                proxy.connect,
                dest_host="fstream.binance.com",
                dest_port=443,
            )
            websocket = websockets.connect(
                uri,
                sock=sock,
                server_hostname="fstream.binance.com",
                ping_interval=20,
                ping_timeout=10
            )
        else:
            websocket = websockets.connect(uri, ping_interval=20, ping_timeout=10)

        async with websocket as ws:
            start_time = time.time()
            start_time_ms = int(start_time * 1000)
            
            while time.time() - start_time < DURATION_SECONDS:
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    data = json.loads(message)
                    
                    # Binance forceOrder 格式
                    order = data.get('o', {})
                    symbol = order.get('s', '').upper()
                    if order.get('X') == 'FILLED' and is_usdt_perpetual(symbol):
                        record = {
                            'symbol': symbol,
                            'ts_ms': int(order.get('T', 0)),
                            'side': order.get('S'),
                            'price': float(order.get('ap', 0)),
                            'amount': float(order.get('z', 0)),
                        }
                        binance_records.append(record)

                        check_binance_bucket(record['ts_ms'])
                        binance_bucket_records.append(record)
                        
                except asyncio.TimeoutError:
                    maybe_flush_binance_bucket(int(time.time() * 1000))
                    continue
                except Exception as e:
                    print(f"  [Binance] 错误: {e}")
                    
            flush_binance_bucket()
            end_time_ms = int(time.time() * 1000)
            
    except asyncio.CancelledError:
        print("[Binance] 采集被中断")
        raise
    except Exception as e:
        print(f"[Binance] 连接错误: {e}")
    
    print(f"[Binance] 采集结束，共 {len(binance_records)} 条记录")

def get_bucket_start(timestamp_ms):
    seconds = timestamp_ms // 1000
    bucket_seconds = (seconds // BUCKET_SECONDS) * BUCKET_SECONDS
    return bucket_seconds * 1000

def format_bucket_range(bucket_start_ms):
    start = datetime.fromtimestamp(bucket_start_ms / 1000)
    end = datetime.fromtimestamp((bucket_start_ms + BUCKET_SECONDS * 1000) / 1000)
    return (
        f"[{start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} ~ "
        f"{end.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]})"
    )

def flush_binance_bucket():
    global binance_bucket_records
    if not binance_bucket_records:
        return

    range_str = format_bucket_range(binance_bucket_start_ms)
    print("")
    print(f"{'=' * 20} {range_str} {'=' * 20}")
    print(f"本桶共 {len(binance_bucket_records)} 条强平")
    print("-" * 80)
    for record in binance_bucket_records:
        side_desc = '空头被强平' if record['side'] == 'BUY' else '多头被强平'
        time_str = datetime.fromtimestamp(record['ts_ms'] / 1000).strftime('%H:%M:%S.%f')[:-3]
        print(
            f"  {time_str} | {record['symbol']:12} | {side_desc} | "
            f"数量: {record['amount']:10.4f} | 均价: {record['price']:.2f}"
        )

    binance_bucket_records = []

def check_binance_bucket(timestamp_ms):
    global binance_bucket_start_ms
    bucket_start = get_bucket_start(timestamp_ms)
    if binance_bucket_start_ms is None:
        binance_bucket_start_ms = bucket_start
        return
    if bucket_start > binance_bucket_start_ms:
        flush_binance_bucket()
        binance_bucket_start_ms = bucket_start

def maybe_flush_binance_bucket(now_ms):
    global binance_bucket_start_ms
    if binance_bucket_start_ms is None or not binance_bucket_records:
        return
    bucket_end_ms = binance_bucket_start_ms + BUCKET_SECONDS * 1000
    if now_ms >= bucket_end_ms + 1000:
        flush_binance_bucket()
        binance_bucket_start_ms = get_bucket_start(now_ms)

# ============ 主程序 ============
def main():
    print("=" * 80)
    print("Binance WebSocket vs Kafka 实时对比")
    print(f"采集时长: {DURATION_SECONDS} 秒 ({DURATION_SECONDS//60} 分钟)")
    print(f"开始时间: {datetime.now()}")
    print("=" * 80)
    print()
    
    # 启动 Kafka 消费线程
    kafka_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    kafka_thread.start()
    
    # 等待一下让 Kafka 准备好
    time.sleep(2)
    
    interrupted = False
    try:
        # 运行 Binance 采集
        asyncio.run(collect_binance())

        # 等待 Kafka 消费完剩余数据
        print("\n等待 Kafka 消费完成...")
        time.sleep(10)
    except KeyboardInterrupt:
        print("\n收到 Ctrl+C，准备退出...")
        interrupted = True
    finally:
        global kafka_running
        kafka_running = False
        kafka_thread.join(timeout=2)

    if interrupted:
        return
    
    # ============ 对比分析 ============
    print("\n" + "=" * 80)
    print("对比分析")
    print("=" * 80)
    
    print(f"\n总记录数:")
    print(f"  Binance 直连: {len(binance_records)} 条")
    print(f"  Kafka:        {len(kafka_records)} 条")
    
    # 按 (symbol, ts_ms, amount) 做 key 匹配
    binance_set = {(r['symbol'], r['ts_ms'], round(r['amount'], 6)): r for r in binance_records}
    kafka_set = {(r['symbol'], r['ts_ms'], round(r['amount'], 6)): r for r in kafka_records}
    
    common_keys = set(binance_set.keys()) & set(kafka_set.keys())
    only_binance_keys = set(binance_set.keys()) - set(kafka_set.keys())
    only_kafka_keys = set(kafka_set.keys()) - set(binance_set.keys())
    
    print(f"\n匹配结果:")
    print(f"  两边都有:     {len(common_keys)} 条")
    print(f"  只在 Binance: {len(only_binance_keys)} 条")
    print(f"  只在 Kafka:   {len(only_kafka_keys)} 条")
    
    if len(binance_records) > 0:
        match_rate = len(common_keys) / len(binance_records) * 100
        print(f"\n匹配率: {match_rate:.1f}%")
    
    # 显示只在 Binance 的记录
    if only_binance_keys:
        print(f"\n只在 Binance 的记录 (Kafka 缺失):")
        for key in sorted(only_binance_keys)[:20]:
            r = binance_set[key]
            dt = datetime.fromtimestamp(r['ts_ms']/1000)
            print(f"  {dt} | {r['symbol']:12} | {r['side']:4} | {r['amount']:.6f} @ {r['price']:.2f}")
        if len(only_binance_keys) > 20:
            print(f"  ... 还有 {len(only_binance_keys) - 20} 条")
    
    # 显示只在 Kafka 的记录
    if only_kafka_keys:
        print(f"\n只在 Kafka 的记录 (Binance 缺失):")
        for key in sorted(only_kafka_keys)[:20]:
            r = kafka_set[key]
            dt = datetime.fromtimestamp(r['ts_ms']/1000)
            print(f"  {dt} | {r['symbol']:12} | {r['side']:4} | {r['amount']:.6f} @ {r['price']:.2f}")
        if len(only_kafka_keys) > 20:
            print(f"  ... 还有 {len(only_kafka_keys) - 20} 条")
    
    # 按币种统计
    print(f"\n按币种统计:")
    binance_by_symbol = defaultdict(int)
    kafka_by_symbol = defaultdict(int)
    
    for r in binance_records:
        binance_by_symbol[r['symbol']] += 1
    for r in kafka_records:
        kafka_by_symbol[r['symbol']] += 1
    
    all_symbols = sorted(set(binance_by_symbol.keys()) | set(kafka_by_symbol.keys()))
    
    print(f"  {'币种':12} | {'Binance':>8} | {'Kafka':>8} | {'差异':>8}")
    print(f"  {'-'*12} | {'-'*8} | {'-'*8} | {'-'*8}")
    for symbol in all_symbols[:30]:
        b = binance_by_symbol.get(symbol, 0)
        k = kafka_by_symbol.get(symbol, 0)
        diff = k - b
        diff_str = f"+{diff}" if diff > 0 else str(diff)
        print(f"  {symbol:12} | {b:>8} | {k:>8} | {diff_str:>8}")
    
    print("\n" + "=" * 80)
    print(f"结束时间: {datetime.now()}")
    print("=" * 80)

if __name__ == "__main__":
    main()
