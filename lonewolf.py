# -*- coding: utf-8 -*-
"""
Lone Wolf game code — extracted VERBATIM from BR-LW/main.py (working file).
Only renames applied:
  - build_match_startup_packets -> build_match_startup_packets_lw
  - play_game -> play_game_lw
  - START_MATCH_INTERVAL / NEW_MATCH_DELAY / MAX_MATCH_DURATION ->
    LW_* variants (BR-LW's original 3.0s / 3.0s / 700 values; the BR file
    uses 1.0s / 1.0s / 70000 for Battle Royale)
  - print_colored(..., Colors.X) -> print_info(...)
All shared helpers (UDP crypto, protobuf, bot_state, etc.) are imported
lazily from Main so there is no circular-import problem and zero duplication.
"""
import asyncio
import json
import random
import socket
import struct
import time
from typing import Dict, List, Optional, Any

# LW's original timing values (from BR-LW/main.py)
LW_START_MATCH_INTERVAL = 3.0
LW_NEW_MATCH_DELAY = 3.0
LW_MAX_MATCH_DURATION = 700


# Shared helpers / constants / state live in Main.py (the working BR file).
# They are imported here directly. This is safe from circular imports because
# lonewolf is only ever imported lazily at runtime from Main.run_account_worker
# (Main never imports lonewolf at its own top level), and even a direct
# `import lonewolf` first would simply load Main fully with no cycle.
# NOTE: module-level __getattr__ (PEP 562) does NOT work for this - it only
# intercepts explicit attribute access, not global name lookups inside
# function bodies (LOAD_GLOBAL), which raised NameError at runtime.
from Main import (
    MATCH_IDLE_TIMEOUT,
    MAX_CONSECUTIVE_PARSE_FAILURES,
    NON_MATCH_RECONNECT_DELAY,
    _dec_match,
    _get_match_count,
    _get_total_match_count,
    _inc_match,
    aes_encrypt,
    bot_state,
    build_hello_packet,
    build_packet,
    build_tcp_startup_packet,
    cache_get,
    cache_invalidate,
    classify,
    decode_packet,
    decode_protobuf,
    has_ssan_zig,
    keepalive_ping,
    layouts_from_mask,
    optimize_tcp_socket,
    optimize_udp_socket,
    print_error,
    print_info,
    print_success,
    print_warning,
    reply_for,
    resolve_host_cloudflare,
    send_keep_alive,
    sv_frame,
    thunderFF_pb2,
    uleb_encode,
)


def _M():
    """Access to Main's shared namespace (kept for compatibility)."""
    import Main
    return Main

async def start_game_lone_wolf(region, client_version, writer, key, iv):
    packet = bytes.fromhex("080112800a0a010b102b3a110a044944433110aa011a064555524f50453a100a044944433210311a064555524f504540014a0801090a0b1219202758016291090a8001303838463832424630324139363736373032303130313030303030303030303030303136303030313030313530303032323246393745454530463030303030303436373632353134303030303030303030303030303030303030303030303030303030303030303030303030303066663030303030303030636163666131366410241afb02735d5e571400024a775d45414d1a041b1c001f11010449715f4243481a001e1d071c1703004b1a4066785c524570735c51486775421b5c5a4c07504042685a63610816054e19025e75196001477c015165406370195f5547404e4550640103020f1304064863754268676c755f65576e40467e5f0a417a4701026d675d6e73670b1108495a4c6a0b78470b740065645e525a057258425f584a447d4e6759440c11044e7c596d7f4b625f7d04055a47505c4e1d6b5b4107447d7201057d7f0f14084e430457674f7e517d72015172415d027473577c4d615f79535256780911030f4d5e027a797f614165067806505d53777750475e75064257076500460817014e741e7e5078487e7a7c465e7669767153497064605a7376677773550d160148037e18675966787f4c42607a645f577e7b441b460776026b18685d0b110205490060020f70676175654674706671797f41067346677c4e06585e780f15074c57047b40517075415f6364027259674b5b0166407f7340600407770a22047a5d5c52300b3a0a167305067162727516134208312e3133302e3232480350015ae90403626253513635686e556f4e36416456324b796f566c636f477776484f624e56526c4d727073504b4f43654177616848494176795556497273743752737149734a7a786b3247525268377a2f637664626d504f6a73552f79626d38547a4c69586d2f474351696d494b53486833447955726f39515152756c34545350626d6d624b7949565937545671577059455372323646572f59624578507338514f706d317372785455736c30796a434144444d4f34616a654b615753366361496c554b4963797a494e396d52516f715277687939797257476d337a644345337a6a61436f492f5a585233656f65365a42647a64677654636b6b665733356e4d4c6a6a565072564b6433523172756174394e50514150724a5546627859696c4c5a3859707336654d5447666b6649793574666a526c314d4648706b51774c6373374439656378566c41636f374e664f6d2b30654756466c4434744478706771385533595973587645384842502f70666c767a737138316a32524f4d7857437556445442492f684735625462773166456e4249725162762b636144775147696f74554e316d4c4b77734379456f4766706746614251457645672b736a764c4c78704743334c304a5344532f74526169504354553344374e6249306547516651622f5a466f4c36455630775a324d6f583932414c572f5049752f56634663584e70596b356f7966326151416a536971486a2f363276354843644f525551303578754e6171795251625653704654303137655237675255636b4966366c6f447476342b514e4a4670766d74757077707774396a5a5974437a4b56743657726d6e36785837706658456251555434684f3758a201050803108703a201050804108103a20105080510c001a20105081d10cc01a2010408161078a20105080e10af01a201020815")
    proto = thunderFF_pb2.StartMatch()
    proto.ParseFromString(packet)
    if hasattr(proto.main, 'region_list') and len(proto.main.region_list) > 0:
        proto.main.region_list[0].region = region
        if len(proto.main.region_list) > 1:
            proto.main.region_list[1].region = region
    if hasattr(proto.main, 'client_version'):
        proto.main.client_version.remote_version = client_version
    packet = proto.SerializeToString()
    encrypted_packet = (await aes_encrypt(packet, key, iv)).hex()
    packet_length = len(encrypted_packet) // 2
    hex_length = hex(packet_length)[2:]
    hex_length = hex_length if len(hex_length) > 1 else "0" + hex_length
    reg = str(region).upper() if region else "BD"
    reg_prefix = "031900" if reg == "BD" else ("031400" if reg == "IND" else "031500")
    final_packet = reg_prefix + "0" * (6 - len(hex_length)) + hex_length + encrypted_packet
    writer.write(bytes.fromhex(final_packet))
    await writer.drain()

async def build_match_startup_packets_lw(token, udp_key, match_code, account_id, block_val,
                                      server_ip="", region="BD", client_version="1.132.6",
                                      client_version_code="2019121227", access_token=""):
    token = token.strip()
    udp_key = bytes.fromhex(udp_key)
    match_code = [int(ch) for ch in str(match_code).strip()]
    thunder_jwt = token[:660] if len(token) > 660 else token
    sharma_jwt = token[660:] if len(token) > 660 else ""
    encoded_thunder_jwt = thunder_jwt.encode() if isinstance(thunder_jwt, str) else thunder_jwt
    encoded_sharma_jwt = sharma_jwt.encode() if isinstance(sharma_jwt, str) else sharma_jwt
    garena420 = await has_ssan_zig(len(encoded_thunder_jwt)) + encoded_thunder_jwt
    reg = str(region).upper() if region else "BD"
    csoversea_block = bytes.fromhex(
        "ca0163736f7665727365612e7374726f6e67686f6c642e66726565666972656d6f62696c652e636f6d"
        "3b302e302e302e303b33342e3132362e37362e34353b33342e38372e3137372e31343b33342e38372e"
        "3137302e3233303b33352e3138352e3138332e35370000000000000100000000000000000000000001"
        "00000800000100000000000100a8a2d7bebd8d8bdf110200"
    )
    mid = bytes.fromhex('0000000001000102030101') + await has_ssan_zig(len(reg)) + reg.encode()
    mid += bytes.fromhex('0001030003000004')
    mid += await has_ssan_zig(len(client_version)) + client_version.encode()
    mid += await has_ssan_zig(len(client_version_code)) + client_version_code.encode()
    mid += csoversea_block
    clean_ip = server_ip.split(':')[0] if server_ip else "0.0.0.0"
    mid += await has_ssan_zig(len(clean_ip)) + clean_ip.encode()
    clean_acc_tok = access_token.strip() if access_token else ""
    if clean_acc_tok:
        mid += await has_ssan_zig(len(clean_acc_tok)) + clean_acc_tok.encode()
    mid += await has_ssan_zig(len(encoded_sharma_jwt)) + encoded_sharma_jwt
    tg_garena420 = (
        await uleb_encode(int(account_id)) +
        await uleb_encode(int(block_val)) +
        await uleb_encode(1) +
        await uleb_encode(43) +
        await uleb_encode(int(block_val)) +
        await uleb_encode(11) +
        mid
    )
    process = await sv_frame(0x5E, match_code, 2, 447, 0, 1, garena420, udp_key)
    loading = await sv_frame(0x5A, match_code, 2, 448, 1, 1, tg_garena420, udp_key)
    return process.hex(), loading.hex()

async def play_game_lw(server_ip_port, thunder, sharma, udp_key, match_code,
                    account_id, player_region, client_version, key, iv,
                    match_index: int):
    match_start_time = time.time()
    ping_task = None
    sock = None
    ping_stop = asyncio.Event()
    uid_str = str(account_id)
    completed_cleanly = False

    try:
        ip, port = server_ip_port.split(":")
        port = int(port)
        resolved_ip = await resolve_host_cloudflare(ip)
        loop = asyncio.get_event_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        optimize_udp_socket(sock)
        sock.setblocking(False)
        udp_key_bytes = bytes.fromhex(udp_key)
        hello_packet = await build_hello_packet(f"{account_id}_2585", udp_key_bytes, match_code)
        await loop.sock_sendto(sock, bytes.fromhex(hello_packet), (resolved_ip, port))

        ack_state = "waiting_for_hello_reply"
        thunder_sent = False
        sharma_sent = False
        join_match_received = False
        local_closed = False
        send_lock = asyncio.Lock()

        ping_task = asyncio.create_task(keepalive_ping(sock, resolved_ip, port, udp_key_bytes, match_code, ping_stop))
        last_activity = time.time()
        MAX_IDLE_BEFORE_HELLO_RESEND = 7.0

        print_info(f"🎮 [MATCH #{match_index}] UDP started → {server_ip_port} (DNS: {resolved_ip})")

        async def send_thunder_sharma_inline():
            nonlocal ack_state, thunder_sent, sharma_sent
            if thunder_sent:
                return
            async with send_lock:
                if thunder_sent:
                    return
                try:
                    await loop.sock_sendto(sock, bytes.fromhex(thunder), (resolved_ip, port))
                    thunder_sent = True
                    await asyncio.sleep(0.1)
                    prepare_ack = await build_packet(0x68, (await layouts_from_mask(match_code))[1], 0, 2, None, 1, b"\x01\x00", udp_key_bytes)
                    await loop.sock_sendto(sock, prepare_ack, (resolved_ip, port))
                    await asyncio.sleep(0.2)
                    await loop.sock_sendto(sock, bytes.fromhex(sharma), (resolved_ip, port))
                    sharma_sent = True
                    ack_state = "thunder_sharma_sent"
                    print_success(f"[MATCH #{match_index}] Thunder+Sharma sent!")
                except Exception as e:
                    print_error(f"[MATCH #{match_index}] send error: {e}")

        while not local_closed:
            if time.time() - match_start_time > LW_MAX_MATCH_DURATION:
                break
            try:
                response, server_addr = await asyncio.wait_for(loop.sock_recvfrom(sock, 65535), timeout=1.5)
                if response:
                    last_activity = time.time()
                    frame = await decode_packet(response, udp_key_bytes, match_code)
                    if frame:
                        ptype = await classify(frame)
                        if frame['cmd'] in [103, 107]:
                            print_success(f"[MATCH #{match_index}] Completed (cmd {frame['cmd']})")
                            completed_cleanly = True
                            local_closed = True
                            continue
                        if frame['cmd'] == 101:
                            try:
                                ack_pkt = await build_packet(0x68, (await layouts_from_mask(match_code))[1], 0, 2, None, 1, b"\x01\x00", udp_key_bytes)
                                await loop.sock_sendto(sock, ack_pkt, server_addr)
                            except Exception:
                                pass
                            continue
                        if ptype in ["ACK", "PING", "HELLO", "JOIN_MATCH"]:
                            if ptype == "HELLO" and ack_state == "waiting_for_hello_reply":
                                typ, reply = await reply_for(frame, udp_key_bytes, match_code, ack_style="short")
                                if reply:
                                    await loop.sock_sendto(sock, reply, server_addr)
                                ack_state = "ack_sent_waiting"
                            elif ptype == "ACK":
                                if ack_state == "waiting_for_hello_reply":
                                    typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                    if reply:
                                        await loop.sock_sendto(sock, reply, server_addr)
                                    ack_state = "ready_to_send_thunder"
                                elif ack_state == "ack_sent_waiting":
                                    ack_state = "ready_to_send_thunder"
                                else:
                                    typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                    if reply:
                                        await loop.sock_sendto(sock, reply, server_addr)
                            elif ptype == "PING":
                                typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                if reply:
                                    await loop.sock_sendto(sock, reply, server_addr)
                            elif ptype == "JOIN_MATCH" and not join_match_received:
                                typ, reply = await reply_for(frame, udp_key_bytes, match_code)
                                if reply:
                                    await loop.sock_sendto(sock, reply, server_addr)
                                    join_match_received = True
            except asyncio.TimeoutError:
                if ack_state == "ready_to_send_thunder" and not thunder_sent:
                    await send_thunder_sharma_inline()
                elif ack_state == "waiting_for_hello_reply":
                    if (time.time() - last_activity) > MAX_IDLE_BEFORE_HELLO_RESEND:
                        try:
                            pkt = await build_hello_packet(f"{account_id}_2585", udp_key_bytes, match_code)
                            await loop.sock_sendto(sock, bytes.fromhex(pkt), (resolved_ip, port))
                        except Exception:
                            pass
                        last_activity = time.time()
                    if (time.time() - match_start_time) > 25.0:
                        print_warning(f"[MATCH #{match_index}] Handshake timeout")
                        break
                elif ack_state == "thunder_sharma_sent":
                    if (time.time() - last_activity) > MATCH_IDLE_TIMEOUT:
                        print_success(f"[MATCH #{match_index}] Finished naturally")
                        completed_cleanly = True
                        break
                continue
            except BlockingIOError:
                await asyncio.sleep(0.05)
            except OSError:
                await asyncio.sleep(0.5)
                continue
            except Exception:
                await asyncio.sleep(0.5)
                continue
            if ack_state == "ready_to_send_thunder" and not thunder_sent:
                await send_thunder_sharma_inline()

        return f"match #{match_index} finished"
    except Exception as e:
        print_error(f"[MATCH #{match_index}] error: {e}")
        return f"match #{match_index} error"
    finally:
        if completed_cleanly:
            try:
                bot_state.increment_match(uid_str)
            except Exception:
                pass
        ping_stop.set()
        if ping_task:
            ping_task.cancel()
            try:
                await ping_task
            except asyncio.CancelledError:
                pass
        if sock:
            try:
                sock.close()
            except Exception:
                pass
        remaining = await _dec_match(uid_str)
        total = await _get_total_match_count()
        print_info(f"[MATCH #{match_index}] Closed. UID active: {remaining} | Total active: {total}")
        try:
            bot_state.update_status(uid_str, "IN_MATCH" if remaining > 0 else "ONLINE", remaining)
        except Exception:
            pass

async def functional_lone_wolf(addrs, starter_packet, account_region, client_version,
                                key, iv, account_id="", account_data=None,
                                max_reconnects=10, match_interval=None):
    reconnects = 0
    ip, port = addrs.split(":")
    play_matches: List[asyncio.Task] = []
    no_response_count = 0
    search_attempts = 0
    last_start_time = 0.0
    uid_str = str(account_id)
    consecutive_parse_failures = 0
    current_token = starter_packet
    current_key = key
    current_iv = iv
    current_account_data = account_data
    try:
        while True:
            writer = None
            try:
                if current_account_data:
                    fresh = None
                    if current_account_data.get('auth_type') == 'guest' and current_account_data.get('auth_uid'):
                        fresh = cache_get(str(current_account_data['auth_uid']))
                    elif current_account_data.get('auth_type') == 'token' and current_account_data.get('auth_token'):
                        fresh = cache_get(f"tok_{current_account_data['auth_token'][:20]}")
                    if fresh:
                        current_account_data = fresh
                        current_key = fresh['aes_ak']
                        current_iv = fresh['iv_i']
                        current_token = await build_tcp_startup_packet(
                            fresh['account_id'], fresh['token'], fresh['server_time'],
                            current_key, current_iv, region=fresh.get('region', account_region), typ='OnLine'
                        )
                    else:
                        print_warning(f"[FUNCTIONAL] Cache miss for {uid_str} → re-login needed")
                        try:
                            if current_account_data.get('auth_uid'):
                                cache_invalidate(str(current_account_data['auth_uid']))
                            if current_account_data.get('auth_token'):
                                cache_invalidate(f"tok_{current_account_data['auth_token'][:20]}")
                        except Exception:
                            pass
                        raise ConnectionError("Cache expired, triggering fresh login")
                resolved_ip = await resolve_host_cloudflare(ip)
                reader, writer = await asyncio.open_connection(resolved_ip, int(port))
                raw_sock = writer.get_extra_info('socket')
                if raw_sock:
                    optimize_tcp_socket(raw_sock)
                writer.write(bytes.fromhex(current_token))
                await writer.drain()
                try:
                    init_ka = await send_keep_alive(account_region)
                    if init_ka and writer and not writer.is_closing():
                        writer.write(init_ka)
                        await asyncio.wait_for(writer.drain(), timeout=3)
                except Exception:
                    pass
                print_success(f"[FUNCTIONAL] TCP Gateway Connected for UID: {uid_str} (DNS: {resolved_ip})")
                reconnects = 0
                no_response_count = 0
                last_start_time = 0.0

                async def send_start_match():
                    nonlocal search_attempts, last_start_time
                    search_attempts += 1
                    current_region = "BD"
                    print_info(f"[LONE WOLF] Sending StartMatch #{search_attempts} region: {current_region}")
                    try:
                        await asyncio.sleep(random.uniform(0.3, 0.6))
                        await start_game_lone_wolf(current_region, client_version, writer, current_key, current_iv)
                        print_success("[LONE WOLF] StartMatch packet sent")
                        active = await _get_match_count(uid_str)
                        try:
                            bot_state.update_status(uid_str, "SEARCHING", active)
                        except Exception:
                            pass
                    except Exception as e:
                        print_error(f"start_game_lone_wolf error: {e}")
                    last_start_time = asyncio.get_running_loop().time()

                await send_start_match()

                while True:
                    play_matches[:] = [m for m in play_matches if not m.done()]
                    active_count = await _get_match_count(uid_str)
                    try:
                        bot_state.update_status(uid_str, "ONLINE" if active_count == 0 else "IN_MATCH", active_count)
                    except Exception:
                        pass
                    now = asyncio.get_running_loop().time()
                    _mi = match_interval if match_interval else LW_START_MATCH_INTERVAL
                    try:
                        _cap = min(2000000, int(bot_state.max_matches_per_account))
                    except Exception:
                        _cap = 2000000
                    if len(play_matches) < _cap and now - last_start_time >= _mi:
                        await send_start_match()
                    try:
                        data = await asyncio.wait_for(reader.read(8192), timeout=0.5)
                    except asyncio.TimeoutError:
                        no_response_count += 1
                        if no_response_count > 80:
                            print_warning(f"[FUNCTIONAL] Gateway silent ({uid_str}). Reconnecting...")
                            raise ConnectionError("Gateway idle timeout")
                        continue
                    if not data:
                        raise ConnectionError("Connection closed by server")
                    hex_data = data.hex()
                    packet_length = len(data)
                    no_response_count = 0
                    if hex_data.startswith("0300") and 10 < packet_length < 30:
                        print_info("Match starting, please wait...")
                        continue
                    if hex_data.startswith("0300") and packet_length >= 300:
                        print_info("=" * 60)
                        print_info(f"MATCH FOUND! Loading...")
                        print_info("=" * 60)
                        try:
                            res = json.loads(await decode_protobuf(hex_data[10:]))
                            token = None
                            udp_key = None
                            match_code = None
                            server_ip_port = None
                            match_account_id = None
                            block_val = None
                            if '42' in res and 'data' in res['42']:
                                match_code = res['42']['data']
                            if '5' in res and 'data' in res['5']:
                                res_field5 = res['5']['data']
                                server_ip_port = res_field5.get('2', {}).get('data')
                                udp_key = res_field5.get('3', {}).get('data')
                                token = res_field5.get('4', {}).get('data')
                                if '42' in res_field5:
                                    match_code = res_field5['42']['data']
                            if '1' in res and 'data' in res['1']:
                                match_account_id = res['1']['data']
                            if '5' in res and 'data' in res['5']:
                                block_val = res['5']['data'].get('1', {}).get('data')
                            effective_acc_id = match_account_id or account_id or "BD_BOT"
                            if token and udp_key and match_code and server_ip_port:
                                acc_tok = ""
                                if current_account_data:
                                    acc_tok = current_account_data.get('access_token', '') or ""
                                thunder, sharma = await build_match_startup_packets_lw(
                                    token, udp_key, match_code, effective_acc_id, block_val or 0,
                                    server_ip=server_ip_port, region=account_region,
                                    client_version=client_version, access_token=acc_tok
                                )
                                match_index = await _inc_match(uid_str)
                                total = await _get_total_match_count()
                                print_info(f"🚀 [MATCH #{match_index}] UDP starting → {server_ip_port} (background)")
                                print_success(f"[FUNCTIONAL] UDP task started. UID active: {match_index} | Total: {total}")
                                new_match = asyncio.create_task(play_game_lw(
                                    server_ip_port, thunder, sharma, udp_key, match_code,
                                    effective_acc_id, "BD", client_version, current_key, current_iv,
                                    match_index=match_index
                                ))
                                play_matches.append(new_match)
                                consecutive_parse_failures = 0
                                try:
                                    writer.close()
                                    await writer.wait_closed()
                                except Exception:
                                    pass
                                print_info(f"[OFFLINE] {LW_NEW_MATCH_DELAY}s offline → reload token → new StartMatch")
                                await asyncio.sleep(LW_NEW_MATCH_DELAY)
                                reconnects = 0
                                break
                            else:
                                consecutive_parse_failures += 1
                                print_warning(f"[FUNCTIONAL] Non-match big packet (#{consecutive_parse_failures}/{MAX_CONSECUTIVE_PARSE_FAILURES}) → reconnecting")
                                if consecutive_parse_failures >= MAX_CONSECUTIVE_PARSE_FAILURES:
                                    print_error(f"[FUNCTIONAL] {MAX_CONSECUTIVE_PARSE_FAILURES}x parse failures → invalidating cache for fresh login")
                                    if current_account_data:
                                        try:
                                            if current_account_data.get('auth_uid'):
                                                cache_invalidate(str(current_account_data['auth_uid']))
                                            if current_account_data.get('auth_token'):
                                                cache_invalidate(f"tok_{current_account_data['auth_token'][:20]}")
                                        except Exception:
                                            pass
                                    consecutive_parse_failures = 0
                                try:
                                    writer.close()
                                    await writer.wait_closed()
                                except Exception:
                                    pass
                                await asyncio.sleep(NON_MATCH_RECONNECT_DELAY)
                                break
                        except Exception as e:
                            print_error(f"[FUNCTIONAL] Match packet error: {e}")
                            consecutive_parse_failures += 1
                            if consecutive_parse_failures >= MAX_CONSECUTIVE_PARSE_FAILURES:
                                if current_account_data:
                                    try:
                                        if current_account_data.get('auth_uid'):
                                            cache_invalidate(str(current_account_data['auth_uid']))
                                        if current_account_data.get('auth_token'):
                                            cache_invalidate(f"tok_{current_account_data['auth_token'][:20]}")
                                    except Exception:
                                        pass
                                consecutive_parse_failures = 0
                            try:
                                writer.close()
                                await writer.wait_closed()
                            except Exception:
                                pass
                            await asyncio.sleep(NON_MATCH_RECONNECT_DELAY)
                            break
                    if 30 <= packet_length <= 40:
                        continue
            except asyncio.CancelledError:
                print_warning(f"[FUNCTIONAL] Cancelled — cancelling {len(play_matches)} UDP matches")
                for m in play_matches:
                    if not m.done():
                        m.cancel()
                if play_matches:
                    await asyncio.gather(*play_matches, return_exceptions=True)
                play_matches.clear()
                raise
            except Exception as e:
                print_error(f"[FUNCTIONAL] TCP state ({uid_str}): {e}")
                play_matches[:] = [m for m in play_matches if not m.done()]
                if writer:
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except Exception:
                        pass
                if "Cache expired" in str(e):
                    print_warning(f"[FUNCTIONAL] Triggering re-login for {uid_str}")
                    # stop in-flight LW matches first so no orphaned UDP game
                    # keeps running while the worker re-logs in and restarts LW
                    for m in list(play_matches):
                        if not m.done():
                            m.cancel()
                    if play_matches:
                        try:
                            await asyncio.wait_for(
                                asyncio.gather(*play_matches, return_exceptions=True),
                                timeout=15)
                        except Exception:
                            pass
                    play_matches.clear()
                    break
                reconnects += 1
                if reconnects > max_reconnects:
                    print_error("[FUNCTIONAL] Max reconnects reached, retrying...")
                    reconnects = 0
                    await asyncio.sleep(3)
                    continue
                await asyncio.sleep(min(reconnects, 2))
    except asyncio.CancelledError:
        print_warning(f"[FUNCTIONAL] Outer cancelled. {len(play_matches)} UDP matches still running.")
        for m in play_matches:
            if not m.done():
                m.cancel()
        if play_matches:
            await asyncio.gather(*play_matches, return_exceptions=True)
        play_matches.clear()
        raise