# -*- coding: utf-8 -*-
"""
FreeFire Level Up Bot - Professional Web Dashboard & Real-Time EXP Tracker
Embedded Async Web Server (aiohttp)
Optimized for ultra-smooth operation, zero memory leaks, and dynamic multi-account control.
"""

import asyncio
import json
import os
import re
import time
from typing import Dict, List, Any, Optional
from aiohttp import web

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "index.html")

EXP_TABLE: Dict[int, int] = {
    1: 0, 2: 48, 3: 202, 4: 544, 5: 1012, 6: 1844, 7: 2792, 8: 3800,
    9: 4870, 10: 6004, 11: 7192, 12: 8448, 13: 9760, 14: 11140, 15: 12566,
    16: 14060, 17: 15610, 18: 17224, 19: 18902, 20: 20632, 21: 22424, 22: 24278,
    23: 26192, 24: 28166, 25: 30200, 26: 32294, 27: 34448, 28: 37804, 29: 41274,
    30: 44870, 31: 48582, 32: 53394, 33: 58566, 34: 64096, 35: 69994, 36: 76460,
    37: 83506, 38: 91128, 39: 99322, 40: 108092, 41: 120144, 42: 133266, 43: 147472,
    44: 162760, 45: 179126, 46: 196572, 47: 215368, 48: 235516, 49: 257010, 50: 279860,
    51: 304056, 52: 348318, 53: 394982, 54: 444044, 55: 495508, 56: 549364, 57: 633756,
    58: 721744, 59: 813336, 60: 908522, 61: 1041438, 62: 1180352, 63: 1325266,
    64: 1476184, 65: 1634300, 66: 1840946, 67: 2056594, 68: 2281242, 69: 2514880,
    70: 2757530, 71: 3059506, 72: 3372284, 73: 3699456, 74: 4041030, 75: 4397002,
    76: 4829104, 77: 5282204, 78: 5756304, 79: 6251408, 80: 6776502, 81: 7381324,
    82: 8043154, 83: 8752982, 84: 9510808, 85: 10316338, 86: 11277190, 87: 12291748,
    88: 13360304, 89: 14482858, 90: 15659418, 91: 17026708, 92: 18453950, 93: 19941280,
    94: 21488570, 95: 23095858, 96: 24763138, 97: 26490428, 98: 28378704, 99: 30124996,
    100: 32032884
}

def calculate_level_progress(level: int, current_exp: int) -> Dict[str, Any]:
    level = max(1, level)
    next_level = min(100, level + 1)
    base_exp = EXP_TABLE.get(level, 0)
    target_exp = EXP_TABLE.get(next_level, base_exp + 50000)
    
    needed_for_level = max(1, target_exp - base_exp)
    earned_in_level = max(0, current_exp - base_exp)
    remaining_exp = max(0, target_exp - current_exp)
    progress_pct = min(100.0, max(0.0, (earned_in_level / needed_for_level) * 100.0))

    return {
        "next_level": next_level,
        "base_exp": base_exp,
        "target_exp": target_exp,
        "needed_for_level": needed_for_level,
        "earned_in_level": earned_in_level,
        "remaining_exp": remaining_exp,
        "progress_pct": round(progress_pct, 1)
    }

# Global bot state shared between Main.py and Web Dashboard
class BotState:
    def __init__(self):
        self.accounts: Dict[str, Dict[str, Any]] = {}
        self.logs: List[Dict[str, Any]] = []
        self.max_logs = 200
        self.errors: List[Dict[str, Any]] = []
        self.max_errors = 100
        self.match_start_times: Dict[str, float] = {}
        self.match_history: List[Dict[str, Any]] = []
        self.max_match_history = 100
        self.max_account_match_history = 20
        self.udp_silence_threshold = 30  # seconds without server packets while IN_MATCH
        self.udp_rate_window = 60        # seconds of packet timestamps kept for pkt/sec
        self.total_matches = 0
        self.total_gained_exp = 0
        self.start_time = time.time()
        self.account_workers: Dict[str, asyncio.Task] = {}
        self.account_token_map: Dict[str, str] = {}  # uid -> token or token_prefix -> uid
        self.auth_to_game_id: Dict[str, str] = {}   # guest login uid -> in-game account id
        self.game_to_auth_id: Dict[str, str] = {}   # in-game account id -> guest login uid
        self.paused_accounts: set = set()
        self.refresh_callbacks: Dict[str, Any] = {}
        self.account_credentials: Dict[str, Dict[str, Any]] = {}
        self.active_writers: Dict[str, set] = {}
        self.target_level: int = 30
        self.milestone_checkpoint: int = 8
        self.max_active: int = 20
        self.max_matches_per_account: int = 8
        self.lw_break_after: int = 1000
        self.lw_break_minutes: int = 1
        self._lw_break_counters: Dict[str, int] = {}
        self.account_targets: Dict[str, int] = {}  # login-uid -> target level override (schedule)
        self.waiting: list = []  # verified accounts parked until a worker slot frees up
        self.proxies_file = "proxies.json"
        self.proxies: List[Dict[str, Any]] = []  # {host,port,username,password,status,last_checked,latency_ms}
        self.proxy_enabled: bool = False
        # Per-account play preferences (set from dashboard when adding accounts)
        self.account_play_mode: Dict[str, str] = {}  # uid -> "AUTO" | "BR" | "LW"
        self.account_speed: Dict[str, float] = {}     # uid -> match interval seconds (0 = default)
        self._proxy_rr: int = 0
        self._load_proxies()
        self.completed_file = "completed.txt"
        self.settings_file = "br_settings.json"
        self.completed: Dict[str, Dict[str, Any]] = {}
        self._load_settings()
        self._load_completed()

    def register_writer(self, uid: str, writer):
        uid_str = str(uid)
        if uid_str not in self.active_writers:
            self.active_writers[uid_str] = set()
        self.active_writers[uid_str].add(writer)

    def unregister_writer(self, uid: str, writer):
        uid_str = str(uid)
        if uid_str in self.active_writers:
            self.active_writers[uid_str].discard(writer)
            if not self.active_writers[uid_str]:
                self.active_writers.pop(uid_str, None)

    def close_writers_for_account(self, uid: str):
        uid_str = str(uid)
        candidates = {uid_str}
        if uid_str in self.auth_to_game_id:
            candidates.add(str(self.auth_to_game_id[uid_str]))
        if uid_str in self.game_to_auth_id:
            candidates.add(str(self.game_to_auth_id[uid_str]))
        if uid_str in self.account_token_map:
            mapped = self.account_token_map[uid_str]
            candidates.add(str(mapped))
            candidates.add(str(mapped)[:16])

        for c in list(candidates):
            writers = list(self.active_writers.get(c, []))
            for w in writers:
                try:
                    if hasattr(w, "close"):
                        if hasattr(w, "is_closing"):
                            if not w.is_closing():
                                w.close()
                        else:
                            w.close()
                except Exception:
                    pass
            self.active_writers.pop(c, None)

    def log(self, message: str, level: str = "info", uid: Optional[str] = None):
        entry = {
            "time": time.strftime("%H:%M:%S"),
            "level": level,
            "message": message,
            "uid": str(uid) if uid else None
        }
        self.logs.append(entry)
        if len(self.logs) > self.max_logs:
            self.logs.pop(0)
        if level == "error":
            self.errors.append({
                "time": entry["time"],
                "message": message,
                "uid": entry["uid"]
            })
            if len(self.errors) > self.max_errors:
                self.errors.pop(0)

    def log_error(self, message: str, uid: Optional[str] = None, detail: Optional[str] = None):
        """Record a structured error with optional response detail (one entry per list)."""
        entry = {
            "time": time.strftime("%H:%M:%S"),
            "level": "error",
            "message": f"{message} | {detail}" if detail else message,
            "uid": str(uid) if uid else None
        }
        self.logs.append(entry)
        if len(self.logs) > self.max_logs:
            self.logs.pop(0)
        self.errors.append({
            "time": entry["time"],
            "message": message,
            "uid": entry["uid"],
            "detail": detail
        })
        if len(self.errors) > self.max_errors:
            self.errors.pop(0)

    def clear_errors(self):
        self.errors.clear()

    def register_account(self, uid: str, nickname: str, region: str, level: int, exp: int,
                         likes: int = 0, token: Optional[str] = None, auth_uid: Optional[str] = None):
        uid_str = str(uid)
        auth_uid_str = str(auth_uid) if auth_uid else self.game_to_auth_id.get(uid_str, "")
        if auth_uid_str:
            self.auth_to_game_id[auth_uid_str] = uid_str
            self.game_to_auth_id[uid_str] = auth_uid_str
            self.account_token_map[auth_uid_str] = uid_str
            self.account_token_map[uid_str] = auth_uid_str
        if token:
            self.account_token_map[uid_str] = token
            self.account_token_map[token[:16]] = uid_str
            if auth_uid_str:
                self.account_token_map[auth_uid_str] = token

        prog = calculate_level_progress(level or 1, exp)

        lvl_val = level or 1
        acc_mode = "BR" if lvl_val < 3 else "LONE_WOLF"
        acc_mode_label = "Battle Royale (Lvl < 3)" if lvl_val < 3 else "Lone Wolf (Lvl 3+)"

        if uid_str not in self.accounts:
            self.accounts[uid_str] = {
                "uid": uid_str,
                "auth_uid": auth_uid_str or "",
                "nickname": nickname or f"Player_{uid_str[:6]}",
                "region": region or "BD",
                "level": lvl_val,
                "next_level": prog["next_level"],
                "mode": acc_mode,
                "mode_label": acc_mode_label,
                "initial_exp": exp,
                "current_exp": exp,
                "gained_exp": 0,
                "remaining_exp": prog["remaining_exp"],
                "target_exp": prog["target_exp"],
                "needed_for_level": prog["needed_for_level"],
                "earned_in_level": prog["earned_in_level"],
                "progress_pct": prog["progress_pct"],
                "likes": likes or 0,
                "status": "PAUSED" if self.is_paused(uid_str) else "ONLINE",
                "matches_played": 0,
                "active_matches": 0,
                "last_match_time": None,
                "match_history": [],
                "udp": self._fresh_udp_stats(),
                "token": token or "",
                "start_time": time.time(),
                "is_paused": self.is_paused(uid_str),
                "paused_at": time.time() if self.is_paused(uid_str) else None,
                "total_pause_duration": 0.0,
                "last_updated": time.strftime("%H:%M:%S")
            }
        else:
            acc = self.accounts[uid_str]
            if auth_uid_str:
                acc["auth_uid"] = auth_uid_str
            if nickname:
                acc["nickname"] = nickname
            if region:
                acc["region"] = region
            if level:
                acc["level"] = level
            if token:
                acc["token"] = token
            acc["current_exp"] = exp
            acc["gained_exp"] = max(0, exp - acc["initial_exp"])
            acc["next_level"] = prog["next_level"]
            acc["remaining_exp"] = prog["remaining_exp"]
            acc["target_exp"] = prog["target_exp"]
            acc["needed_for_level"] = prog["needed_for_level"]
            acc["earned_in_level"] = prog["earned_in_level"]
            acc["progress_pct"] = prog["progress_pct"]
            acc["likes"] = likes
            if not acc.get("is_paused"):
                acc["status"] = "ONLINE"
            acc["last_updated"] = time.strftime("%H:%M:%S")
            acc.setdefault("udp", self._fresh_udp_stats())
        self.recalc_totals()
        self.check_level_milestones(uid_str, 1, cancel_workers=False)

    def get_account_uptime(self, uid_str: str) -> int:
        acc = self.accounts.get(uid_str)
        if not acc:
            mapped = self.game_to_auth_id.get(uid_str) or self.auth_to_game_id.get(uid_str)
            if mapped and mapped in self.accounts:
                acc = self.accounts[mapped]
        if not acc:
            return 0
        start_t = acc.get("start_time", time.time())
        total_pause = acc.get("total_pause_duration", 0.0)
        if acc.get("is_paused") and acc.get("paused_at"):
            return max(0, int(acc["paused_at"] - start_t - total_pause))
        return max(0, int(time.time() - start_t - total_pause))

    def is_paused(self, uid: str) -> bool:
        uid_str = str(uid)
        if uid_str in self.paused_accounts:
            return True
        game_id = self.auth_to_game_id.get(uid_str)
        if game_id and game_id in self.paused_accounts:
            return True
        auth_uid = self.game_to_auth_id.get(uid_str)
        if auth_uid and auth_uid in self.paused_accounts:
            return True
        acc = self.accounts.get(uid_str) or (self.accounts.get(game_id) if game_id else None)
        if acc and acc.get("is_paused"):
            return True
        return False

    def toggle_pause(self, uid: str) -> bool:
        uid_str = str(uid)
        candidates = {uid_str}
        if uid_str in self.auth_to_game_id:
            candidates.add(self.auth_to_game_id[uid_str])
        if uid_str in self.game_to_auth_id:
            candidates.add(self.game_to_auth_id[uid_str])

        target_acc = None
        target_key = uid_str
        for c in candidates:
            if c in self.accounts:
                target_acc = self.accounts[c]
                target_key = c
                break

        is_now_paused = not self.is_paused(uid_str)
        if is_now_paused:
            for c in candidates:
                self.paused_accounts.add(c)
                self.close_writers_for_account(c)
            if target_acc:
                target_acc["is_paused"] = True
                target_acc["paused_at"] = time.time()
                target_acc["status"] = "PAUSED"
            nick = target_acc.get("nickname", target_key) if target_acc else target_key
            self.log(f"⏸ UID {target_key} ({nick}) matchmaking PAUSED (TCP socket disconnected).", "warning", target_key)
            if "on_pause_toggle" in self.refresh_callbacks:
                try:
                    asyncio.create_task(self.refresh_callbacks["on_pause_toggle"](target_key, True))
                except Exception:
                    pass
        else:
            for c in candidates:
                self.paused_accounts.discard(c)
            if target_acc:
                target_acc["is_paused"] = False
                if target_acc.get("paused_at"):
                    pause_dur = time.time() - target_acc["paused_at"]
                    target_acc["total_pause_duration"] = target_acc.get("total_pause_duration", 0.0) + pause_dur
                    target_acc["paused_at"] = None
                target_acc["status"] = "ONLINE"
            nick = target_acc.get("nickname", target_key) if target_acc else target_key
            self.log(f"▶ UID {target_key} ({nick}) matchmaking RESUMED.", "success", target_key)
            if "on_pause_toggle" in self.refresh_callbacks:
                try:
                    asyncio.create_task(self.refresh_callbacks["on_pause_toggle"](target_key, False))
                except Exception:
                    pass

        return is_now_paused

    def toggle_pause_all(self) -> bool:
        any_active = any(not self.is_paused(k) for k in self.accounts.keys())
        for k in list(self.accounts.keys()):
            current_paused = self.is_paused(k)
            if any_active and not current_paused:
                self.toggle_pause(k)
            elif not any_active and current_paused:
                self.toggle_pause(k)
        return any_active

    # ---------- target level / completed (UID:PASS) ----------
    def _load_settings(self):
        try:
            with open(self.settings_file, "r", encoding="utf-8") as f:
                d = json.load(f)
            tl = int(d.get("target_level", 30))
            if 2 <= tl <= 100:
                self.target_level = tl
            ma = int(d.get("max_active", 20))
            if 1 <= ma <= 200:
                self.max_active = ma
            mm = int(d.get("max_matches_per_account", 8))
            if 1 <= mm <= 50:
                self.max_matches_per_account = mm
            ba = int(d.get("lw_break_after", 1000))
            if 0 <= ba <= 100000:
                self.lw_break_after = ba
            bm = int(d.get("lw_break_minutes", 1))
            if 1 <= bm <= 1440:
                self.lw_break_minutes = bm
        except FileNotFoundError:
            pass
        except Exception:
            pass

    def save_settings(self):
        try:
            with open(self.settings_file, "w", encoding="utf-8") as f:
                json.dump({"target_level": self.target_level, "max_active": self.max_active,
                           "max_matches_per_account": self.max_matches_per_account,
                           "lw_break_after": self.lw_break_after,
                           "lw_break_minutes": self.lw_break_minutes}, f)
        except Exception:
            pass

    def set_target_level(self, lvl: int):
        self.target_level = int(lvl)
        self.save_settings()
        self.log(f"Target level set to {lvl} for all accounts.", "info", "")
        for uid in list(self.accounts.keys()):
            try:
                if int(self.accounts[uid].get("level", 1) or 1) >= self.target_level:
                    self.check_level_milestones(str(uid), 1)
            except Exception:
                pass

    def set_max_active(self, n: int):
        n = int(n)
        if not 1 <= n <= 200:
            raise ValueError("max_active must be 1-200")
        self.max_active = n
        self.save_settings()
        self.log(f"Max concurrent working accounts set to {n}. Extra accounts wait in queue.", "info", "")

    def set_max_matches_per_account(self, n: int):
        n = int(n)
        if not 1 <= n <= 50:
            raise ValueError("max matches must be 1-50")
        self.max_matches_per_account = n
        self.save_settings()
        self.log(f"Max concurrent matches per account set to {n}.", "info", "")

    def set_lw_break(self, after_matches: int, minutes: int):
        try:
            after_matches = int(after_matches)
        except Exception:
            after_matches = 0
        try:
            minutes = int(minutes)
        except Exception:
            minutes = 1
        if not 0 <= after_matches <= 100000:
            raise ValueError("break-after must be 0-100000")
        if not 1 <= minutes <= 1440:
            raise ValueError("break minutes must be 1-1440")
        self.lw_break_after = after_matches
        self.lw_break_minutes = minutes
        self.save_settings()
        if after_matches > 0:
            self.log(f"LW auto-break set: {after_matches} matches -> {minutes}min rest.", "info", "")
        else:
            self.log("LW auto-break disabled.", "info", "")

    def get_lw_break_count(self, uid: str) -> int:
        u = str(uid)
        total = int(self._lw_break_counters.get(u, 0) or 0)
        # also count under mapped ids (match packets may carry either form)
        try:
            for k in (self.auth_to_game_id.get(u), self.game_to_auth_id.get(u)):
                if k:
                    total += int(self._lw_break_counters.get(str(k), 0) or 0)
        except Exception:
            pass
        return total

    def reset_lw_break_count(self, uid: str):
        u = str(uid)
        self._lw_break_counters[u] = 0
        try:
            for k in (self.auth_to_game_id.get(u), self.game_to_auth_id.get(u)):
                if k:
                    self._lw_break_counters[str(k)] = 0
        except Exception:
            pass

    def set_account_target(self, uid: str, level: int):
        """Per-account target-level override (used by Schedule). Falls back to global."""
        try:
            lvl = int(level)
        except Exception:
            return
        if lvl >= 2:
            self.account_targets[str(uid)] = lvl
        else:
            self.account_targets.pop(str(uid), None)
        try:
            self.refresh_completion_targets()
        except Exception:
            pass

    def clear_account_target(self, uid: str):
        self.account_targets.pop(str(uid), None)

    def refresh_completion_targets(self):
        """Re-evaluate target_done from completed.txt milestones using each
        account's effective target (per-account override else global).
        Needed because _load_completed() runs before account_targets exist."""
        for uid, e in self.completed.items():
            try:
                eff = self.get_account_target(uid)
                if any(int(lvl) >= eff for lvl in e.get("milestones", {})):
                    e["target_done"] = True
                    self.paused_accounts.add(str(uid))
            except Exception:
                pass

    def reset_completion(self, uid: str):
        """Undo a target completion so the account can level again (schedule restart)."""
        u = str(uid)
        cands = {u}
        try:
            if u in self.auth_to_game_id:
                cands.add(str(self.auth_to_game_id[u]))
            if u in self.game_to_auth_id:
                cands.add(str(self.game_to_auth_id[u]))
        except Exception:
            pass
        for c in cands:
            e = self.completed.get(c)
            if e:
                e["target_done"] = False
            self.paused_accounts.discard(c)
            acc = self.accounts.get(c)
            if acc:
                acc["is_paused"] = False
                acc["completed"] = False
                if acc.get("status") == "PAUSED":
                    acc["status"] = "ONLINE"

    def get_account_target(self, uid: str) -> int:
        """Effective target for an account: override (uid or mapped id) else global."""
        u = str(uid)
        for k in (u, self.auth_to_game_id.get(u), self.game_to_auth_id.get(u)):
            if k and k in self.account_targets:
                try:
                    return int(self.account_targets[k])
                except Exception:
                    pass
        return self.target_level

    def active_worker_count(self) -> int:
        """Distinct live worker tasks (several keys can map to one task)."""
        seen = set()
        for t in self.account_workers.values():
            try:
                if t is not None and not t.done():
                    seen.add(id(t))
            except Exception:
                pass
        return len(seen)

    def waiting_idents(self):
        out = []
        for item in self.waiting:
            try:
                if item.get("uid"):
                    out.append(str(item["uid"]))
                elif item.get("token"):
                    out.append("Token_" + str(item["token"])[:8] + "...")
            except Exception:
                pass
        return out

    def _waiting_ident(self, item) -> str:
        if item.get("uid"):
            return "uid:" + str(item["uid"])
        if item.get("token"):
            return "token:" + str(item["token"])
        return ""

    def park_waiting(self, item) -> bool:
        """Park a verified account until a worker slot frees. False if already queued."""
        ident = self._waiting_ident(item)
        if not ident:
            return False
        for w in self.waiting:
            if self._waiting_ident(w) == ident:
                return False
        self.waiting.append(dict(item))
        return True

    def drop_waiting(self, uid=None, token=None):
        keep = []
        for w in self.waiting:
            if uid and w.get("uid") and str(w["uid"]) == str(uid):
                continue
            if token and w.get("token") and w["token"] == token:
                continue
            keep.append(w)
        self.waiting[:] = keep

    # ==================== SOCKS5 PROXY MANAGER ====================
    def _load_proxies(self):
        try:
            with open(self.proxies_file, "r", encoding="utf-8") as f:
                d = json.load(f)
            self.proxies = d.get("proxies", []) if isinstance(d, dict) else []
            self.proxy_enabled = bool(d.get("enabled", False)) if isinstance(d, dict) else False
        except Exception:
            self.proxies = []
            self.proxy_enabled = False

    def save_proxies(self):
        try:
            with open(self.proxies_file, "w", encoding="utf-8") as f:
                json.dump({"enabled": self.proxy_enabled, "proxies": self.proxies}, f, indent=2)
        except Exception:
            pass

    @staticmethod
    def parse_proxy_line(line: str):
        """Accepts: host:port | user:pass@host:port | socks5://[user:pass@]host:port | host:port:user:pass"""
        s = line.strip().strip(",;")
        if not s or s.startswith("#"):
            return None
        for prefix in ("socks5://", "socks://"):
            if s.lower().startswith(prefix):
                s = s[len(prefix):]
                break
        username = ""
        password = ""
        if "@" in s:
            auth, s = s.rsplit("@", 1)
            if ":" in auth:
                username, password = auth.split(":", 1)
            else:
                username = auth
        parts = s.split(":")
        host = ""
        port_s = ""
        if len(parts) == 4 and not username:
            # host:port:user:pass format
            host, port_s, username, password = parts[0], parts[1], parts[2], parts[3]
        elif len(parts) == 2:
            host, port_s = parts[0], parts[1]
        else:
            return None
        host = host.strip().strip("[]")
        try:
            port = int(port_s.strip())
        except Exception:
            return None
        if not host or not (1 <= port <= 65535):
            return None
        return {"host": host, "port": port, "username": username.strip(),
                "password": password.strip(), "status": "unchecked",
                "last_checked": "", "latency_ms": 0}

    def add_proxies(self, lines):
        added, skipped = [], 0
        for line in lines:
            p = self.parse_proxy_line(str(line))
            if not p:
                skipped += 1
                continue
            if any(x["host"] == p["host"] and x["port"] == p["port"] for x in self.proxies):
                skipped += 1
                continue
            self.proxies.append(p)
            added.append(p)
        self.save_proxies()
        return added, skipped

    def remove_proxy(self, host, port):
        before = len(self.proxies)
        self.proxies = [x for x in self.proxies
                        if not (x["host"] == str(host) and int(x["port"]) == int(port))]
        if len(self.proxies) != before:
            self.save_proxies()
            return True
        return False

    def set_proxy_enabled(self, enabled: bool):
        self.proxy_enabled = bool(enabled)
        self.save_proxies()

    def get_valid_proxies(self):
        return [p for p in self.proxies if p.get("status") == "valid"]

    # ---- per-account play mode + speed ----
    @staticmethod
    def _norm_mode(mode):
        m = str(mode or "AUTO").strip().upper()
        return m if m in ("AUTO", "BR", "LW") else "AUTO"

    def _mode_keys(self, uid_str):
        keys = [str(uid_str)]
        try:
            if uid_str in self.auth_to_game_id:
                keys.append(str(self.auth_to_game_id[uid_str]))
            if uid_str in self.game_to_auth_id:
                keys.append(str(self.game_to_auth_id[uid_str]))
        except Exception:
            pass
        return keys

    def set_play_mode(self, uid, mode):
        m = self._norm_mode(mode)
        for k in self._mode_keys(uid):
            self.account_play_mode[k] = m

    def get_play_mode(self, uid):
        for k in self._mode_keys(uid):
            if k in self.account_play_mode:
                return self.account_play_mode[k]
        return "AUTO"

    def set_speed(self, uid, speed):
        try:
            s = float(speed or 0)
        except Exception:
            s = 0.0
        if s < 0:
            s = 0.0
        for k in self._mode_keys(uid):
            self.account_speed[k] = s

    def get_speed(self, uid):
        for k in self._mode_keys(uid):
            if k in self.account_speed:
                return self.account_speed[k]
        return 0.0

    def get_next_proxy(self):
        """Round-robin pick of next valid proxy. None if disabled or none valid."""
        if not self.proxy_enabled:
            return None
        valid = self.get_valid_proxies()
        if not valid:
            return None
        self._proxy_rr = (self._proxy_rr + 1) % len(valid)
        return valid[self._proxy_rr]

    async def check_proxy(self, proxy: Dict[str, Any], timeout: float = 12.0) -> Dict[str, Any]:
        """Validate a SOCKS5 proxy: handshake (+auth) then CONNECT to www.google.com:80."""
        host = proxy["host"]
        port = int(proxy["port"])
        username = proxy.get("username", "")
        password = proxy.get("password", "")
        t0 = time.time()
        ok, err = False, ""
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
            try:
                writer.write(b"\x05\x02\x00\x02" if username else b"\x05\x01\x00")
                await writer.drain()
                resp = await asyncio.wait_for(reader.readexactly(2), timeout=timeout)
                if len(resp) != 2 or resp[0] != 0x05:
                    err = "bad socks version"
                else:
                    method = resp[1]
                    if method == 0x02:
                        u = username.encode()[:255]
                        pw = password.encode()[:255]
                        writer.write(b"\x01" + bytes([len(u)]) + u + bytes([len(pw)]) + pw)
                        await writer.drain()
                        ar = await asyncio.wait_for(reader.readexactly(2), timeout=timeout)
                        if len(ar) != 2 or ar[1] != 0x00:
                            err = "auth failed"
                        else:
                            method = 0x00
                    if not err:
                        if method != 0x00:
                            err = f"no acceptable auth method ({method})"
                        else:
                            writer.write(b"\x05\x01\x00\x03\x0ewww.google.com\x00\x50")
                            await writer.drain()
                            cr = await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
                            if len(cr) < 2 or cr[1] != 0x00:
                                err = f"connect refused ({cr[1] if len(cr) >= 2 else '?'})"
                            else:
                                atyp = cr[3]
                                if atyp == 0x01:
                                    await asyncio.wait_for(reader.readexactly(6), timeout=timeout)
                                elif atyp == 0x03:
                                    ln = (await asyncio.wait_for(reader.readexactly(1), timeout=timeout))[0]
                                    await asyncio.wait_for(reader.readexactly(ln + 2), timeout=timeout)
                                elif atyp == 0x04:
                                    await asyncio.wait_for(reader.readexactly(18), timeout=timeout)
                                ok = True
            finally:
                try:
                    writer.close()
                except Exception:
                    pass
        except asyncio.TimeoutError:
            err = "timeout"
        except Exception as e:
            err = type(e).__name__
        proxy["status"] = "valid" if ok else "invalid"
        proxy["last_checked"] = time.strftime("%Y-%m-%d %H:%M:%S")
        proxy["latency_ms"] = round((time.time() - t0) * 1000, 1) if ok else 0
        if err:
            proxy["error"] = err
        else:
            proxy.pop("error", None)
        self.save_proxies()
        return proxy

    async def check_all_proxies(self):
        if self.proxies:
            await asyncio.gather(*(self.check_proxy(p) for p in self.proxies),
                                 return_exceptions=True)
        return self.proxies

    def _load_completed(self):
        self.completed = {}
        try:
            with open(self.completed_file, "r", encoding="utf-8") as f:
                for line in f:
                    m = re.match(r"^\[(.*?)\] UID:(\S+) PASS Level:(\d+) Nick:(.*?) Region:(\S+) EXP:(\d+)", line.strip())
                    if not m:
                        continue
                    ts, uid, lvl, nick, region, exp = m.groups()
                    e = self.completed.setdefault(uid, {"nickname": nick, "auth_uid": "", "milestones": {}, "target_done": False})
                    e["milestones"][lvl] = ts
                    if nick:
                        e["nickname"] = nick
                    if int(lvl) >= self.target_level:
                        e["target_done"] = True
        except FileNotFoundError:
            pass
        except Exception as ex:
            print(f"completed load error: {ex}")
        for uid, e in self.completed.items():
            if e.get("target_done"):
                self.paused_accounts.add(uid)

    def is_completed(self, uid: str) -> bool:
        uid_str = str(uid)
        e = self.completed.get(uid_str)
        if e and e.get("target_done"):
            return True
        mapped = self.game_to_auth_id.get(uid_str) or self.auth_to_game_id.get(uid_str)
        if mapped and self.completed.get(str(mapped), {}).get("target_done"):
            return True
        return False

    def get_completed_list(self):
        out = []
        for uid, e in self.completed.items():
            if not e.get("target_done"):
                continue
            out.append({
                "uid": uid,
                "nickname": e.get("nickname", ""),
                "auth_uid": e.get("auth_uid", ""),
                "level": self.target_level,
                "time": e["milestones"].get(str(self.target_level), ""),
            })
        out.sort(key=lambda x: x["time"], reverse=True)
        return out

    def check_level_milestones(self, uid_str: str, old_level: int = 1, cancel_workers: bool = True):
        acc = self.accounts.get(uid_str)
        if not acc:
            return
        try:
            new_level = int(acc.get("level", 1) or 1)
            old_level = int(old_level or 1)
        except Exception:
            return
        if old_level < self.milestone_checkpoint <= new_level:
            self._record_milestone(uid_str, self.milestone_checkpoint, is_target=False)
        eff_target = self.get_account_target(uid_str)
        if old_level < eff_target <= new_level and not self.is_completed(uid_str):
            self._record_milestone(uid_str, eff_target, is_target=True)
            self._auto_off_account(uid_str, cancel_workers=cancel_workers)

    def _record_milestone(self, uid_str: str, level: int, is_target: bool):
        acc = self.accounts.get(uid_str, {})
        entry = self.completed.setdefault(uid_str, {"nickname": "", "auth_uid": "", "milestones": {}, "target_done": False})
        if str(level) in entry["milestones"]:
            return
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        entry["milestones"][str(level)] = ts
        entry["nickname"] = acc.get("nickname", entry.get("nickname", ""))
        entry["auth_uid"] = acc.get("auth_uid", entry.get("auth_uid", ""))
        if is_target:
            entry["target_done"] = True
        line = f"[{ts}] UID:{uid_str} PASS Level:{level} Nick:{entry['nickname']} Region:{acc.get('region', '')} EXP:{acc.get('current_exp', 0)}\n"
        try:
            with open(self.completed_file, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception as ex:
            self.log(f"completed.txt write failed: {ex}", "error", uid_str)
        tag = "TARGET" if is_target else "checkpoint"
        self.log(f"UID {uid_str} ({entry['nickname']}) PASS - Level {level} ({tag}).", "success", uid_str)

    def _auto_off_account(self, uid_str: str, cancel_workers: bool = True):
        candidates = {uid_str}
        if uid_str in self.auth_to_game_id:
            candidates.add(str(self.auth_to_game_id[uid_str]))
        if uid_str in self.game_to_auth_id:
            candidates.add(str(self.game_to_auth_id[uid_str]))
        for c in candidates:
            self.paused_accounts.add(c)
            self.close_writers_for_account(c)
            if cancel_workers:
                t = self.account_workers.pop(c, None)
                if t:
                    try:
                        t.cancel()
                    except Exception:
                        pass
        acc = self.accounts.get(uid_str)
        if acc:
            acc["is_paused"] = True
            acc["paused_at"] = time.time()
            acc["status"] = "PAUSED"
            acc["completed"] = True
            acc["pass_level"] = self.get_account_target(uid_str)
        self.log(f"UID {uid_str} auto-OFF: target Level {self.get_account_target(uid_str)} reached - matchmaking stopped.", "warning", uid_str)
        if "on_pause_toggle" in self.refresh_callbacks:
            try:
                asyncio.create_task(self.refresh_callbacks["on_pause_toggle"](uid_str, True))
            except Exception:
                pass

    def update_exp(self, uid: str, current_exp: int, level: Optional[int] = None):
        uid_str = str(uid)
        if uid_str in self.accounts:
            acc = self.accounts[uid_str]
            old_exp = acc["current_exp"]
            old_level = acc.get("level", 1)
            acc["current_exp"] = current_exp
            if level is not None and level > 0:
                acc["level"] = level
            acc["gained_exp"] = max(0, current_exp - acc["initial_exp"])
            
            current_lvl = acc["level"]
            acc["mode"] = "BR" if current_lvl < 3 else "LONE_WOLF"
            acc["mode_label"] = "Battle Royale (Lvl < 3)" if current_lvl < 3 else "Lone Wolf (Lvl 3+)"

            prog = calculate_level_progress(acc["level"], current_exp)
            acc["next_level"] = prog["next_level"]
            acc["remaining_exp"] = prog["remaining_exp"]
            acc["target_exp"] = prog["target_exp"]
            acc["needed_for_level"] = prog["needed_for_level"]
            acc["earned_in_level"] = prog["earned_in_level"]
            acc["progress_pct"] = prog["progress_pct"]
            acc["last_updated"] = time.strftime("%H:%M:%S")

            # Check for Level 2 -> 3 Mode Transition
            if old_level < 3 and current_lvl >= 3:
                self.log(
                    f"🎉 LEVEL UP! UID {uid_str} ({acc['nickname']}) reached Level {current_lvl}! Switching from Battle Royale to Lone Wolf mode!",
                    "success",
                    uid_str
                )

            diff = current_exp - old_exp
            if diff > 0:
                self._attribute_exp_to_match(uid_str, diff)
                self.log(
                    f"★ UID {uid_str} ({acc['nickname']}) gained +{diff:,} EXP | Level {acc['level']} [{acc['mode']}] ({prog['progress_pct']}% - {prog['remaining_exp']:,} EXP to Lvl {prog['next_level']})",
                    "success",
                    uid_str
                )
            self.recalc_totals()
            self.check_level_milestones(uid_str, old_level)

    def get_account_status(self, uid: str) -> str:
        u = str(uid)
        for k in (u, self.auth_to_game_id.get(u), self.game_to_auth_id.get(u)):
            if k:
                a = self.accounts.get(k)
                if a and a.get("status"):
                    return str(a.get("status"))
        return ""

    def get_account_level(self, uid: str) -> int:
        uid_str = str(uid)
        acc = self.accounts.get(uid_str)
        if not acc:
            mapped = self.game_to_auth_id.get(uid_str) or self.auth_to_game_id.get(uid_str)
            if mapped and mapped in self.accounts:
                acc = self.accounts[mapped]
        if acc:
            return int(acc.get("level", 1) or 1)
        return 1

    def get_account_mode(self, uid: str) -> str:
        lvl = self.get_account_level(uid)
        return "BR" if lvl < 3 else "LONE_WOLF"

    def update_status(self, uid: str, status: str, active_matches: Optional[int] = None):
        uid_str = str(uid)
        if uid_str in self.accounts:
            self.accounts[uid_str]["status"] = status
            if active_matches is not None:
                self.accounts[uid_str]["active_matches"] = active_matches
            self.accounts[uid_str]["last_updated"] = time.strftime("%H:%M:%S")

    def increment_match(self, uid: str):
        uid_str = str(uid)
        self.total_matches += 1
        self._lw_break_counters[uid_str] = self._lw_break_counters.get(uid_str, 0) + 1
        if uid_str in self.accounts:
            self.accounts[uid_str]["matches_played"] += 1
            self.accounts[uid_str]["last_match_time"] = time.strftime("%H:%M:%S")
            self.accounts[uid_str]["last_updated"] = time.strftime("%H:%M:%S")
            self.log(f"⚔ Match #{self.accounts[uid_str]['matches_played']} finished for {self.accounts[uid_str]['nickname']} ({uid_str})", "info", uid_str)

    @staticmethod
    def _fresh_udp_stats() -> Dict[str, Any]:
        return {
            "sent": 0, "recv": 0,
            "bytes_sent": 0, "bytes_recv": 0,
            "sent_kinds": {},   # kind -> count (ping/hello/thunder/sharma/ack/reply)
            "recv_types": {},   # classified incoming type -> count
            "send_times": [],   # recent send timestamps (pruned to rate window)
            "recv_times": [],   # recent recv timestamps (pruned to rate window)
            "last_sent": None,
            "last_recv": None,
            "silence_warned": False,
        }

    def udp_count(self, uid: str, direction: str, kind: str, nbytes: int):
        """Count one UDP packet. direction: 'sent' | 'recv'. Cheap; safe to call per-packet."""
        uid_str = str(uid)
        acc = self.accounts.get(uid_str)
        if acc is None:
            return
        u = acc.setdefault("udp", self._fresh_udp_stats())
        now = time.time()
        if direction == "sent":
            u["sent"] += 1
            u["bytes_sent"] += nbytes
            u["last_sent"] = now
            sk = u.setdefault("sent_kinds", {})
            sk[kind] = sk.get(kind, 0) + 1
            times = u.setdefault("send_times", [])
        else:
            u["recv"] += 1
            u["bytes_recv"] += nbytes
            u["last_recv"] = now
            u["silence_warned"] = False
            rt = u.setdefault("recv_types", {})
            rt[kind] = rt.get(kind, 0) + 1
            times = u.setdefault("recv_times", [])
        times.append(now)
        cutoff = now - self.udp_rate_window
        while times and times[0] < cutoff:  # append-only time order: trim from front
            times.pop(0)

    def udp_check_silence(self, uid: str):
        """Log an error if an IN_MATCH account got no server packets for too long."""
        uid_str = str(uid)
        acc = self.accounts.get(uid_str)
        if not acc or acc.get("status") != "IN_MATCH" or acc.get("is_paused"):
            return
        u = acc.setdefault("udp", self._fresh_udp_stats())
        if u.get("silence_warned"):
            return
        now = time.time()
        last = u.get("last_recv")
        if last:
            silent_for = now - last
        else:
            start = self.match_start_times.get(uid_str)
            if not start:
                return
            silent_for = now - start
        if silent_for >= self.udp_silence_threshold:
            u["silence_warned"] = True
            self.log_error(f"UDP silent for {int(silent_for)}s while IN_MATCH (no packets from server)", uid=uid_str)

    def record_match_start(self, uid: str):
        """Mark a match as started (for duration + EXP/hour tracking)."""
        uid_str = str(uid)
        self.match_start_times[uid_str] = time.time()
        if uid_str in self.accounts:
            acc = self.accounts[uid_str]
            acc["active_matches"] = 1
            u = acc.setdefault("udp", self._fresh_udp_stats())
            u["silence_warned"] = False
            if not acc.get("is_paused"):
                acc["status"] = "IN_MATCH"
            acc["last_updated"] = time.strftime("%H:%M:%S")

    def record_match_end(self, uid: str):
        """Record a finished match with duration; EXP is attached later by update_exp."""
        uid_str = str(uid)
        now = time.time()
        start = self.match_start_times.pop(uid_str, None)
        duration = int(now - start) if start else 0
        entry = {
            "uid": uid_str,
            "start": time.strftime("%H:%M:%S", time.localtime(start)) if start else time.strftime("%H:%M:%S"),
            "end": time.strftime("%H:%M:%S"),
            "duration_sec": duration,
            "exp_gained": 0,
            "pending_exp": True,
            "ts": now,
        }
        acc = self.accounts.get(uid_str)
        if acc is not None:
            hist = acc.setdefault("match_history", [])
            hist.append(entry)
            if len(hist) > self.max_account_match_history:
                del hist[0:len(hist) - self.max_account_match_history]
            acc["active_matches"] = 0
        self.match_history.append(entry)
        if len(self.match_history) > self.max_match_history:
            del self.match_history[0:len(self.match_history) - self.max_match_history]
        self.increment_match(uid_str)

    def _attribute_exp_to_match(self, uid_str: str, diff: int):
        """Attach an EXP gain to the most recent pending match entry of this account."""
        now = time.time()
        for e in reversed(self.match_history):
            if e.get("uid") == uid_str and e.get("pending_exp") and (now - e.get("ts", 0)) < 600:
                e["exp_gained"] = diff
                e["pending_exp"] = False
                break

    def recalc_totals(self):
        self.total_gained_exp = sum(acc.get("gained_exp", 0) for acc in self.accounts.values())


bot_state = BotState()


# Fallback HTML if templates/index.html is missing
FALLBACK_INDEX_HTML = """<!DOCTYPE html>
<html>
<head><title>TAZHINI Bot Dashboard</title></head>
<body style="background:#0a0a12;color:#fff;font-family:sans-serif;text-align:center;padding:50px;">
<h1>TAZHINI BOT RUNNING</h1>
<p>templates/index.html is loading...</p>
</body>
</html>"""


# ==================== HTTP HANDLERS ====================

async def handle_index(request: web.Request) -> web.Response:
    content = FALLBACK_INDEX_HTML
    if os.path.exists(TEMPLATE_PATH):
        try:
            with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
                content = f.read()
        except Exception:
            pass
    return web.Response(text=content, content_type="text/html", charset="utf-8")


async def handle_get_stats(request: web.Request) -> web.Response:
    accounts_data = list(bot_state.accounts.values())
    accounts_data.sort(key=lambda x: x.get("gained_exp", 0), reverse=True)
    uptime_sec = max(1, int(time.time() - bot_state.start_time))
    total_gained = bot_state.total_gained_exp
    exp_per_hour = int((total_gained / uptime_sec) * 3600)
    total_active_matches = sum(acc.get("active_matches", 0) for acc in accounts_data)

    for acc in accounts_data:
        uid_k = str(acc.get("uid", ""))
        acc["uptime_seconds"] = bot_state.get_account_uptime(uid_k)
        acc["is_paused"] = bot_state.is_paused(uid_k)
        acc["completed"] = bot_state.is_completed(uid_k)
        if acc["completed"]:
            acc["pass_level"] = bot_state.target_level
        uptime_h = acc["uptime_seconds"] / 3600.0
        acc["exp_per_hour"] = round(acc.get("gained_exp", 0) / uptime_h, 1) if uptime_h >= 1/60 else 0
        # UDP packet monitor summary
        u = acc.setdefault("udp", bot_state._fresh_udp_stats())
        now = time.time()
        win = bot_state.udp_rate_window
        send_times = [t for t in u.get("send_times", []) if t >= now - win]
        recv_times = [t for t in u.get("recv_times", []) if t >= now - win]
        def _pps(times):
            if not times:
                return 0.0
            return round(len(times) / max(now - times[0], 1.0), 1)
        last_recv = u.get("last_recv")
        acc["udp_summary"] = {
            "sent": u.get("sent", 0),
            "recv": u.get("recv", 0),
            "bytes_sent": u.get("bytes_sent", 0),
            "bytes_recv": u.get("bytes_recv", 0),
            "pps": round(_pps(send_times) + _pps(recv_times), 1),
            "sent_kinds": dict(u.get("sent_kinds", {})),
            "recv_types": dict(u.get("recv_types", {})),
            "last_recv_ago": int(now - last_recv) if last_recv else None,
            "silent": bool(u.get("silence_warned")),
        }

    return web.json_response({
        "ok": True,
        "total_accounts": len(bot_state.accounts),
        "total_matches": bot_state.total_matches,
        "total_active_matches": total_active_matches,
        "total_gained_exp": total_gained,
        "exp_per_hour": exp_per_hour,
        "accounts": accounts_data,
        "recent_matches": list(reversed(bot_state.match_history[-20:])),
        "logs": bot_state.logs[-80:],
        "errors": bot_state.errors[-50:],
        "uptime": uptime_sec,
        "target_level": bot_state.target_level,
        "max_active": bot_state.max_active,
        "max_matches_per_account": bot_state.max_matches_per_account,
        "lw_break_after": bot_state.lw_break_after,
        "lw_break_minutes": bot_state.lw_break_minutes,
        "active_workers": bot_state.active_worker_count(),
        "proxy_enabled": bot_state.proxy_enabled,
        "proxy_valid_count": len(bot_state.get_valid_proxies()),
        "proxy_total_count": len(bot_state.proxies),
        "waiting": bot_state.waiting_idents(),
        "completed": bot_state.get_completed_list()
    })


def _process_account_add(data: Dict[str, Any], existing: List[Dict[str, Any]]):
    """Shared add logic for single + bulk adds. Returns (ok, identifier, error, new_existing)."""
    if "uid" in data and "password" in data:
        uid = str(data["uid"]).strip()
        pwd = str(data["password"]).strip()
        if not uid or not pwd:
            return False, "", "UID and Password are required", existing

        # Cancel previous worker if already running for this UID
        if uid in bot_state.account_workers:
            try:
                bot_state.account_workers[uid].cancel()
            except Exception:
                pass
            bot_state.account_workers.pop(uid, None)

        existing = [acc for acc in existing if str(acc.get("uid", "")) != uid]
        entry = {"uid": uid, "password": pwd}
        try:
            _tl = int(data.get("target_level") or 0)
        except Exception:
            _tl = 0
        if _tl >= 2:
            entry["target_level"] = _tl
            bot_state.set_account_target(uid, _tl)
        pm = bot_state._norm_mode(data.get("play_mode"))
        if pm != "AUTO":
            entry["play_mode"] = pm
        try:
            sp = float(data.get("speed") or 0)
        except Exception:
            sp = 0.0
        if sp > 0:
            entry["speed"] = sp
        existing.append(entry)
        bot_state.set_play_mode(uid, pm)
        bot_state.set_speed(uid, sp)
        return True, uid, "", existing

    elif "token" in data:
        token = str(data["token"]).strip()
        if not token:
            return False, "", "Token is required", existing

        # Cancel worker if token prefix matches
        tok_key = token[:16]
        for k in list(bot_state.account_workers.keys()):
            if k == tok_key or k.startswith(tok_key[:10]) or tok_key.startswith(k[:10]):
                try:
                    bot_state.account_workers[k].cancel()
                except Exception:
                    pass
                bot_state.account_workers.pop(k, None)

        existing = [acc for acc in existing if acc.get("token", "") != token]
        entry = {"token": token}
        pm = bot_state._norm_mode(data.get("play_mode"))
        if pm != "AUTO":
            entry["play_mode"] = pm
        try:
            sp = float(data.get("speed") or 0)
        except Exception:
            sp = 0.0
        if sp > 0:
            entry["speed"] = sp
        existing.append(entry)
        bot_state.set_play_mode(tok_key, pm)
        bot_state.set_play_mode(token, pm)
        bot_state.set_speed(tok_key, sp)
        bot_state.set_speed(token, sp)
        return True, f"Token_{token[:8]}...", "", existing

    return False, "", "Invalid payload", existing


def _parse_bulk_line(line: str) -> Optional[Dict[str, Any]]:
    """Parse one bulk-add line into an add payload. Supports:
    uid:password | uid,password | uid password | uid|password | token:<value>"""
    line = line.strip().strip(",;")
    if not line or line.startswith("#"):
        return None
    if line.lower().startswith("token:"):
        tok = line[6:].strip()
        return {"token": tok} if tok else None
    parts = re.split(r"[:\s,;|]+", line, maxsplit=1)
    if len(parts) == 2 and parts[0].strip() and parts[1].strip():
        return {"uid": parts[0].strip(), "password": parts[1].strip()}
    return None


def _load_accounts_file() -> List[Dict[str, Any]]:
    accounts_file = "accounts.json"
    if os.path.exists(accounts_file):
        try:
            with open(accounts_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []



# ==================== SCHEDULE MANAGER ====================
# Batch level-up: upload a uid:pass file -> run N accounts at a time ->
# each stops at the schedule's target level -> next batch starts automatically.

SCHEDULES_FILE = "schedules.json"
_SCHED_TICK_SEC = 10
_SCHED_IDLE_REQUEUE_SEC = 2700   # 45 min: account not in system at all -> re-queue
_SCHED_MAX_REQUEUE = 2
_SCHED_NO_PROGRESS_SEC = 5400   # 90 min: in system but no level gain -> failed


def _sched_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def parse_schedule_file(text: str):
    """Parse uploaded account file. Format per line: uid:pass . Returns list of dicts."""
    out, seen = [], set()
    for raw in str(text or "").splitlines():
        line = raw.strip().strip(",;")
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        uid, pwd = line.split(":", 1)
        uid, pwd = uid.strip(), pwd.strip()
        if not uid or not pwd or uid in seen:
            continue
        seen.add(uid)
        out.append({"uid": uid, "password": pwd})
    return out


class ScheduleManager:
    def __init__(self, state):
        self.state = state
        self.schedules: Dict[str, Dict[str, Any]] = {}
        self._load()

    # ---------- persistence ----------
    def _load(self):
        try:
            if os.path.exists(SCHEDULES_FILE):
                with open(SCHEDULES_FILE, "r", encoding="utf-8") as f:
                    d = json.load(f)
                if isinstance(d, dict):
                    self.schedules = d
        except Exception:
            self.schedules = {}
        # after a restart, "active" accounts are re-driven by the normal
        # startup (accounts.json); the tick reconciles everything.
        for s in self.schedules.values():
            if s.get("status") == "running":
                s["status"] = "running"  # keep running across restarts

    def _save(self):
        try:
            with open(SCHEDULES_FILE, "w", encoding="utf-8") as f:
                json.dump(self.schedules, f, indent=2)
        except Exception:
            pass

    # ---------- CRUD ----------
    def create(self, name, filename, accounts, batch_size, target_level, play_mode, speed):
        sid = f"sched_{int(time.time())}_{os.urandom(3).hex()}"
        try:
            batch_size = max(1, min(100, int(batch_size)))
        except Exception:
            batch_size = 10
        try:
            target_level = max(2, min(100, int(target_level)))
        except Exception:
            target_level = 21
        pm = self.state._norm_mode(play_mode) if hasattr(self.state, "_norm_mode") else "AUTO"
        try:
            speed = float(speed or 0)
        except Exception:
            speed = 0.0
        # skip uids already managed by another schedule (avoid double-driving)
        taken = set()
        for other in self.schedules.values():
            for a in other.get("accounts", []):
                taken.add(str(a.get("uid", "")))
        fresh = [a for a in accounts if str(a.get("uid", "")) not in taken]
        skipped_dup = len(accounts) - len(fresh)
        sched = {
            "id": sid,
            "name": str(name or filename or "schedule"),
            "filename": str(filename or ""),
            "skipped_duplicates": skipped_dup,
            "accounts": [
                {"uid": a["uid"], "password": a["password"], "status": "pending",
                 "level": 1, "started_at": "", "done_at": "", "fail_reason": ""}
                for a in fresh
            ],
            "batch_size": batch_size,
            "target_level": target_level,
            "play_mode": pm,
            "speed": speed,
            "status": "idle",
            "created_at": _sched_ts(),
            "completed_at": "",
        }
        self.schedules[sid] = sched
        self._save()
        if skipped_dup:
            self.state.log(f"Schedule '{sched['name']}': skipped {skipped_dup} duplicate uid(s) already in another schedule.", "warning")
        return sched

    def get(self, sid):
        return self.schedules.get(sid)

    def delete(self, sid):
        s = self.schedules.pop(sid, None)
        if s is not None:
            # accounts stay in the bot as regular accounts: clear overrides,
            # unpause anything the scheduler paused so they keep working.
            for acc in s.get("accounts", []):
                try:
                    self.state.clear_account_target(acc["uid"])
                    if acc.get("status") == "active" and self.state.is_paused(acc["uid"]):
                        self.state.toggle_pause(acc["uid"])
                except Exception:
                    pass
            self._save()
        return s is not None

    def progress(self, sched):
        accs = sched.get("accounts", [])
        total = len(accs)
        done = sum(1 for a in accs if a.get("status") == "done")
        active = sum(1 for a in accs if a.get("status") == "active")
        failed = sum(1 for a in accs if a.get("status") == "failed")
        pending = total - done - active - failed
        return {"total": total, "done": done, "active": active,
                "failed": failed, "pending": pending}

    # ---------- control ----------
    def start(self, sid):
        s = self.get(sid)
        if not s:
            return False
        if s["status"] in ("idle", "paused", "completed"):
            # fresh start from idle/completed resets non-done accounts to pending
            if s["status"] in ("idle", "completed"):
                for a in s["accounts"]:
                    if a.get("status") != "done":
                        a["status"] = "pending"
                        a["fail_reason"] = ""
                        try:
                            self.state.reset_completion(a["uid"])
                        except Exception:
                            pass
                s["completed_at"] = ""
            else:  # resume from pause: unpause workers
                for a in s["accounts"]:
                    if a.get("status") == "active":
                        try:
                            if self.state.is_paused(a["uid"]):
                                self.state.toggle_pause(a["uid"])
                        except Exception:
                            pass
            s["status"] = "running"
            self._save()
            self.state.log(f"Schedule '{s['name']}' started: {len(s['accounts'])} accounts, "
                           f"{s['batch_size']} at a time, target Lv{s['target_level']}.", "success")
            return True
        return False

    def pause(self, sid):
        s = self.get(sid)
        if not s or s["status"] != "running":
            return False
        s["status"] = "paused"
        for a in s["accounts"]:
            if a.get("status") == "active":
                try:
                    if not self.state.is_paused(a["uid"]):
                        self.state.toggle_pause(a["uid"])
                except Exception:
                    pass
        self._save()
        self.state.log(f"Schedule '{s['name']}' paused.", "warning")
        return True

    # ---------- bot integration ----------
    def _worker_state(self, uid: str) -> str:
        """'live' = worker running, 'parked' = waiting for a Max slot, 'gone' = neither."""
        u = str(uid)
        st = self.state
        candidates = {u}
        try:
            if u in st.auth_to_game_id:
                candidates.add(str(st.auth_to_game_id[u]))
            if u in st.game_to_auth_id:
                candidates.add(str(st.game_to_auth_id[u]))
        except Exception:
            pass
        for k in candidates:
            t = st.account_workers.get(k)
            if t is not None:
                try:
                    if not t.done():
                        return "live"
                except Exception:
                    return "live"
        try:
            for w in st.waiting:
                if str(w.get("uid", "")) == u:
                    return "parked"
        except Exception:
            pass
        return "gone"

    def _in_system(self, uid: str) -> bool:
        return self._worker_state(uid) in ("live", "parked")

    async def _add_to_bot(self, sched, acc) -> bool:
        st = self.state
        uid = str(acc["uid"])
        pwd = str(acc["password"])
        data = {"uid": uid, "password": pwd, "play_mode": sched["play_mode"],
                "speed": sched["speed"], "target_level": sched["target_level"]}
        try:
            existing = _load_accounts_file()
            ok, _ident, _err, existing = _process_account_add(data, existing)
            if not ok:
                return False
            with open("accounts.json", "w", encoding="utf-8") as f:
                json.dump(existing, f, indent=2)
            if "queue_account_login" in st.refresh_callbacks:
                st.refresh_callbacks["queue_account_login"](data)
            elif "on_account_added" in st.refresh_callbacks:
                asyncio.create_task(st.refresh_callbacks["on_account_added"](data))
            acc["_added_at"] = time.time()
            acc["_last_seen"] = time.time()
            acc["_last_lvl"] = 1
            acc["_last_change"] = time.time()
            acc["_requeues"] = 0
            return True
        except Exception as e:
            st.log(f"Schedule '{sched['name']}': add failed for {uid}: {e}", "error")
            return False

    def _stop_bot_account(self, uid: str):
        st = self.state
        u = str(uid)
        try:
            cands = {u}
            if u in st.auth_to_game_id:
                cands.add(str(st.auth_to_game_id[u]))
            if u in st.game_to_auth_id:
                cands.add(str(st.game_to_auth_id[u]))
            for k in list(st.account_workers.keys()):
                if k in cands:
                    t = st.account_workers.pop(k, None)
                    if t:
                        try:
                            t.cancel()
                        except Exception:
                            pass
            if not st.is_paused(u):
                st.toggle_pause(u)
        except Exception:
            pass

    # ---------- main loop ----------
    async def run(self):
        while True:
            try:
                for sched in list(self.schedules.values()):
                    if sched.get("status") == "running":
                        await self._tick(sched)
            except Exception as e:
                try:
                    self.state.log(f"Scheduler error: {e}", "error")
                except Exception:
                    pass
            await asyncio.sleep(_SCHED_TICK_SEC)

    async def _tick(self, sched):
        st = self.state
        now = time.time()
        target = int(sched.get("target_level", 21))
        batch = int(sched.get("batch_size", 10))
        changed = False

        for acc in sched["accounts"]:
            if acc.get("status") != "active":
                continue
            uid = str(acc["uid"])

            # 1) completed by the bot's own milestone machinery?
            try:
                if st.is_completed(uid):
                    acc["status"] = "done"
                    acc["done_at"] = _sched_ts()
                    changed = True
                    continue
            except Exception:
                pass

            # 2) track level
            try:
                lvl = int(st.get_account_level(uid) or 1)
            except Exception:
                lvl = 1
            acc["level"] = lvl
            if lvl != acc.get("_last_lvl"):
                acc["_last_lvl"] = lvl
                acc["_last_change"] = now
                changed = True

            # 3) at/over target but milestone didn't fire -> force completion
            if lvl >= target:
                try:
                    st._auto_off_account(uid)
                except Exception:
                    pass
                acc["status"] = "done"
                acc["done_at"] = _sched_ts()
                changed = True
                continue

            # 4) idle / stuck handling
            wstate = self._worker_state(uid)
            if wstate == "parked":
                # waiting for a Max slot - not stuck, just queued
                acc["_last_seen"] = now
            elif wstate == "live":
                acc["_last_seen"] = now
                try:
                    _on_break = st.get_account_status(uid) == "BREAK"
                except Exception:
                    _on_break = False
                if _on_break:
                    acc["_last_change"] = now  # resting, not stuck
                    continue
                last_change = acc.get("_last_change") or acc.get("_added_at") or now
                if now - last_change > _SCHED_NO_PROGRESS_SEC:
                    acc["status"] = "failed"
                    acc["fail_reason"] = "no level progress 90m"
                    self._stop_bot_account(uid)
                    st.log(f"Schedule '{sched['name']}': {uid} failed (no progress 90m) - slot freed.", "error")
                    changed = True
            else:
                last_seen = acc.get("_last_seen") or acc.get("_added_at") or now
                if now - last_seen > _SCHED_IDLE_REQUEUE_SEC:
                    # user manually removed it from the bot? respect that, don't resurrect
                    try:
                        in_file = any(str(e.get("uid", "")) == uid for e in _load_accounts_file())
                    except Exception:
                        in_file = True
                    if not in_file:
                        acc["status"] = "failed"
                        acc["fail_reason"] = "removed from bot by user"
                        st.log(f"Schedule '{sched['name']}': {uid} was removed from the bot - marked failed.", "warning")
                        changed = True
                        continue
                    rq = int(acc.get("_requeues") or 0)
                    if rq < _SCHED_MAX_REQUEUE:
                        acc["_requeues"] = rq + 1
                        acc["_last_seen"] = now
                        ok = await self._add_to_bot(sched, acc)
                        st.log(f"Schedule '{sched['name']}': re-queued idle {uid} (attempt {rq + 1}).",
                               "warning" if ok else "error")
                    else:
                        acc["status"] = "failed"
                        acc["fail_reason"] = "idle, re-queue exhausted"
                        st.log(f"Schedule '{sched['name']}': {uid} failed (idle too long) - slot freed.", "error")
                    changed = True

        # 5) fill batch up to batch_size
        active = [a for a in sched["accounts"] if a.get("status") == "active"]
        if len(active) < batch:
            for acc in sched["accounts"]:
                if len(active) >= batch:
                    break
                if acc.get("status") != "pending":
                    continue
                ok = await self._add_to_bot(sched, acc)
                if ok:
                    acc["status"] = "active"
                    acc["started_at"] = _sched_ts()
                    active.append(acc)
                    changed = True
                else:
                    acc["status"] = "failed"
                    acc["fail_reason"] = "add failed"
                    changed = True

        # 6) all done?
        prog = self.progress(sched)
        if prog["pending"] == 0 and prog["active"] == 0 and prog["total"] > 0:
            sched["status"] = "completed"
            sched["completed_at"] = _sched_ts()
            st.log(f"Schedule '{sched['name']}' COMPLETED: {prog['done']}/{prog['total']} done, "
                   f"{prog['failed']} failed.", "success")
            changed = True

        if changed:
            self._save()


schedule_manager = ScheduleManager(bot_state)


async def handle_set_lw_break(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "error": "bad json"}, status=400)
    try:
        bot_state.set_lw_break(data.get("lw_break_after", 1000), data.get("lw_break_minutes", 30))
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=400)
    return web.json_response({"status": "ok", "lw_break_after": bot_state.lw_break_after,
                              "lw_break_minutes": bot_state.lw_break_minutes})


async def handle_schedule_upload(request: web.Request) -> web.Response:
    """Multipart file upload -> parse uid:pass lines -> return count + preview."""
    try:
        reader = await request.multipart()
        field = await reader.next()
        if field is None or field.name != "file":
            return web.json_response({"status": "error", "error": "no file field"}, status=400)
        filename = field.filename or "upload.txt"
        data = await field.read()
        try:
            text = data.decode("utf-8", errors="replace")
        except Exception:
            text = ""
        accounts = parse_schedule_file(text)
        preview = [{"uid": a["uid"]} for a in accounts[:5]]
        return web.json_response({"status": "ok", "filename": filename,
                                  "count": len(accounts), "preview": preview,
                                  "accounts": accounts if len(accounts) <= 2000 else []})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)


async def handle_schedule_create(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "error": "bad json"}, status=400)
    accounts = data.get("accounts") or []
    if not accounts:
        return web.json_response({"status": "error", "error": "no accounts"}, status=400)
    # sanitize
    clean = []
    for a in accounts:
        try:
            u, p = str(a.get("uid", "")).strip(), str(a.get("password", "")).strip()
        except Exception:
            continue
        if u and p:
            clean.append({"uid": u, "password": p})
    if not clean:
        return web.json_response({"status": "error", "error": "no valid accounts"}, status=400)
    sched = schedule_manager.create(
        data.get("name"), data.get("filename"),
        clean, data.get("batch_size", 10), data.get("target_level", 21),
        data.get("play_mode", "AUTO"), data.get("speed", 0))
    return web.json_response({"status": "ok", "id": sched["id"],
                              "count": len(sched["accounts"]),
                              "skipped_duplicates": sched.get("skipped_duplicates", 0),
                              "progress": schedule_manager.progress(sched)})


async def handle_schedule_list(request: web.Request) -> web.Response:
    out = []
    for sid, s in schedule_manager.schedules.items():
        out.append({"id": sid, "name": s.get("name"), "filename": s.get("filename"),
                    "status": s.get("status"), "batch_size": s.get("batch_size"),
                    "target_level": s.get("target_level"), "play_mode": s.get("play_mode"),
                    "speed": s.get("speed"), "created_at": s.get("created_at"),
                    "completed_at": s.get("completed_at", ""),
                    "progress": schedule_manager.progress(s)})
    out.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return web.json_response({"status": "ok", "schedules": out})


async def handle_schedule_action(request: web.Request) -> web.Response:
    action = request.match_info.get("action", "")
    sid = request.match_info.get("sid", "")
    if action == "start":
        ok = schedule_manager.start(sid)
    elif action == "pause":
        ok = schedule_manager.pause(sid)
    elif action == "delete":
        ok = schedule_manager.delete(sid)
    else:
        return web.json_response({"status": "error", "error": "unknown action"}, status=400)
    if not ok:
        return web.json_response({"status": "error", "error": "action failed"}, status=400)
    s = schedule_manager.get(sid)
    prog = schedule_manager.progress(s) if s else {}
    return web.json_response({"status": "ok", "action": action,
                              "schedule_status": s.get("status") if s else "deleted",
                              "progress": prog})


async def handle_add_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        existing = _load_accounts_file()

        ok, identifier, err, existing = _process_account_add(data, existing)
        if not ok:
            return web.json_response({"status": "error", "error": err})

        with open("accounts.json", "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)

        bot_state.log(f"New account queued for verification: {identifier}", "success")

        # Queue for one-by-one login verification in Main.py (no hammering)
        if "queue_account_login" in bot_state.refresh_callbacks:
            bot_state.refresh_callbacks["queue_account_login"](data)
        elif "on_account_added" in bot_state.refresh_callbacks:
            asyncio.create_task(bot_state.refresh_callbacks["on_account_added"](data))

        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_bulk_add_accounts(request: web.Request) -> web.Response:
    """Queue many accounts; Main.py verifies + starts them strictly one by one."""
    try:
        data = await request.json()
        raw = data.get("accounts", "")
        lines = raw if isinstance(raw, list) else str(raw).splitlines()
        existing = _load_accounts_file()

        items, seen = [], set()
        bulk_mode = data.get("play_mode", "AUTO")
        try:
            bulk_speed = float(data.get("speed") or 0)
        except Exception:
            bulk_speed = 0.0
        for line in lines:
            parsed = _parse_bulk_line(str(line))
            if not parsed:
                continue
            parsed["play_mode"] = bulk_mode
            parsed["speed"] = bulk_speed
            key = parsed.get("uid") or parsed.get("token", "")[:32]
            if key in seen:
                continue
            seen.add(key)
            ok, identifier, err, existing = _process_account_add(parsed, existing)
            if ok:
                items.append(parsed)
                if "queue_account_login" in bot_state.refresh_callbacks:
                    bot_state.refresh_callbacks["queue_account_login"](parsed)
                elif "on_account_added" in bot_state.refresh_callbacks:
                    asyncio.create_task(bot_state.refresh_callbacks["on_account_added"](parsed))
            else:
                bot_state.log(f"Bulk add skipped line: {err}", "error")

        with open("accounts.json", "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)

        queued = len(items)
        if queued:
            bot_state.log(f"Bulk add: {queued} accounts queued - verifying one by one", "success")
        return web.json_response({"status": "ok", "queued": queued})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_delete_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        req_uid = str(data.get("uid", "")).strip()
        req_auth_uid = str(data.get("auth_uid", "")).strip()
        if not req_uid and not req_auth_uid:
            return web.json_response({"status": "error", "error": "UID is required"})

        # Collect ALL possible candidate identifiers for this account
        candidate_ids = set()
        if req_uid:
            candidate_ids.add(req_uid)
        if req_auth_uid:
            candidate_ids.add(req_auth_uid)

        # Check game_to_auth and auth_to_game mappings
        for cid in list(candidate_ids):
            if cid in bot_state.game_to_auth_id:
                candidate_ids.add(str(bot_state.game_to_auth_id[cid]))
            if cid in bot_state.auth_to_game_id:
                candidate_ids.add(str(bot_state.auth_to_game_id[cid]))

        # Inspect bot_state.accounts
        target_tokens = set()
        for cid in list(candidate_ids):
            acc_info = bot_state.accounts.get(cid, {})
            if acc_info:
                if acc_info.get("auth_uid"):
                    candidate_ids.add(str(acc_info["auth_uid"]))
                if acc_info.get("uid"):
                    candidate_ids.add(str(acc_info["uid"]))
                t = acc_info.get("token") or acc_info.get("access_token")
                if t:
                    target_tokens.add(str(t))

        # Inspect bot_state.account_credentials
        for cid in list(candidate_ids):
            creds = bot_state.account_credentials.get(cid, {})
            if creds:
                if creds.get("auth_uid"):
                    candidate_ids.add(str(creds["auth_uid"]))
                if creds.get("account_id"):
                    candidate_ids.add(str(creds["account_id"]))
                t = creds.get("token") or creds.get("access_token") or creds.get("auth_token")
                if t:
                    target_tokens.add(str(t))

        # Also clean token_cache.json if entries match
        token_cache_file = "token_cache.json"
        if os.path.exists(token_cache_file):
            try:
                with open(token_cache_file, "r", encoding="utf-8") as f:
                    tcache = json.load(f)
                dirty_cache = False
                for k, v in list(tcache.items()):
                    k_str = str(k)
                    v_acc_id = str(v.get("account_id", ""))
                    v_auth_uid = str(v.get("auth_uid", ""))
                    if k_str in candidate_ids or v_acc_id in candidate_ids or v_auth_uid in candidate_ids:
                        candidate_ids.add(k_str)
                        if v_acc_id:
                            candidate_ids.add(v_acc_id)
                        if v_auth_uid:
                            candidate_ids.add(v_auth_uid)
                        del tcache[k]
                        dirty_cache = True
                if dirty_cache:
                    with open(token_cache_file, "w", encoding="utf-8") as f:
                        json.dump(tcache, f, indent=2)
            except Exception:
                pass

        # Remove from accounts.json
        accounts_file = "accounts.json"
        if os.path.exists(accounts_file):
            try:
                with open(accounts_file, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                new_existing = []
                for acc in existing:
                    acc_uid = str(acc.get("uid", "")).strip()
                    acc_tok = str(acc.get("token", "")).strip()
                    is_match = False
                    if acc_uid and acc_uid in candidate_ids:
                        is_match = True
                    if acc_tok and (acc_tok in candidate_ids or acc_tok in target_tokens):
                        is_match = True
                    for tok in target_tokens:
                        if acc_tok and (acc_tok.startswith(tok[:16]) or tok.startswith(acc_tok[:16])):
                            is_match = True
                    if not is_match:
                        new_existing.append(acc)

                with open(accounts_file, "w", encoding="utf-8") as f:
                    json.dump(new_existing, f, indent=2)
            except Exception:
                pass

        # Remove matching devices from devices.json
        devices_file = "devices.json"
        if os.path.exists(devices_file):
            try:
                with open(devices_file, "r", encoding="utf-8") as f:
                    devices_data = json.load(f)
                dirty_devices = False
                for dev_k in list(devices_data.keys()):
                    dev_k_str = str(dev_k)
                    if dev_k_str in candidate_ids:
                        del devices_data[dev_k]
                        dirty_devices = True
                    else:
                        for tok in target_tokens:
                            if dev_k_str == tok[:16] or tok.startswith(dev_k_str):
                                del devices_data[dev_k]
                                dirty_devices = True
                                break
                if dirty_devices:
                    with open(devices_file, "w", encoding="utf-8") as f:
                        json.dump(devices_data, f, indent=4)
            except Exception:
                pass

        # Remove from in-memory bot_state.accounts & credentials
        for cid in candidate_ids:
            bot_state.accounts.pop(cid, None)
            bot_state.account_credentials.pop(cid, None)
            bot_state.auth_to_game_id.pop(cid, None)
            bot_state.game_to_auth_id.pop(cid, None)
            bot_state.account_token_map.pop(cid, None)

        # Cancel matching worker tasks
        cancelled_keys = []
        for k, worker in list(bot_state.account_workers.items()):
            k_str = str(k)
            should_cancel = False
            if k_str in candidate_ids:
                should_cancel = True
            for tok in target_tokens:
                if k_str == tok[:16] or tok.startswith(k_str[:10]):
                    should_cancel = True
            if should_cancel:
                try:
                    worker.cancel()
                except Exception:
                    pass
                cancelled_keys.append(k)

        for k in cancelled_keys:
            bot_state.account_workers.pop(k, None)

        for cid in candidate_ids:
            bot_state.close_writers_for_account(cid)
            bot_state.drop_waiting(uid=cid)
        for tok in target_tokens:
            bot_state.drop_waiting(token=tok)

        # Trigger on_account_deleted callback in Main.py if registered
        if "on_account_deleted" in bot_state.refresh_callbacks:
            try:
                asyncio.create_task(bot_state.refresh_callbacks["on_account_deleted"](list(candidate_ids)))
            except Exception:
                pass

        target_repr = req_uid or req_auth_uid
        bot_state.log(f"Account {target_repr} completely deleted from system and stopped.", "warning", target_repr)
        bot_state.recalc_totals()
        return web.json_response({"status": "ok", "deleted": list(candidate_ids)})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_refresh_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        if "on_refresh_account" in bot_state.refresh_callbacks:
            asyncio.create_task(bot_state.refresh_callbacks["on_refresh_account"](uid))
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_restart_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        if "on_restart_account" in bot_state.refresh_callbacks:
            asyncio.create_task(bot_state.refresh_callbacks["on_restart_account"](uid))
        elif "on_refresh_account" in bot_state.refresh_callbacks:
            asyncio.create_task(bot_state.refresh_callbacks["on_refresh_account"](uid))
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_clear_logs(request: web.Request) -> web.Response:
    bot_state.logs.clear()
    bot_state.clear_errors()
    return web.json_response({"status": "ok"})


async def handle_remove_all_accounts(request: web.Request) -> web.Response:
    """Remove ALL accounts: stop workers, wipe accounts.json/devices.json/token_cache.json."""
    try:
        removed_uids = list(bot_state.accounts.keys())

        # Cancel every running worker task
        for k, worker in list(bot_state.account_workers.items()):
            try:
                worker.cancel()
            except Exception:
                pass
        bot_state.account_workers.clear()

        # Close open sockets for each account
        for cid in removed_uids:
            try:
                bot_state.close_writers_for_account(cid)
            except Exception:
                pass

        # Clear in-memory state
        bot_state.accounts.clear()
        bot_state.account_credentials.clear()
        bot_state.paused_accounts.clear()
        bot_state.auth_to_game_id.clear()
        bot_state.game_to_auth_id.clear()
        bot_state.account_token_map.clear()
        bot_state.match_history.clear()
        bot_state.match_start_times.clear()
        bot_state.waiting.clear()
        bot_state.recalc_totals()

        # Wipe the account files
        for fname, default in (("accounts.json", []), ("devices.json", {}), ("token_cache.json", {})):
            try:
                with open(fname, "w", encoding="utf-8") as f:
                    json.dump(default, f, indent=2)
            except Exception:
                pass

        # Notify Main.py if it registered the callback
        if "on_account_deleted" in bot_state.refresh_callbacks:
            try:
                asyncio.create_task(bot_state.refresh_callbacks["on_account_deleted"](removed_uids))
            except Exception:
                pass

        bot_state.log(f"All accounts removed ({len(removed_uids)} account(s) cleared from system).", "warning")
        return web.json_response({"ok": True, "status": "ok", "removed": len(removed_uids)})
    except Exception as e:
        return web.json_response({"ok": False, "status": "error", "error": str(e)})


async def handle_toggle_pause(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        if not uid:
            return web.json_response({"status": "error", "error": "UID is required"})
        is_paused = bot_state.toggle_pause(uid)
        return web.json_response({"status": "ok", "is_paused": is_paused})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_set_target_level(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "error": "bad json"}, status=400)
    try:
        lvl = int(data.get("target_level", 0))
    except Exception:
        lvl = 0
    if not 2 <= lvl <= 100:
        return web.json_response({"status": "error", "error": "target must be 2-100"}, status=400)
    bot_state.set_target_level(lvl)
    return web.json_response({"status": "ok", "target_level": lvl})


async def handle_set_max_active(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "error": "bad json"}, status=400)
    try:
        n = int(data.get("max_active", 0))
    except Exception:
        n = 0
    if not 1 <= n <= 200:
        return web.json_response({"status": "error", "error": "max must be 1-200"}, status=400)
    try:
        bot_state.set_max_active(n)
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=400)
    return web.json_response({"status": "ok", "max_active": n})


async def handle_set_max_matches(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "error": "bad json"}, status=400)
    try:
        n = int(data.get("max_matches_per_account", 0))
    except Exception:
        n = 0
    if not 1 <= n <= 50:
        return web.json_response({"status": "error", "error": "max must be 1-50"}, status=400)
    try:
        bot_state.set_max_matches_per_account(n)
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=400)
    return web.json_response({"status": "ok", "max_matches_per_account": n})


async def handle_get_proxies(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "ok",
        "enabled": bot_state.proxy_enabled,
        "proxies": bot_state.proxies,
        "valid_count": len(bot_state.get_valid_proxies()),
    })


async def handle_add_proxies(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "error": "bad json"}, status=400)
    raw = data.get("proxies", "")
    if isinstance(raw, str):
        lines = raw.splitlines()
    elif isinstance(raw, list):
        lines = raw
    else:
        lines = [str(raw)]
    added, skipped = bot_state.add_proxies(lines)
    # auto-check newly added proxies (parallel for speed)
    if added:
        await asyncio.gather(*(bot_state.check_proxy(p) for p in added),
                             return_exceptions=True)
    return web.json_response({
        "status": "ok",
        "added": len(added),
        "skipped": skipped,
        "proxies": bot_state.proxies,
        "enabled": bot_state.proxy_enabled,
    })


async def handle_check_proxies(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except Exception:
        data = {}
    host = str(data.get("host", "")).strip()
    port = data.get("port", 0)
    try:
        await bot_state.check_all_proxies()
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)
    return web.json_response({
        "status": "ok",
        "proxies": bot_state.proxies,
        "valid_count": len(bot_state.get_valid_proxies()),
    })


async def handle_remove_proxy(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "error": "bad json"}, status=400)
    host = str(data.get("host", "")).strip()
    try:
        port = int(data.get("port", 0))
    except Exception:
        port = 0
    if not host or not port:
        return web.json_response({"status": "error", "error": "host and port required"}, status=400)
    ok = bot_state.remove_proxy(host, port)
    return web.json_response({"status": "ok" if ok else "error",
                              "proxies": bot_state.proxies})


async def handle_toggle_proxies(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"status": "error", "error": "bad json"}, status=400)
    enabled = data.get("enabled")
    if enabled is None:
        enabled = not bot_state.proxy_enabled
    bot_state.set_proxy_enabled(bool(enabled))
    return web.json_response({"status": "ok", "enabled": bot_state.proxy_enabled})


async def handle_toggle_pause_all(request: web.Request) -> web.Response:
    try:
        paused_state = bot_state.toggle_pause_all()
        return web.json_response({"status": "ok", "all_paused": paused_state})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def start_web_dashboard(host: str = "2001:41d0:306:277a::2417", port: int = 5000):
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/stats", handle_get_stats)
    app.router.add_post("/api/account/add", handle_add_account)
    app.router.add_post("/api/accounts/bulk_add", handle_bulk_add_accounts)
    app.router.add_post("/api/account/delete", handle_delete_account)
    app.router.add_post("/api/accounts/remove_all", handle_remove_all_accounts)
    app.router.add_post("/api/account/refresh", handle_refresh_account)
    app.router.add_post("/api/account/restart", handle_restart_account)
    app.router.add_post("/api/account/pause", handle_toggle_pause)
    app.router.add_post("/api/account/pause_all", handle_toggle_pause_all)
    app.router.add_post("/api/settings/target_level", handle_set_target_level)
    app.router.add_post("/api/settings/max_active", handle_set_max_active)
    app.router.add_post("/api/settings/max_matches_per_account", handle_set_max_matches)
    app.router.add_post("/api/settings/lw_break", handle_set_lw_break)
    app.router.add_post("/api/schedule/upload", handle_schedule_upload)
    app.router.add_post("/api/schedule/create", handle_schedule_create)
    app.router.add_get("/api/schedule/list", handle_schedule_list)
    app.router.add_post("/api/schedule/{action}/{sid}", handle_schedule_action)
    app.router.add_get("/api/proxies", handle_get_proxies)
    app.router.add_post("/api/proxies/add", handle_add_proxies)
    app.router.add_post("/api/proxies/check", handle_check_proxies)
    app.router.add_post("/api/proxies/remove", handle_remove_proxy)
    app.router.add_post("/api/proxies/toggle", handle_toggle_proxies)
    app.router.add_post("/api/logs/clear", handle_clear_logs)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    try:
        asyncio.create_task(schedule_manager.run())
    except Exception:
        pass
    return runner
