import httpx, time, sys
t = time.time()
try:
    r = httpx.get(
        "https://version.ggwhitehawk.com/live/ver.php?version=1.132.6&lang=hi&device=android"
        "&channel=android&appstore=googleplay&region=BD&whitelist_version=1.3.0&whitelist_sp_version=1.0.0",
        timeout=8.0,
    )
    print("HTTP", r.status_code, "in", round(time.time() - t, 1), "s", flush=True)
    d = r.json()
    ok = bool(d.get("server_url") and d.get("remote_version") and d.get("latest_release_version"))
    print("keys ok:", ok, flush=True)
except Exception as e:
    print("FAILED in", round(time.time() - t, 1), "s:", type(e).__name__, str(e)[:150], flush=True)
    sys.exit(1)
